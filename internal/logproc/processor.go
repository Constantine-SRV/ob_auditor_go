package logproc

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"obauditor/internal/config"
	"obauditor/internal/db"
	"obauditor/internal/logging"
	"obauditor/internal/model"
)

// LogFileProcessor обрабатывает лог-файлы OceanBase из заданных директорий.
//
// Перед чтением каждого файла:
//  1. Определяется server_ip (fileIp) — из первой строки или local_ip в теле лога
//  2. Загружаются открытые сессии из БД (Set<uint64> proxy_sessid)
//  3. В процессе чтения:
//     - LOGIN_OK → insert + добавить proxy_sessid в set
//     - LOGOFF   → найти proxy_sessid в set, вызвать updateLogoff, убрать из set
//
// last_line_num хранит байтовый OFFSET — file.Seek(offset) пропускает
// уже прочитанное без построчного перебора.
type LogFileProcessor struct {
	db          *sql.DB
	dao         *db.LogFileDao
	sessionDao  *db.SessionDao
	collectorId string
	log         *logging.Logger

	totalInserted   int64
	totalLogoff     int64
	totalLogoffMiss int64
	totalLines      int64
}

const staleMinutes = 10

var (
	pServerFirstLineIp = regexp.MustCompile(`address:\s*"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):\d+"`)
	pProxyLocalIp      = regexp.MustCompile(`\blocal_ip:\{(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):\d+\}`)
)

const logTsFormat = "2006-01-02 15:04:05.000000"

func NewLogFileProcessor(database *sql.DB, cfg *config.AppConfig, log *logging.Logger) *LogFileProcessor {
	return &LogFileProcessor{
		db:          database,
		dao:         db.NewLogFileDao(database),
		sessionDao:  db.NewSessionDao(database),
		collectorId: cfg.CollectorId,
		log:         log,
	}
}

func (p *LogFileProcessor) TotalInserted() int64   { return p.totalInserted }
func (p *LogFileProcessor) TotalLogoff() int64     { return p.totalLogoff }
func (p *LogFileProcessor) TotalLogoffMiss() int64 { return p.totalLogoffMiss }
func (p *LogFileProcessor) TotalLines() int64      { return p.totalLines }

// ProcessServerDirs обрабатывает все указанные директории как SERVER-логи.
func (p *LogFileProcessor) ProcessServerDirs(dirs []string) error {
	for _, dir := range dirs {
		if err := p.processDirectory(dir, "SERVER",
			[]string{"observer.log", "observer.log."}); err != nil {
			return err
		}
	}
	return nil
}

// ProcessProxyDirs обрабатывает все указанные директории как PROXY-логи.
func (p *LogFileProcessor) ProcessProxyDirs(dirs []string) error {
	for _, dir := range dirs {
		if err := p.processDirectory(dir, "PROXY",
			[]string{"obproxy.log", "obproxy.log."}); err != nil {
			return err
		}
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────

func (p *LogFileProcessor) processDirectory(dirPath, fileType string, namePrefixes []string) error {
	stat, err := os.Stat(dirPath)
	if err != nil || !stat.IsDir() {
		p.log.Errorf("[LogFileProcessor] Directory not found: %s", dirPath)
		return nil // не падаем — другие директории могут быть валидными
	}
	p.log.Debugf("[LogFileProcessor] Processing dir: %s type=%s collector=%s",
		dirPath, fileType, p.collectorId)

	knownFiles, err := p.dao.LoadByDir(p.collectorId, dirPath)
	if err != nil {
		return fmt.Errorf("loadByDir %s: %w", dirPath, err)
	}

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", dirPath, err)
	}

	var files []os.FileInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		matched := false
		for _, prefix := range namePrefixes {
			if name == prefix || strings.HasPrefix(name, prefix) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, info)
	}

	if len(files) == 0 {
		p.log.Debugf("[LogFileProcessor] No log files found in: %s", dirPath)
		return nil
	}

	// Ротированные первыми, активный — последним.
	activeName := namePrefixes[0]
	sort.Slice(files, func(i, j int) bool {
		ai := files[i].Name() == activeName
		aj := files[j].Name() == activeName
		if ai != aj {
			// активный сортируется как "больше" (т.е. в конец)
			return aj // если j активный, то i < j
		}
		return files[i].Name() < files[j].Name()
	})

	for _, f := range files {
		if err := p.processFile(filepath.Join(dirPath, f.Name()), f, dirPath, fileType,
			activeName, knownFiles); err != nil {
			p.log.Errorf("[LogFileProcessor] %s: %v", f.Name(), err)
		}
	}
	return nil
}

// processFile обрабатывает один лог-файл.
func (p *LogFileProcessor) processFile(fullPath string, info os.FileInfo,
	dirPath, fileType, activeName string,
	knownFiles map[string]*model.LogFileRecord) error {

	fileName := info.Name()
	isActive := fileName == activeName
	currentSize := info.Size()

	record, exists := knownFiles[fileName]
	isNew := !exists
	if isNew {
		record = &model.LogFileRecord{
			CollectorId: p.collectorId,
			FileDir:     dirPath,
			FileName:    fileName,
			FileType:    fileType,
			FileSize:    0,
			LastLineNum: 0,
		}
	}

	startOffset := record.LastLineNum

	if isActive && !isNew && p.shouldResetToStart(record, currentSize) {
		p.log.Infof("[LogFileProcessor] Rotation detected for %s — reading from start", fileName)
		startOffset = 0
		record.LastLineNum = 0
		record.LastTimestamp = ""
		record.LastTid = nil
		record.LastTraceId = ""
	}

	if !isNew && currentSize == record.FileSize && startOffset >= currentSize {
		p.log.Debugf("[LogFileProcessor] %s — no changes (size=%d), skipping", fileName, currentSize)
		return nil
	}

	p.log.Debugf("[LogFileProcessor] Processing %s (size=%d, offset=%d)",
		fileName, currentSize, startOffset)

	// IP узла
	serverIp := record.FileIp
	if serverIp == "" && fileType == "SERVER" {
		serverIp = p.readServerIpFromFirstLine(fullPath)
		record.FileIp = serverIp
	}
	if serverIp == "" {
		p.log.Debugf("[LogFileProcessor] %s — file_ip=(searching in log...)", fileName)
	} else {
		p.log.Debugf("[LogFileProcessor] %s — file_ip=%s", fileName, serverIp)
	}

	// Открытые сессии из БД
	openSessions := p.loadOpenSessions(serverIp)
	p.log.Debugf("[LogFileProcessor] %s — loaded %d open sessions", fileName, len(openSessions))

	t0 := time.Now()
	handler := NewLogLineHandler(fileType, fileName, serverIp, p.sessionDao, openSessions, p.log)
	if err := p.readAndProcess(fullPath, startOffset, record, handler, isNew); err != nil {
		return err
	}
	elapsedMs := time.Since(t0).Milliseconds()

	record.FileSize = currentSize

	if isNew {
		if err := p.dao.Insert(record); err != nil {
			return fmt.Errorf("insert logfile record: %w", err)
		}
		p.log.Debugf("[LogFileProcessor] %s — inserted new record id=%d", fileName, record.Id)
	} else {
		if err := p.dao.Update(record); err != nil {
			return fmt.Errorf("update logfile record: %w", err)
		}
	}

	fileIp := record.FileIp
	if fileIp == "" {
		fileIp = ""
	}
	p.log.Infof("[LogFileProcessor] %s — done. lines=%d events=%d inserted=%d logoff=%d logoffMiss=%d offset=%d ip=%s time=%dms",
		fileName,
		handler.ProcessedCount(), handler.EventCount(),
		handler.InsertedCount(), handler.LogoffCount(),
		handler.LogoffMissCount(), record.LastLineNum,
		fileIp, elapsedMs)

	p.totalInserted += handler.InsertedCount()
	p.totalLogoff += handler.LogoffCount()
	p.totalLogoffMiss += handler.LogoffMissCount()
	p.totalLines += handler.ProcessedCount()
	return nil
}

func (p *LogFileProcessor) loadOpenSessions(serverIp string) map[uint64]struct{} {
	if serverIp == "" {
		return make(map[uint64]struct{})
	}
	m, err := p.sessionDao.LoadOpenProxySessids(serverIp)
	if err != nil {
		p.log.Errorf("[LogFileProcessor] Failed to load open sessions: %v", err)
		return make(map[uint64]struct{})
	}
	return m
}

// readAndProcess читает файл начиная с offset, строка за строкой.
// При успешном прочтении обновляет record.LastLineNum на новую позицию.
func (p *LogFileProcessor) readAndProcess(fullPath string, startOffset int64,
	record *model.LogFileRecord, handler *LogLineHandler, isNew bool) error {

	f, err := os.Open(fullPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", fullPath, err)
	}
	defer f.Close()

	if startOffset > 0 {
		size, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		if startOffset <= size {
			if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
				return err
			}
		} else {
			// файл оказался меньше offset — читаем с начала
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return err
			}
			startOffset = 0
		}
	}

	// bufio.Scanner имеет лимит длины строки (по умолчанию 64KB);
	// строки логов могут быть длинными, увеличиваем буфер до 1МБ.
	reader := bufio.NewReaderSize(f, 64*1024)

	needProxyIp := record.FileType == "PROXY" && record.FileIp == ""
	var bytesRead int64 = startOffset

	for {
		// ReadString включает разделитель \n; используем его для точного подсчёта байт.
		// При EOF без терминатора получаем последнюю строку и io.EOF.
		line, err := reader.ReadString('\n')
		// Учитываем длину прочитанных байт ВСЕГДА (даже на EOF).
		if line != "" {
			bytesRead += int64(len(line))

			// Только полная строка (с \n) считается обработанной — не процессим
			// "недозаписанную" хвостовую строку, чтобы не разделить логин надвое
			// между прогонами.
			if err == nil {
				processed := stripCR(strings.TrimSuffix(line, "\n"))

				if needProxyIp && strings.Contains(processed, "server session born") {
					if m := pProxyLocalIp.FindStringSubmatch(processed); m != nil {
						record.FileIp = m[1]
						needProxyIp = false
						p.log.Debugf("[LogFileProcessor] %s — found proxy ip: %s",
							record.FileName, record.FileIp)
						if !isNew && record.Id > 0 {
							if e := p.dao.UpdateFileIp(record); e != nil {
								p.log.Errorf("[LogFileProcessor] updateFileIp failed: %v", e)
							}
						}
						handler.SetServerIp(record.FileIp)
					}
				}

				handler.Handle(processed)
			} else {
				// EOF посередине строки — откатываем байты, эту строку не считаем обработанной
				bytesRead -= int64(len(line))
			}
		}
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("read %s: %w", fullPath, err)
		}
	}

	record.LastLineNum = bytesRead
	return nil
}

// readServerIpFromFirstLine — IP из первой строки observer.log.
func (p *LogFileProcessor) readServerIpFromFirstLine(fullPath string) string {
	f, err := os.Open(fullPath)
	if err != nil {
		p.log.Errorf("[LogFileProcessor] Cannot open first line of %s: %v", fullPath, err)
		return ""
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 64*1024)
	firstLine, err := reader.ReadString('\n')
	if err != nil && firstLine == "" {
		return ""
	}
	firstLine = stripCR(strings.TrimSuffix(firstLine, "\n"))
	if m := pServerFirstLineIp.FindStringSubmatch(firstLine); m != nil {
		return m[1]
	}
	return ""
}

// shouldResetToStart — нужно ли начать читать файл заново.
func (p *LogFileProcessor) shouldResetToStart(record *model.LogFileRecord, currentSize int64) bool {
	// Файл стал меньше → ротация.
	if currentSize < record.FileSize {
		return true
	}
	if record.LastTimestamp != "" {
		ts, err := time.ParseInLocation(logTsFormat, record.LastTimestamp, time.Local)
		if err != nil {
			p.log.Errorf("[LogFileProcessor] Cannot parse lastTimestamp: %s", record.LastTimestamp)
			return false
		}
		minutesAgo := int(time.Since(ts).Minutes())
		if minutesAgo > staleMinutes {
			p.log.Debugf("[LogFileProcessor] Last timestamp %s is %d min ago — resetting",
				record.LastTimestamp, minutesAgo)
			return true
		}
	}
	return false
}

// stripCR — обрезает \r на конце (Windows line endings в файлах с Linux-сервера).
func stripCR(s string) string {
	if len(s) > 0 && s[len(s)-1] == '\r' {
		return s[:len(s)-1]
	}
	return s
}
