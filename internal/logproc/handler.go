package logproc

import (
	"obauditor/internal/db"
	"obauditor/internal/logging"
	"obauditor/internal/model"
)

// LogLineHandler — обработчик одной строки лога.
// Диспетчеризует в правильный парсер по типу файла (SERVER / PROXY).
//
// LOGIN_OK / LOGIN_FAIL → SessionDao.InsertLogin().
//
//	Если LOGIN_OK и proxy_sessid != 0 → добавляем в openSessions.
//
// LOGOFF → SessionDao.UpdateLogoff() / UpdateLogoffDirect.
//
//	Ищем proxy_sessid в openSessions. Если не найден — логофф для "старой"
//	сессии, всё равно вызываем UPDATE как fallback (не теряем закрытия).
type LogLineHandler struct {
	fileType    string
	fileName    string
	serverIp    string
	sessionDao  *db.SessionDao
	openSess    map[uint64]struct{} // proxy_sessid открытых сессий
	log         *logging.Logger
	proxyParser *ObProxyLineParser

	processedCount  int64
	skippedCount    int64
	eventCount      int64
	insertedCount   int64
	logoffCount     int64
	logoffMissCount int64
}

func NewLogLineHandler(fileType, fileName, serverIp string,
	sessionDao *db.SessionDao, openSess map[uint64]struct{}, log *logging.Logger) *LogLineHandler {
	if openSess == nil {
		openSess = make(map[uint64]struct{})
	}
	return &LogLineHandler{
		fileType:    fileType,
		fileName:    fileName,
		serverIp:    serverIp,
		sessionDao:  sessionDao,
		openSess:    openSess,
		log:         log,
		proxyParser: NewObProxyLineParser(),
	}
}

func (h *LogLineHandler) SetServerIp(ip string) {
	if ip != "" {
		h.serverIp = ip
	}
}

// Handle — обработать одну строку.
func (h *LogLineHandler) Handle(raw string) {
	h.processedCount++

	var event *model.LoginEvent
	switch h.fileType {
	case "SERVER":
		event = ParseObServerLine(raw)
	case "PROXY":
		event = h.proxyParser.Parse(raw)
	}
	if event == nil {
		return
	}
	h.eventCount++

	switch event.EventType {
	case "LOGIN_OK", "LOGIN_FAIL":
		h.handleLogin(event)
	case "LOGOFF":
		h.handleLogoff(event)
	}
}

func (h *LogLineHandler) handleLogin(event *model.LoginEvent) {
	if err := h.sessionDao.InsertLogin(event, h.serverIp); err != nil {
		h.log.Errorf("[LogLineHandler] insertLogin failed (%s %s): %v",
			event.Source, event.EventType, err)
		return
	}
	h.insertedCount++

	// LOGIN_OK с proxy_sessid → добавляем в set
	if event.EventType == "LOGIN_OK" {
		if ps := event.EffectiveProxySessid(); ps != nil {
			h.openSess[*ps] = struct{}{}
		}
	}
}

func (h *LogLineHandler) handleLogoff(event *model.LoginEvent) {
	proxySessid := event.EffectiveProxySessid()

	// proxy_sessid=nil или 0 — прямое подключение без прокси.
	// Закрываем по session_id + server_ip только для SERVER-записей.
	if proxySessid == nil || *proxySessid == 0 {
		if event.Source == "SERVER" && event.SessionId != nil && h.serverIp != "" {
			updated, err := h.sessionDao.UpdateLogoffDirect(*event.SessionId, h.serverIp, event.EventTime)
			if err != nil {
				h.log.Errorf("[LogLineHandler] updateLogoffDirect failed session_id=%d: %v",
					*event.SessionId, err)
				return
			}
			if updated > 0 {
				h.logoffCount++
				h.log.Debugf("[LogLineHandler] LOGOFF direct closed %d row(s) session_id=%d ip=%s",
					updated, *event.SessionId, h.serverIp)
			} else {
				h.log.Debugf("[LogLineHandler] LOGOFF direct no rows updated session_id=%d",
					*event.SessionId)
			}
		} else {
			h.log.Debugf("[LogLineHandler] LOGOFF skipped (no proxy_sessid, no session_id or ip): %v", event)
		}
		return
	}

	// proxy_sessid > 0 — закрываем сразу обе строки (SERVER + PROXY)
	ps := *proxySessid
	_, inSet := h.openSess[ps]
	if inSet {
		delete(h.openSess, ps)
	} else {
		h.logoffMissCount++
		h.log.Debugf("[LogLineHandler] LOGOFF fallback (not in set) proxy_sessid=%d", ps)
	}

	updated, err := h.sessionDao.UpdateLogoff(ps, event.EventTime)
	if err != nil {
		h.log.Errorf("[LogLineHandler] updateLogoff failed proxy_sessid=%d: %v", ps, err)
		return
	}
	if updated > 0 {
		h.logoffCount++
		h.log.Debugf("[LogLineHandler] LOGOFF closed %d row(s) proxy_sessid=%d at %s",
			updated, ps, event.EventTime)
	} else {
		h.log.Debugf("[LogLineHandler] LOGOFF no rows updated proxy_sessid=%d", ps)
	}
}

// Геттеры счётчиков
func (h *LogLineHandler) ProcessedCount() int64  { return h.processedCount }
func (h *LogLineHandler) SkippedCount() int64    { return h.skippedCount }
func (h *LogLineHandler) EventCount() int64      { return h.eventCount }
func (h *LogLineHandler) InsertedCount() int64   { return h.insertedCount }
func (h *LogLineHandler) LogoffCount() int64     { return h.logoffCount }
func (h *LogLineHandler) LogoffMissCount() int64 { return h.logoffMissCount }
