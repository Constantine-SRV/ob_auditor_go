package db

import (
	"database/sql"

	"obauditor/internal/model"
)

// LogFileDao — DAO для таблицы logfiles.
// Все операции фильтруются по collector_id — каждый сервис
// видит и меняет только свои записи.
type LogFileDao struct {
	db *sql.DB
}

func NewLogFileDao(db *sql.DB) *LogFileDao {
	return &LogFileDao{db: db}
}

// LoadByDir загружает записи для данного коллектора и директории. Ключ: fileName.
func (d *LogFileDao) LoadByDir(collectorId, fileDir string) (map[string]*model.LogFileRecord, error) {
	q := "SELECT id, collector_id, file_dir, file_name, file_type, file_size, " +
		"last_line_num, last_timestamp, last_tid, last_trace_id, file_ip " +
		"FROM logfiles WHERE collector_id = ? AND file_dir = ?"
	rows, err := d.db.Query(q, collectorId, fileDir)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]*model.LogFileRecord)
	for rows.Next() {
		r := &model.LogFileRecord{}
		var lastTs, lastTraceId, fileIp sql.NullString
		var lastTid sql.NullInt32
		if err := rows.Scan(&r.Id, &r.CollectorId, &r.FileDir, &r.FileName, &r.FileType,
			&r.FileSize, &r.LastLineNum, &lastTs, &lastTid, &lastTraceId, &fileIp); err != nil {
			return nil, err
		}
		if lastTs.Valid {
			r.LastTimestamp = lastTs.String
		}
		if lastTraceId.Valid {
			r.LastTraceId = lastTraceId.String
		}
		if fileIp.Valid {
			r.FileIp = fileIp.String
		}
		if lastTid.Valid {
			v := int(lastTid.Int32)
			r.LastTid = &v
		}
		result[r.FileName] = r
	}
	return result, rows.Err()
}

// Insert вставляет новую запись, заполняет сгенерированный id.
func (d *LogFileDao) Insert(r *model.LogFileRecord) error {
	q := "INSERT INTO logfiles " +
		"(collector_id, file_dir, file_name, file_type, file_size, last_line_num, " +
		" last_timestamp, last_tid, last_trace_id, file_ip) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	res, err := d.db.Exec(q,
		r.CollectorId, r.FileDir, r.FileName, r.FileType, r.FileSize, r.LastLineNum,
		nullStr(r.LastTimestamp), nullIntPtr(r.LastTid), nullStr(r.LastTraceId), nullStr(r.FileIp))
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	r.Id = id
	return nil
}

// Update обновляет состояние после обработки файла.
func (d *LogFileDao) Update(r *model.LogFileRecord) error {
	q := "UPDATE logfiles SET " +
		"file_size = ?, last_line_num = ?, " +
		"last_timestamp = ?, last_tid = ?, last_trace_id = ?, file_ip = ? " +
		"WHERE id = ?"
	_, err := d.db.Exec(q,
		r.FileSize, r.LastLineNum,
		nullStr(r.LastTimestamp), nullIntPtr(r.LastTid), nullStr(r.LastTraceId), nullStr(r.FileIp),
		r.Id)
	return err
}

// UpdateFileIp обновляет только file_ip — вызывается сразу как найден IP в PROXY-логе.
func (d *LogFileDao) UpdateFileIp(r *model.LogFileRecord) error {
	if r.Id == 0 {
		return nil // ещё не сохранена в БД
	}
	_, err := d.db.Exec("UPDATE logfiles SET file_ip = ? WHERE id = ?", nullStr(r.FileIp), r.Id)
	return err
}

// ── helpers для nullable значений ────────────────────────────────────

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullIntPtr(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}
