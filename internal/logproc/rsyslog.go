package logproc

import (
	"database/sql"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"obauditor/internal/logging"
)

// RsyslogSender — пересылка событий аудита в rsyslog по UDP (RFC 3164).
//
// Три типа событий, каждый со своим курсором в rsyslog_cursor:
//
//	login  — новые строки sessions (cursor по id)
//	logoff — закрытые сессии sessions (cursor по logoff_time + id)
//	ddl    — новые строки ddl_dcl_audit_log (cursor по id)
//
// Курсор logoff: используется пара (last_time, last_id), потому что
// logoff_time обновляется у существующей строки — сессия с id=50 может
// закрыться позже чем сессия с id=200.
type RsyslogSender struct {
	db        *sql.DB
	host      string
	port      int
	batchSize int
	pri       string
	hostname  string
	log       *logging.Logger
}

const maxMsg = 1024 // RFC 3164

// Формат для DB DATETIME(6) → строка
const dbTsFmt = "2006-01-02 15:04:05.000000"

// Формат для syslog header
const syslogTsFmt = "Jan _2 15:04:05"

func NewRsyslogSender(db *sql.DB, host string, port, batchSize int,
	facility string, log *logging.Logger) *RsyslogSender {
	if batchSize <= 0 {
		batchSize = 500
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	pri := fmt.Sprintf("<%d>", resolveFacility(facility)*8+6) // severity=info(6)
	return &RsyslogSender{
		db:        db,
		host:      host,
		port:      port,
		batchSize: batchSize,
		pri:       pri,
		hostname:  hostname,
		log:       log,
	}
}

// resolveFacility — RFC 3164 facility numbers. Default = user(1).
func resolveFacility(name string) int {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "kern":
		return 0
	case "user":
		return 1
	case "mail":
		return 2
	case "daemon":
		return 3
	case "auth":
		return 4
	case "syslog":
		return 5
	case "lpr":
		return 6
	case "news":
		return 7
	case "uucp":
		return 8
	case "cron":
		return 9
	case "local0":
		return 16
	case "local1":
		return 17
	case "local2":
		return 18
	case "local3":
		return 19
	case "local4":
		return 20
	case "local5":
		return 21
	case "local6":
		return 22
	case "local7":
		return 23
	default:
		return 1
	}
}

// Send отправляет все новые события в rsyslog.
// Возвращает [loginsSent, logoffsSent, ddlSent] — {0,0,0} при ошибке.
func (s *RsyslogSender) Send() (int, int, int) {
	if err := s.ensureCursorRows(); err != nil {
		s.log.Errorf("[RsyslogSender] ensureCursorRows: %v", err)
		return 0, 0, 0
	}

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", s.host, s.port))
	if err != nil {
		s.log.Errorf("[RsyslogSender] resolve udp: %v", err)
		return 0, 0, 0
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		s.log.Errorf("[RsyslogSender] dial udp: %v", err)
		return 0, 0, 0
	}
	defer conn.Close()

	logins, err := s.sendLogins(conn)
	if err != nil {
		s.log.Errorf("[RsyslogSender] sendLogins: %v", err)
		return 0, 0, 0
	}
	logoffs, err := s.sendLogoffs(conn)
	if err != nil {
		s.log.Errorf("[RsyslogSender] sendLogoffs: %v", err)
		return logins, 0, 0
	}
	ddl, err := s.sendDdl(conn)
	if err != nil {
		s.log.Errorf("[RsyslogSender] sendDdl: %v", err)
		return logins, logoffs, 0
	}

	if logins+logoffs+ddl > 0 {
		s.log.Infof("[RsyslogSender] Forwarded login=%d logoff=%d ddl=%d to %s:%d",
			logins, logoffs, ddl, s.host, s.port)
	} else {
		s.log.Debugf("[RsyslogSender] No new events to forward")
	}
	return logins, logoffs, ddl
}

// ── Login events — cursor по id ──────────────────────────────────────

func (s *RsyslogSender) sendLogins(conn *net.UDPConn) (int, error) {
	cursor, err := s.getLastId("login")
	if err != nil {
		return 0, err
	}
	total := 0
	query := "SELECT id, source, login_time, is_success, client_ip, " +
		"       tenant_name, user_name, error_code, client_type, session_id " +
		"FROM admintools.sessions WHERE id > ? ORDER BY id ASC LIMIT ?"

	for {
		rows, err := s.db.Query(query, cursor, s.batchSize)
		if err != nil {
			return total, err
		}
		var maxId int64 = int64(cursor)
		count := 0
		for rows.Next() {
			var id int64
			var src, loginTime sql.NullString
			var isSuccess int
			var clientIp, tenant, user, clientType, sessionId sql.NullString
			var errorCode sql.NullInt64
			if err := rows.Scan(&id, &src, &loginTime, &isSuccess, &clientIp,
				&tenant, &user, &errorCode, &clientType, &sessionId); err != nil {
				_ = rows.Close()
				return total, err
			}
			ok := "FAIL"
			if isSuccess == 1 {
				ok = "OK"
			}
			msg := fmt.Sprintf("LOGIN result=%s source=%s user=%s tenant=%s client_ip=%s "+
				"session_id=%s client_type=%s time=%s",
				ok, dashIfNull(src), dashIfNull(user), dashIfNull(tenant),
				dashIfNull(clientIp), dashIfNull(sessionId),
				dashIfNull(clientType), dashIfNull(loginTime))
			if err := s.sendUdp(conn, msg); err != nil {
				_ = rows.Close()
				return total, err
			}
			if id > maxId {
				maxId = id
			}
			count++
		}
		_ = rows.Close()
		if count == 0 {
			break
		}
		if err := s.updateLastId("login", maxId); err != nil {
			return total, err
		}
		cursor = uint64(maxId)
		total += count
		if count < s.batchSize {
			break
		}
	}
	return total, nil
}

// ── Logoff events — cursor по (logoff_time, id) ──────────────────────

func (s *RsyslogSender) sendLogoffs(conn *net.UDPConn) (int, error) {
	lastTime, err := s.getLogoffLastTime()
	if err != nil {
		return 0, err
	}
	lastId, err := s.getLastId("logoff")
	if err != nil {
		return 0, err
	}
	total := 0

	for {
		rows, err := s.buildLogoffQuery(lastTime, lastId)
		if err != nil {
			return total, err
		}
		count := 0
		maxTime := lastTime
		maxId := lastId

		for rows.Next() {
			var id int64
			var src, loginTime, clientIp, tenant, user, sessionId sql.NullString
			var logoffTime sql.NullTime
			if err := rows.Scan(&id, &src, &loginTime, &logoffTime, &clientIp,
				&tenant, &user, &sessionId); err != nil {
				_ = rows.Close()
				return total, err
			}
			logoffStr := "-"
			if logoffTime.Valid {
				logoffStr = logoffTime.Time.Format(dbTsFmt)
			}
			msg := fmt.Sprintf("LOGOFF source=%s user=%s tenant=%s client_ip=%s "+
				"session_id=%s login_time=%s logoff_time=%s",
				dashIfNull(src), dashIfNull(user), dashIfNull(tenant),
				dashIfNull(clientIp), dashIfNull(sessionId),
				dashIfNull(loginTime), logoffStr)
			if err := s.sendUdp(conn, msg); err != nil {
				_ = rows.Close()
				return total, err
			}
			// сортировка по (logoff_time ASC, id ASC): всегда последняя строка
			// = верхняя граница курсора, даже если её id меньше предыдущих.
			maxTime = logoffStr
			maxId = uint64(id)
			count++
		}
		_ = rows.Close()

		if count == 0 {
			break
		}
		if err := s.updateLogoffCursor(maxTime, maxId); err != nil {
			return total, err
		}
		lastTime = maxTime
		lastId = maxId
		total += count
		if count < s.batchSize {
			break
		}
	}
	return total, nil
}

func (s *RsyslogSender) buildLogoffQuery(lastTime string, lastId uint64) (*sql.Rows, error) {
	if lastTime == "" {
		// первый запуск — отправляем все закрытые сессии
		return s.db.Query(
			"SELECT id, source, login_time, logoff_time, client_ip, tenant_name, user_name, session_id "+
				"FROM admintools.sessions WHERE logoff_time IS NOT NULL "+
				"ORDER BY logoff_time ASC, id ASC LIMIT ?", s.batchSize)
	}
	return s.db.Query(
		"SELECT id, source, login_time, logoff_time, client_ip, tenant_name, user_name, session_id "+
			"FROM admintools.sessions WHERE logoff_time IS NOT NULL "+
			"AND (logoff_time > ? OR (logoff_time = ? AND id > ?)) "+
			"ORDER BY logoff_time ASC, id ASC LIMIT ?",
		lastTime, lastTime, lastId, s.batchSize)
}

// ── DDL events — cursor по id ────────────────────────────────────────

func (s *RsyslogSender) sendDdl(conn *net.UDPConn) (int, error) {
	cursor, err := s.getLastId("ddl")
	if err != nil {
		return 0, err
	}
	total := 0

	query := "SELECT id, request_ts, tenant_name, user_name, " +
		"       db_name, stmt_type, query_sql, ret_code " +
		"FROM admintools.ddl_dcl_audit_log WHERE id > ? ORDER BY id ASC LIMIT ?"

	for {
		rows, err := s.db.Query(query, cursor, s.batchSize)
		if err != nil {
			return total, err
		}
		var maxId int64 = int64(cursor)
		count := 0
		for rows.Next() {
			var id int64
			var requestTs sql.NullString
			var tenant, user, dbName, stmtType, querySql sql.NullString
			var retCode sql.NullInt64
			if err := rows.Scan(&id, &requestTs, &tenant, &user,
				&dbName, &stmtType, &querySql, &retCode); err != nil {
				_ = rows.Close()
				return total, err
			}
			rawSql := ""
			if querySql.Valid {
				rawSql = querySql.String
				if len(rawSql) > 256 {
					rawSql = rawSql[:256] + "..."
				}
				rawSql = strings.ReplaceAll(rawSql, "\n", " ")
				rawSql = strings.ReplaceAll(rawSql, "\r", " ")
			} else {
				rawSql = "-"
			}
			retStr := "-"
			if retCode.Valid {
				retStr = fmt.Sprintf("%d", retCode.Int64)
			}
			msg := fmt.Sprintf("DDL user=%s tenant=%s db=%s stmt=%s ret=%s sql=%s time=%s",
				dashIfNull(user), dashIfNull(tenant), dashIfNull(dbName),
				dashIfNull(stmtType), retStr, rawSql, dashIfNull(requestTs))
			if err := s.sendUdp(conn, msg); err != nil {
				_ = rows.Close()
				return total, err
			}
			if id > maxId {
				maxId = id
			}
			count++
		}
		_ = rows.Close()
		if count == 0 {
			break
		}
		if err := s.updateLastId("ddl", maxId); err != nil {
			return total, err
		}
		cursor = uint64(maxId)
		total += count
		if count < s.batchSize {
			break
		}
	}
	return total, nil
}

// ── UDP отправка ─────────────────────────────────────────────────────

func (s *RsyslogSender) sendUdp(conn *net.UDPConn, message string) error {
	ts := time.Now().Format(syslogTsFmt)
	full := s.pri + ts + " " + s.hostname + " obauditor: " + message
	if len(full) > maxMsg {
		full = full[:maxMsg]
	}
	_, err := conn.Write([]byte(full))
	if err == nil {
		s.log.Debugf("[RsyslogSender] UDP → %s", full)
	}
	return err
}

// ── Cursor management ────────────────────────────────────────────────

func (s *RsyslogSender) ensureCursorRows() error {
	for _, t := range []string{"login", "logoff", "ddl"} {
		_, err := s.db.Exec(
			"INSERT IGNORE INTO admintools.rsyslog_cursor (event_type) VALUES (?)", t)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *RsyslogSender) getLastId(eventType string) (uint64, error) {
	var v uint64
	err := s.db.QueryRow(
		"SELECT last_id FROM admintools.rsyslog_cursor WHERE event_type = ?",
		eventType).Scan(&v)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return v, err
}

func (s *RsyslogSender) getLogoffLastTime() (string, error) {
	var v sql.NullString
	err := s.db.QueryRow(
		"SELECT last_time FROM admintools.rsyslog_cursor WHERE event_type = 'logoff'").Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if !v.Valid {
		return "", nil
	}
	return v.String, nil
}

func (s *RsyslogSender) updateLastId(eventType string, id int64) error {
	_, err := s.db.Exec(
		"UPDATE admintools.rsyslog_cursor SET last_id = ?, updated_at = NOW(6) WHERE event_type = ?",
		id, eventType)
	return err
}

func (s *RsyslogSender) updateLogoffCursor(t string, id uint64) error {
	_, err := s.db.Exec(
		"UPDATE admintools.rsyslog_cursor SET last_id = ?, last_time = ?, updated_at = NOW(6) "+
			"WHERE event_type = 'logoff'",
		id, t)
	return err
}

// ── helpers ──────────────────────────────────────────────────────────

func dashIfNull(ns sql.NullString) string {
	if !ns.Valid || ns.String == "" {
		return "-"
	}
	return ns.String
}
