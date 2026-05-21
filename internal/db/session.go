package db

import (
	"database/sql"
	"fmt"

	"obauditor/internal/model"
)

// SessionDao — DAO для таблицы sessions.
//
// INSERT IGNORE — дубли тихо игнорируются благодаря UNIQUE KEY uk_sess.
//
// UNSIGNED BIGINT (proxy_sessid, session_id, cs_id) хранятся как uint64.
// go-sql-driver/mysql корректно работает с uint64 через interpolateParams,
// что мы включили в DSN.
type SessionDao struct {
	db *sql.DB
}

const insertLoginSQL = `
INSERT IGNORE INTO sessions
(source, server_ip, cluster_name, session_id, login_time,
 is_success, client_ip, tenant_name, user_name,
 error_code, ` + "`ssl`" + `, client_type, proxy_sessid, cs_id,
 server_node_ip, from_proxy)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

const loadOpenSQL = `
SELECT proxy_sessid FROM sessions
WHERE server_ip = ? AND logoff_time IS NULL AND is_success = 1
  AND proxy_sessid IS NOT NULL`

const updateLogoffSQL = `
UPDATE sessions SET logoff_time = ?
WHERE proxy_sessid = ? AND logoff_time IS NULL`

// Закрыть прямую сессию (без прокси). cluster_name='' — SERVER-строки всегда
// имеют пустой cluster_name, это позволяет OB использовать UNIQUE KEY полностью.
const updateLogoffDirectSQL = `
UPDATE sessions SET logoff_time = ?
WHERE source = 'SERVER' AND server_ip = ? AND cluster_name = ''
  AND session_id = ? AND logoff_time IS NULL`

// Закрыть PROXY-строки для которых SERVER зафиксировал неудачный вход.
const syncFailedProxySQL = `
UPDATE sessions p
JOIN sessions s
  ON  s.proxy_sessid = p.proxy_sessid
  AND s.source       = 'SERVER'
  AND s.is_success   = 0
  AND s.logoff_time  IS NOT NULL
SET p.logoff_time = s.logoff_time,
    p.is_success  = 0,
    p.error_code  = s.error_code
WHERE p.source      = 'PROXY'
  AND p.logoff_time IS NULL`

func NewSessionDao(db *sql.DB) *SessionDao {
	return &SessionDao{db: db}
}

// InsertLogin — добавить LOGIN_OK или LOGIN_FAIL.
func (d *SessionDao) InsertLogin(e *model.LoginEvent, fileServerIp string) error {
	if e.EventType != "LOGIN_OK" && e.EventType != "LOGIN_FAIL" {
		return nil
	}

	serverNodeIp := ""
	if e.Source == "PROXY" && e.ServerIp != "" {
		serverNodeIp = e.ServerIp
	} else if fileServerIp != "" {
		serverNodeIp = fileServerIp
	}

	isSuccess := 0
	if e.EventType == "LOGIN_OK" {
		isSuccess = 1
	}

	sessionId := uint64Or0(e.SessionId)

	// Какой proxy_sessid использовать (SERVER берёт ProxySessid, PROXY — ProxySessionId)
	proxySessid := e.EffectiveProxySessid()

	_, err := d.db.Exec(insertLoginSQL,
		e.Source,
		fileServerIp,
		e.ClusterName,
		sessionId,
		e.EventTime,
		isSuccess,
		nullStr(e.ClientIp),
		nullStr(e.TenantName),
		nullStr(e.UserName),
		nullIntPtrFromAny(e.ErrorCode),
		nullStr(e.Ssl),
		nullStr(e.ClientType),
		nullUint64Ptr(proxySessid),
		nullUint64Ptr(e.CsId),
		serverNodeIp,
		nullBoolPtr(e.FromProxy),
	)
	return err
}

// LoadOpenProxySessids — proxy_sessid всех открытых успешных сессий для server_ip.
func (d *SessionDao) LoadOpenProxySessids(serverIp string) (map[uint64]struct{}, error) {
	result := make(map[uint64]struct{})
	if serverIp == "" {
		return result, nil
	}
	rows, err := d.db.Query(loadOpenSQL, serverIp)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var v uint64
		if err := rows.Scan(&v); err != nil {
			fmt.Printf("[SessionDao] Cannot parse proxy_sessid: %v\n", err)
			continue
		}
		result[v] = struct{}{}
	}
	return result, rows.Err()
}

// UpdateLogoff — закрыть все открытые сессии с данным proxy_sessid.
// Закрывает сразу обе записи (SERVER и PROXY) одним запросом.
func (d *SessionDao) UpdateLogoff(proxySessid uint64, logoffTime string) (int64, error) {
	res, err := d.db.Exec(updateLogoffSQL, logoffTime, proxySessid)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// UpdateLogoffDirect — закрыть прямую сессию (proxy_sessid=0) по session_id+server_ip.
func (d *SessionDao) UpdateLogoffDirect(sessionId uint64, serverIp, logoffTime string) (int64, error) {
	if serverIp == "" {
		return 0, nil
	}
	res, err := d.db.Exec(updateLogoffDirectSQL, logoffTime, serverIp, sessionId)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// SyncFailedProxySessions — закрыть PROXY-строки для которых SERVER зафиксировал ошибку.
func (d *SessionDao) SyncFailedProxySessions() (int64, error) {
	res, err := d.db.Exec(syncFailedProxySQL)
	if err != nil {
		return 0, err
	}
	updated, _ := res.RowsAffected()
	if updated > 0 {
		fmt.Printf("[SessionDao] syncFailedProxySessions: closed %d PROXY row(s)\n", updated)
	}
	return updated, nil
}

// ── helpers ──────────────────────────────────────────────────────────

func uint64Or0(p *uint64) uint64 {
	if p == nil {
		return 0
	}
	return *p
}

func nullUint64Ptr(p *uint64) any {
	if p == nil {
		return nil
	}
	return *p
}

func nullBoolPtr(p *bool) any {
	if p == nil {
		return nil
	}
	if *p {
		return 1
	}
	return 0
}

func nullIntPtrFromAny(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}
