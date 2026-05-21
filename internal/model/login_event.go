// Package model — структуры данных, разделяемые между парсерами и DAO.
package model

import "fmt"

// LoginEvent — событие входа/выхода пользователя OceanBase.
//
// EventType: LOGIN_OK | LOGIN_FAIL | LOGOFF
// Source:    SERVER   | PROXY
//
// Поля Int/Long из Java становятся *int/*uint64 для допустимости nil.
// proxy_sessid хранится как uint64 — может превышать int64.MaxValue.
type LoginEvent struct {
	EventType string // LOGIN_OK, LOGIN_FAIL, LOGOFF
	Source    string // SERVER, PROXY
	EventTime string // "2026-03-10 10:20:29.213321"

	// Общие поля
	ClientIp   string
	TenantName string
	UserName   string
	SessionId  *uint64 // sessid (SERVER) или server_sessid (PROXY)
	ErrorCode  *int    // nil для OK и LOGOFF

	// SERVER-only
	Ssl         string  // "Y" / "N"
	ClientType  string  // JDBC, JAVA, OCI, OBCLIENT, MYSQL_CLI
	ProxySessid *uint64 // proxy_sessid из строки SERVER
	FromProxy   *bool   // from_proxy=true/false (SERVER)

	// PROXY-only
	ClusterName    string
	CsId           *uint64
	ProxySessionId *uint64 // proxy_sessid из PROXY-лога

	// IP OBServer-узла из тела строки лога:
	//   SERVER: не заполняется здесь (берётся из первой строки файла)
	//   PROXY:  заполняется из server_ip={...:port} в строке update_cmd_stats
	ServerIp string
}

// EffectiveProxySessid возвращает либо ProxySessid (SERVER-источник),
// либо ProxySessionId (PROXY-источник). Аналог Java:
//
//	event.proxySessid != null ? event.proxySessid : event.proxySessionId
func (e *LoginEvent) EffectiveProxySessid() *uint64 {
	if e.ProxySessid != nil {
		return e.ProxySessid
	}
	return e.ProxySessionId
}

func (e *LoginEvent) String() string {
	ip := dashIfEmpty(e.ClientIp)
	tn := dashIfEmpty(e.TenantName)
	un := dashIfEmpty(e.UserName)
	sess := "<nil>"
	if e.SessionId != nil {
		sess = fmt.Sprintf("%d", *e.SessionId)
	}
	return fmt.Sprintf("[%s] %s | %s | ip=%-20s tenant=%-12s user=%-16s sessid=%s",
		e.Source, e.EventType, e.EventTime, ip, tn, un, sess)
}

func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
