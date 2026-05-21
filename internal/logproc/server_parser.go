// Package logproc — обработка лог-файлов OceanBase.
package logproc

import (
	"regexp"
	"strconv"
	"strings"
	"sync"

	"obauditor/internal/model"
)

// ObServerLineParser — парсер строк observer.log. Stateless.
//
//  1. LOGIN (OK или FAIL):
//     MySQL LOGIN(direct_client_ip="...", client_ip=..., tenant_name=..., user_name=...,
//     sessid=..., proxy_sessid=..., use_ssl=..., proc_ret=...,
//     from_proxy=..., from_java_client=..., from_jdbc_client=..., from_oci_client=...,
//     conn->client_type_=...)
//
//  2. LOGOFF:
//     connection close(sessid=..., proxy_sessid=..., tenant_id=..., from_proxy=...)

// Список служебных пользователей — обновляется один раз при старте через SetIgnoredUsers.
var (
	serverIgnoredUsersMu sync.RWMutex
	serverIgnoredUsers   = map[string]struct{}{
		"ocp_monitor": {}, "proxy_ro": {}, "proxyro": {},
	}
)

// SetServerIgnoredUsers — задаёт список игнорируемых пользователей из конфигурации.
func SetServerIgnoredUsers(users []string) {
	m := make(map[string]struct{}, len(users))
	for _, u := range users {
		m[u] = struct{}{}
	}
	serverIgnoredUsersMu.Lock()
	serverIgnoredUsers = m
	serverIgnoredUsersMu.Unlock()
}

func isServerIgnoredUser(u string) bool {
	if u == "" {
		return false
	}
	serverIgnoredUsersMu.RLock()
	defer serverIgnoredUsersMu.RUnlock()
	_, ok := serverIgnoredUsers[u]
	return ok
}

// ─────────────────────────────────────────────────────────────────────

var (
	srvPTimestamp = regexp.MustCompile(`^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\]`)

	// direct_client_ip — реальный IP источника (при proxy = IP клиента, не proxy)
	srvPDirectClientIp = regexp.MustCompile(`\bdirect_client_ip=(?:"([^"]+)"|([^,)]+))`)
	srvPClientIp       = regexp.MustCompile(`\bclient_ip=(?:"([^"]+)"|([^,)]+))`)
	srvPTenant         = regexp.MustCompile(`\btenant_name=([^,)]+)`)
	srvPUser           = regexp.MustCompile(`\buser_name=([^,)]+)`)
	srvPSessid         = regexp.MustCompile(`\bsessid=(\d+)`)
	srvPProxySessid    = regexp.MustCompile(`\bproxy_sessid=(\d+)`)
	srvPSsl            = regexp.MustCompile(`\buse_ssl=(true|false)`)
	srvPProcRet        = regexp.MustCompile(`\bproc_ret=(-?\d+)`)

	// Тип клиента — флаги from_*_client приоритетнее conn->client_type_
	srvPFromJava   = regexp.MustCompile(`\bfrom_java_client=(true|false)`)
	srvPFromJdbc   = regexp.MustCompile(`\bfrom_jdbc_client=(true|false)`)
	srvPFromOci    = regexp.MustCompile(`\bfrom_oci_client=(true|false)`)
	srvPClientType = regexp.MustCompile(`\bconn->client_type_=(\d+)`)

	// from_proxy — присутствует и в LOGIN и в LOGOFF строках
	srvPFromProxy = regexp.MustCompile(`\bfrom_proxy=(true|false)`)
	srvPTenantId  = regexp.MustCompile(`\btenant_id=(\d+)`)
)

// ParseObServerLine — парсит одну строку observer.log.
// Возвращает nil если строка не содержит интересующего события.
func ParseObServerLine(line string) *model.LoginEvent {
	if strings.Contains(line, "MySQL LOGIN") {
		return parseServerLogin(line)
	}
	if strings.Contains(line, "connection close") {
		return parseServerLogoff(line)
	}
	return nil
}

func parseServerLogin(line string) *model.LoginEvent {
	userName := extractFirstGroup(srvPUser, line)
	directClientIp := extractFirstGroup(srvPDirectClientIp, line)

	if isServerIgnoredUser(userName) {
		return nil
	}

	e := &model.LoginEvent{
		Source:    "SERVER",
		EventTime: extractFirstGroup(srvPTimestamp, line),
	}
	// Используем direct_client_ip (реальный источник), fallback на client_ip
	if directClientIp != "" {
		e.ClientIp = directClientIp
	} else {
		e.ClientIp = extractFirstGroup(srvPClientIp, line)
	}
	e.TenantName = extractFirstGroup(srvPTenant, line)
	e.UserName = userName

	if s := extractFirstGroup(srvPSessid, line); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			e.SessionId = &v
		}
	}
	if s := extractFirstGroup(srvPProxySessid, line); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			e.ProxySessid = &v
		}
	}

	sslVal := extractFirstGroup(srvPSsl, line)
	if sslVal == "true" {
		e.Ssl = "Y"
	} else {
		e.Ssl = "N"
	}

	procRet := extractFirstGroup(srvPProcRet, line)
	if procRet == "0" {
		e.EventType = "LOGIN_OK"
	} else {
		e.EventType = "LOGIN_FAIL"
		if procRet != "" {
			if code, err := strconv.Atoi(procRet); err == nil {
				e.ErrorCode = &code
			}
		}
	}

	if fp := extractFirstGroup(srvPFromProxy, line); fp != "" {
		v := fp == "true"
		e.FromProxy = &v
	}

	e.ClientType = resolveClientType(line)
	return e
}

func parseServerLogoff(line string) *model.LoginEvent {
	e := &model.LoginEvent{
		Source:    "SERVER",
		EventType: "LOGOFF",
		EventTime: extractFirstGroup(srvPTimestamp, line),
	}

	if s := extractFirstGroup(srvPSessid, line); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			e.SessionId = &v
		}
	}
	if s := extractFirstGroup(srvPProxySessid, line); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			e.ProxySessid = &v
		}
	}

	// connection close содержит tenant_id (число), не tenant_name
	e.TenantName = extractFirstGroup(srvPTenantId, line)

	if fp := extractFirstGroup(srvPFromProxy, line); fp != "" {
		v := fp == "true"
		e.FromProxy = &v
	}
	return e
}

// resolveClientType — флаги from_*_client приоритетнее client_type_.
// Пример: client_type_=1 (OBCLIENT) но from_jdbc_client=true → JDBC.
func resolveClientType(line string) string {
	if extractFirstGroup(srvPFromJdbc, line) == "true" {
		return "JDBC"
	}
	if extractFirstGroup(srvPFromJava, line) == "true" {
		return "JAVA"
	}
	if extractFirstGroup(srvPFromOci, line) == "true" {
		return "OCI"
	}
	ctype := extractFirstGroup(srvPClientType, line)
	if ctype == "" {
		return ""
	}
	switch ctype {
	case "1":
		return "OBCLIENT"
	case "2":
		return "JDBC"
	case "3":
		return "MYSQL_CLI"
	default:
		return "TYPE_" + ctype
	}
}

// extractFirstGroup — возвращает первую непустую группу совпадения.
// Аналог extractStr() из Java-кода.
func extractFirstGroup(p *regexp.Regexp, line string) string {
	m := p.FindStringSubmatch(line)
	if m == nil {
		return ""
	}
	for i := 1; i < len(m); i++ {
		if m[i] != "" {
			return strings.TrimSpace(m[i])
		}
	}
	return ""
}
