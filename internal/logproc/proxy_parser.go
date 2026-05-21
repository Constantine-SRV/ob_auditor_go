package logproc

import (
	"regexp"
	"strconv"
	"strings"
	"sync"

	"obauditor/internal/model"
)

// ObProxyLineParser — парсер obproxy.log. STATEFUL: отслеживает
// незавершённые сессии между строками внутри одного файла.
//
// РЕЗУЛЬТАТ ЛОГИНА определяется по client_response_bytes в строке
// "update_cmd_stats sql=OB_MYSQL_COM_LOGIN":
//
//	≤ 60 байт → MySQL OK packet              → LOGIN_OK
//	>  60 байт → MySQL ERR packet (Access denied и т.п.) → LOGIN_FAIL
//
// Поток событий:
//  1. LOGIN (OK или FAIL) — по cs_id:
//     Шаг 1: "server session born"           → bornMap (cs_id, cluster, tenant, user)
//     Шаг 2: "update_cmd_stats" + "OB_MYSQL_COM_LOGIN" → анализируем
//     client_response_bytes и эмитим LOGIN_OK или LOGIN_FAIL.
//
//  2. LEGACY-путь LOGIN_FAIL (старые версии OBProxy без update_cmd_stats):
//     "error_transfer" + "OB_MYSQL_COM_LOGIN" → failMap (cs_id, ts, client_ip)
//     "client session do_io_close" → если cs_id в failMap → LOGIN_FAIL.
//     Также: если cs_id всё ещё в bornMap (т.е. update_cmd_stats так и не пришла)
//     → эмитим LOGIN_FAIL по факту close.
//
//  3. LOGOFF — одна строка:
//     "handle_server_connection_break" + "COM_QUIT"
type ObProxyLineParser struct {
	bornMap map[uint64]*bornInfo
	failMap map[uint64]*failInfo
}

const okPacketMaxBytes = 60

type bornInfo struct {
	cluster string
	tenant  string
	user    string
}

type failInfo struct {
	timestamp string
	clientIp  string
}

func NewObProxyLineParser() *ObProxyLineParser {
	return &ObProxyLineParser{
		bornMap: make(map[uint64]*bornInfo),
		failMap: make(map[uint64]*failInfo),
	}
}

// ── Список служебных пользователей ───────────────────────────────────

var (
	proxyIgnoredUsersMu sync.RWMutex
	proxyIgnoredUsers   = map[string]struct{}{
		"ocp_monitor": {}, "proxy_ro": {}, "proxyro": {},
	}
)

func SetProxyIgnoredUsers(users []string) {
	m := make(map[string]struct{}, len(users))
	for _, u := range users {
		m[u] = struct{}{}
	}
	proxyIgnoredUsersMu.Lock()
	proxyIgnoredUsers = m
	proxyIgnoredUsersMu.Unlock()
}

func isProxyIgnoredUser(u string) bool {
	if u == "" {
		return false
	}
	proxyIgnoredUsersMu.RLock()
	defer proxyIgnoredUsersMu.RUnlock()
	_, ok := proxyIgnoredUsers[u]
	return ok
}

// ── Ключевые слова и регулярки ───────────────────────────────────────

const (
	kwBorn      = "server session born"
	kwCmdStats  = "update_cmd_stats"
	kwLoginSQL  = "OB_MYSQL_COM_LOGIN"
	kwErrXfer   = "error_transfer"
	kwDoClose   = "client session do_io_close"
	kwLogoff    = "handle_server_connection_break"
	kwQuit      = "COM_QUIT"
)

var (
	pxPTimestamp    = regexp.MustCompile(`^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\]`)
	pxPCsId         = regexp.MustCompile(`\bcs_id=(\d+)`)
	pxPSmId         = regexp.MustCompile(`\bsm_id=(\d+)`)
	pxPClusterBorn  = regexp.MustCompile(`cluster_name:"([^"]+)"`)
	pxPTenantBorn   = regexp.MustCompile(`tenant_name:"([^"]+)"`)
	pxPUserBorn     = regexp.MustCompile(`user_name:"([^"]+)"`)
	pxPIsProxyMysql = regexp.MustCompile(`\bis_proxy_mysql_client:(\d)`)
	pxPProxySessid  = regexp.MustCompile(`\bproxy_sessid=(\d+)`)
	pxPServerSessid = regexp.MustCompile(`\bserver_sessid=(\d+)`)
	pxPServerIp     = regexp.MustCompile(`\bserver_ip=\{(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):\d+\}`)
	pxPClientIp     = regexp.MustCompile(`\bclient_ip=\{([^}]+)\}`)
	pxPUserFlat     = regexp.MustCompile(`\buser_name=([^,)]+)`)
	pxPTenantFlat   = regexp.MustCompile(`\btenant_name=([^,)]+)`)
	pxPClusterFlat  = regexp.MustCompile(`\bcluster_name=([^,)]+)`)
	pxPClientResp   = regexp.MustCompile(`\bclient_response_bytes:(\d+)`)
	pxPLastSessid   = regexp.MustCompile(`\blast_server_sessid=(\d+)`)
	pxPCloseCluster = regexp.MustCompile(`\bcluster=([^,)]+)`)
	pxPCloseTenant  = regexp.MustCompile(`\btenant=([^,)]+)`)
	pxPCloseUser    = regexp.MustCompile(`\buser=([^,)]+)`)
	pxPProxyUser    = regexp.MustCompile(`proxy_user_name=([^,]+)`)
)

// Parse — основной метод, разбирает одну строку obproxy.log.
func (p *ObProxyLineParser) Parse(line string) *model.LoginEvent {
	switch {
	case strings.Contains(line, kwBorn):
		p.handleBorn(line)
		return nil
	case strings.Contains(line, kwCmdStats) && strings.Contains(line, kwLoginSQL):
		return p.handleLoginCmdStats(line)
	case strings.Contains(line, kwErrXfer) && strings.Contains(line, kwLoginSQL):
		p.handleErrorTransfer(line)
		return nil
	case strings.Contains(line, kwDoClose):
		return p.handleDoClose(line)
	case strings.Contains(line, kwLogoff) && strings.Contains(line, kwQuit):
		return p.handleLogoff(line)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────

func (p *ObProxyLineParser) handleBorn(line string) {
	csIdStr := extractFirstGroup(pxPCsId, line)
	if csIdStr == "" {
		return
	}
	csId, err := strconv.ParseUint(csIdStr, 10, 64)
	if err != nil {
		return
	}

	// Внутренний пробник OBProxy (detect_user и подобные) — отсев по флагу.
	if extractFirstGroup(pxPIsProxyMysql, line) == "1" {
		return
	}

	info := &bornInfo{
		cluster: extractFirstGroup(pxPClusterBorn, line),
		tenant:  extractFirstGroup(pxPTenantBorn, line),
		user:    extractFirstGroup(pxPUserBorn, line),
	}

	if info.tenant == "" {
		return
	}
	if isProxyIgnoredUser(info.user) {
		return
	}

	p.bornMap[csId] = info
}

// handleLoginCmdStats — главный обработчик результата логина.
// Эмитит LOGIN_OK или LOGIN_FAIL по значению client_response_bytes.
func (p *ObProxyLineParser) handleLoginCmdStats(line string) *model.LoginEvent {
	csIdStr := extractFirstGroup(pxPCsId, line)
	if csIdStr == "" {
		return nil
	}
	csId, err := strconv.ParseUint(csIdStr, 10, 64)
	if err != nil {
		return nil
	}

	born, ok := p.bornMap[csId]
	delete(p.bornMap, csId)
	// born может быть nil если: (а) born-строка не попала в окно чтения,
	// (б) пользователь был отфильтрован в handleBorn.
	if !ok {
		return nil
	}

	respBytesStr := extractFirstGroup(pxPClientResp, line)
	success := false
	if respBytesStr != "" {
		if n, err := strconv.Atoi(respBytesStr); err == nil {
			success = n <= okPacketMaxBytes
		}
	}
	// Если поля нет — считаем фейлом (страховка).

	e := &model.LoginEvent{
		Source:    "PROXY",
		EventTime: extractFirstGroup(pxPTimestamp, line),
		CsId:      &csId,
	}
	if success {
		e.EventType = "LOGIN_OK"
	} else {
		e.EventType = "LOGIN_FAIL"
		code := -1
		e.ErrorCode = &code
	}

	// Имена берём из update_cmd_stats, fallback на born.
	e.UserName = orElse(extractFirstGroup(pxPUserFlat, line), born.user)
	e.TenantName = orElse(extractFirstGroup(pxPTenantFlat, line), born.tenant)
	e.ClusterName = orElse(extractFirstGroup(pxPClusterFlat, line), born.cluster)

	e.ClientIp = stripPort(extractFirstGroup(pxPClientIp, line))
	e.ServerIp = extractFirstGroup(pxPServerIp, line)

	if s := extractFirstGroup(pxPProxySessid, line); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			e.ProxySessionId = &v
		}
	}
	if s := extractFirstGroup(pxPServerSessid, line); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			e.SessionId = &v
		}
	}
	return e
}

// ─────────────────────────────────────────────────────────────────────

func (p *ObProxyLineParser) handleErrorTransfer(line string) {
	var csId uint64
	var ok bool

	if s := extractFirstGroup(pxPSmId, line); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			csId = v
			ok = true
		}
	}
	if !ok {
		if s := extractFirstGroup(pxPCsId, line); s != "" {
			if v, err := strconv.ParseUint(s, 10, 64); err == nil {
				csId = v
				ok = true
			}
		}
	}
	if !ok {
		return
	}

	p.failMap[csId] = &failInfo{
		timestamp: extractFirstGroup(pxPTimestamp, line),
		clientIp:  extractFirstGroup(pxPClientIp, line),
	}
}

// handleDoClose — закрытие клиентской сессии (legacy/страховочные сценарии).
//
//  1. cs_id в failMap → LOGIN_FAIL.
//  2. cs_id всё ещё в bornMap (update_cmd_stats не пришла) → LOGIN_FAIL по факту close.
func (p *ObProxyLineParser) handleDoClose(line string) *model.LoginEvent {
	csIdStr := extractFirstGroup(pxPCsId, line)
	if csIdStr == "" {
		return nil
	}
	csId, err := strconv.ParseUint(csIdStr, 10, 64)
	if err != nil {
		return nil
	}

	fail, hasFail := p.failMap[csId]
	delete(p.failMap, csId)
	born, hasBorn := p.bornMap[csId]
	delete(p.bornMap, csId)

	if !hasFail && !hasBorn {
		return nil
	}

	code := -1
	e := &model.LoginEvent{
		Source:      "PROXY",
		EventType:   "LOGIN_FAIL",
		ErrorCode:   &code,
		CsId:        &csId,
		ClusterName: extractFirstGroup(pxPCloseCluster, line),
		TenantName:  extractFirstGroup(pxPCloseTenant, line),
		UserName:    extractFirstGroup(pxPCloseUser, line),
	}
	if s := extractFirstGroup(pxPProxySessid, line); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			e.ProxySessionId = &v
		}
	}
	if s := extractFirstGroup(pxPLastSessid, line); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			e.SessionId = &v
		}
	}

	if hasFail {
		e.EventTime = fail.timestamp
		e.ClientIp = fail.clientIp
	} else {
		e.EventTime = extractFirstGroup(pxPTimestamp, line)
		// fallback из born если в close-строке нет
		if e.ClusterName == "" {
			e.ClusterName = born.cluster
		}
		if e.TenantName == "" {
			e.TenantName = born.tenant
		}
		if e.UserName == "" {
			e.UserName = born.user
		}
	}
	return e
}

// ─────────────────────────────────────────────────────────────────────

func (p *ObProxyLineParser) handleLogoff(line string) *model.LoginEvent {
	proxyUser := extractFirstGroup(pxPProxyUser, line)
	if proxyUser == "" {
		return nil
	}

	// Парсинг user@tenant#cluster
	var user, tenant, cluster string
	if atIdx := strings.Index(proxyUser, "@"); atIdx > 0 {
		user = proxyUser[:atIdx]
		rest := proxyUser[atIdx+1:]
		if hashIdx := strings.Index(rest, "#"); hashIdx > 0 {
			tenant = rest[:hashIdx]
			cluster = rest[hashIdx+1:]
		} else {
			tenant = rest
		}
	}

	if tenant == "" {
		return nil
	}
	if isProxyIgnoredUser(user) {
		return nil
	}

	e := &model.LoginEvent{
		Source:      "PROXY",
		EventType:   "LOGOFF",
		EventTime:   extractFirstGroup(pxPTimestamp, line),
		UserName:    user,
		TenantName:  tenant,
		ClusterName: cluster,
		ClientIp:    stripPort(extractFirstGroup(pxPClientIp, line)),
	}

	if s := extractFirstGroup(pxPCsId, line); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			e.CsId = &v
		}
	}
	if s := extractFirstGroup(pxPProxySessid, line); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			e.ProxySessionId = &v
		}
	}
	return e
}

// ─────────────────────────────────────────────────────────────────────
// helpers

func orElse(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// stripPort: "192.168.73.31:49494" → "192.168.73.31" (только для IPv4).
func stripPort(addr string) string {
	if addr == "" {
		return ""
	}
	colon := strings.LastIndex(addr, ":")
	if colon > 0 && strings.Count(addr, ":") == 1 {
		return addr[:colon]
	}
	return addr
}
