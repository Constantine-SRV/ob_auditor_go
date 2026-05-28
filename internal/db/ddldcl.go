package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"obauditor/internal/logging"
)

// DdlDclAuditDao — сбор DDL/DCL событий из GV$OB_SQL_AUDIT в ddl_dcl_audit_log (v2).
//
// Курсор: per-server-tenant строка в admintools.ddl_dcl_audit_checkpoint.
// Продвигается по (request_time + elapsed_time) — момент попадания записи
// в audit-буфер, а не момент старта запроса. Это решает проблему long-running
// DDL, которые в v1 терялись (старт раньше курсора, попадание в буфер позже).
//
// Алгоритм одного прогона (соответствует sp_collect_ddl_dcl_audit_v2):
//
//  1. Sync: INSERT IGNORE из DBA_OB_UNITS — новые юниты появляются с last_end=0
//  2. Ghost snapshot: те строки checkpoint, которых нет в DBA_OB_UNITS,
//     фиксируем в памяти (map). Их обработаем на этом прогоне ещё раз
//     (заберём остатки), а потом удалим из checkpoint
//  3. Загружаем все строки checkpoint (живые + ghost)
//  4. Цикл по юнитам:
//     - COUNT + MAX(request_time + elapsed_time) с фильтром
//     request_time + elapsed_time > last_end_time
//     - если есть новые → INSERT IGNORE + UPDATE checkpoint(last_end_time, updated_at)
//     - иначе → только UPDATE checkpoint(updated_at)  (heartbeat)
//  5. Cleanup: удаляем из checkpoint все строки, которые были ghost в snapshot
//
// Корректность при сбоях:
//   - INSERT упал → checkpoint НЕ обновился, на следующем прогоне повторим
//     то же окно; UNIQUE KEY (svr_ip, request_id) отбросит дубликаты.
//   - UPDATE checkpoint упал → INSERT уже прошёл, на следующем прогоне
//     INSERT IGNORE-нёт всё, ROW_COUNT ≈ 0, курсор продвинется.
//
// Режимы (ddlDclAuditMode):
//
//	0 — не запускаем (проверяется в main)
//	1 — основной: собирает всегда
//	2 — резервный: только если MAX(updated_at) в checkpoint старше 2 минут
type DdlDclAuditDao struct {
	db  *sql.DB
	log *logging.Logger
}

const fallbackThresholdSec = 120

func NewDdlDclAuditDao(db *sql.DB, log *logging.Logger) *DdlDclAuditDao {
	return &DdlDclAuditDao{db: db, log: log}
}

// auditTarget — динамический объект из ddl_dcl_audit_targets.
type auditTarget struct {
	tenantId   sql.NullInt64
	dbName     sql.NullString
	objectName string
}

// unitCheckpoint — одна строка ddl_dcl_audit_checkpoint.
type unitCheckpoint struct {
	svrIp    string
	svrPort  int64
	tenantId int64
	lastEnd  int64
}

// unitKey — ключ юнита (svr_ip, svr_port, tenant_id).
type unitKey struct {
	svrIp    string
	svrPort  int64
	tenantId int64
}

// ShouldCollectFallback — для режима 2: проверяем что основной коллектор жив.
// В v2 нет одной глобальной строки состояния, поэтому смотрим MAX(updated_at)
// по checkpoint. Если самая свежая отметка старше 2 минут — считаем, что
// основной коллектор где-то лёг, и берём работу на себя.
func (d *DdlDclAuditDao) ShouldCollectFallback() (bool, error) {
	var updatedAt sql.NullTime
	err := d.db.QueryRow(
		"SELECT MAX(updated_at) FROM admintools.ddl_dcl_audit_checkpoint",
	).Scan(&updatedAt)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	if !updatedAt.Valid {
		d.log.Debugf("[DdlDclAuditDao] checkpoint empty / updated_at NULL → will collect (fallback)")
		return true, nil
	}
	ageMs := time.Since(updatedAt.Time).Milliseconds()
	stale := ageMs > fallbackThresholdSec*1000
	if stale {
		d.log.Debugf("[DdlDclAuditDao] max(updated_at) age=%d ms → will collect (fallback)", ageMs)
	} else {
		d.log.Debugf("[DdlDclAuditDao] max(updated_at) age=%d ms → skip (primary alive)", ageMs)
	}
	return stale, nil
}

// Collect — основная точка входа. Возвращает суммарное число вставленных строк.
func (d *DdlDclAuditDao) Collect() (int64, error) {
	startTime := time.Now()

	// 1. Sync списка юнитов из DBA_OB_UNITS.
	unitsNew, err := d.syncUnits()
	if err != nil {
		return 0, fmt.Errorf("syncUnits: %w", err)
	}
	if unitsNew > 0 {
		d.log.Debugf("[DdlDclAuditDao] Added %d new unit(s) to checkpoint", unitsNew)
	}

	// 2. Snapshot ghost-юнитов (в checkpoint, нет в DBA_OB_UNITS).
	ghosts, err := d.snapshotGhostUnits()
	if err != nil {
		return 0, fmt.Errorf("snapshotGhostUnits: %w", err)
	}

	// 3. Загружаем dynamic targets и строим INSERT-шаблон.
	targets, err := d.loadTargets()
	if err != nil {
		return 0, fmt.Errorf("loadTargets: %w", err)
	}
	d.log.Debugf("[DdlDclAuditDao] custom targets: %d", len(targets))
	insertSQL := buildInsertSQL(targets)

	// 4. Загружаем все строки checkpoint.
	checkpoints, err := d.loadCheckpoints()
	if err != nil {
		return 0, fmt.Errorf("loadCheckpoints: %w", err)
	}

	if d.log.IsDebug() && len(checkpoints) > 0 {
		d.log.Debugf("[DdlDclAuditDao] checkpoint rows: %d (ghosts: %d, new: %d)",
			len(checkpoints), len(ghosts), unitsNew)
	}

	// 5. Цикл по юнитам.
	var insertedTotal, rowsScannedTotal int64
	var unitsWithData int64

	for _, cp := range checkpoints {
		key := unitKey{cp.svrIp, cp.svrPort, cp.tenantId}
		_, isGhost := ghosts[key]
		status := "LIVE"
		if isGhost {
			status = "GHOST_PURGED"
		}

		unitStart := time.Now()
		rowsScanned, newEnd, hasNew, err := d.getUnitWindow(cp)
		if err != nil {
			d.log.Errorf("[DdlDclAuditDao] getUnitWindow %s:%d/%d: %v",
				cp.svrIp, cp.svrPort, cp.tenantId, err)
			continue
		}
		rowsScannedTotal += rowsScanned

		var insertedUnit int64
		if hasNew {
			res, err := d.db.Exec(insertSQL,
				cp.svrIp, cp.svrPort, cp.tenantId, cp.lastEnd, newEnd)
			if err != nil {
				d.log.Errorf("[DdlDclAuditDao] insert for unit %s:%d/%d: %v",
					cp.svrIp, cp.svrPort, cp.tenantId, err)
				// checkpoint НЕ обновляем — на следующем прогоне повторим окно
				continue
			}
			insertedUnit, _ = res.RowsAffected()
			insertedTotal += insertedUnit
			unitsWithData++

			if err := d.updateCheckpoint(cp.svrIp, cp.svrPort, cp.tenantId, newEnd); err != nil {
				d.log.Errorf("[DdlDclAuditDao] updateCheckpoint %s:%d/%d: %v",
					cp.svrIp, cp.svrPort, cp.tenantId, err)
			}
		} else {
			// Heartbeat: ничего нового, продвигаем только updated_at.
			if err := d.heartbeatCheckpoint(cp.svrIp, cp.svrPort, cp.tenantId); err != nil {
				d.log.Errorf("[DdlDclAuditDao] heartbeatCheckpoint %s:%d/%d: %v",
					cp.svrIp, cp.svrPort, cp.tenantId, err)
			}
		}

		if d.log.IsDebug() {
			endStr := "NULL"
			if hasNew {
				endStr = fmt.Sprintf("%d", newEnd)
			}
			d.log.Debugf("[DdlDclAuditDao] unit=%s:%d/%d status=%s last_end_before=%d last_end_after=%s rows_scanned=%d inserted=%d duration_ms=%d",
				cp.svrIp, cp.svrPort, cp.tenantId, status,
				cp.lastEnd, endStr, rowsScanned, insertedUnit,
				time.Since(unitStart).Milliseconds())
		}
	}

	// 6. Cleanup ghost-юнитов.
	var unitsGhostPurged int64
	if len(ghosts) > 0 {
		unitsGhostPurged, err = d.cleanupGhosts(ghosts)
		if err != nil {
			d.log.Errorf("[DdlDclAuditDao] cleanupGhosts: %v", err)
		}
	}

	durationMs := time.Since(startTime).Milliseconds()
	// Пер-прогонная статистика — только в DEBUG. В INFO агрегат уходит в
	// сводную строку [stats] (см. пакет daemon).
	d.log.Debugf("[DdlDclAuditDao] Done: inserted=%d rows_scanned=%d units_total=%d units_new=%d units_with_data=%d units_ghost=%d duration_ms=%d",
		insertedTotal, rowsScannedTotal, len(checkpoints),
		unitsNew, unitsWithData, unitsGhostPurged, durationMs)
	return insertedTotal, nil
}

// ─────────────────────────────────────────────────────────────────────
// Шаги алгоритма
// ─────────────────────────────────────────────────────────────────────

// syncUnits — Шаг 1: INSERT IGNORE из DBA_OB_UNITS.
// Возвращает количество добавленных новых юнитов.
func (d *DdlDclAuditDao) syncUnits() (int64, error) {
	var before int64
	if err := d.db.QueryRow(
		"SELECT COUNT(*) FROM admintools.ddl_dcl_audit_checkpoint",
	).Scan(&before); err != nil {
		return 0, err
	}

	_, err := d.db.Exec(
		"INSERT IGNORE INTO admintools.ddl_dcl_audit_checkpoint " +
			"(svr_ip, svr_port, tenant_id, last_end_time) " +
			"SELECT svr_ip, svr_port, tenant_id, 0 " +
			"FROM oceanbase.DBA_OB_UNITS WHERE status = 'ACTIVE'",
	)
	if err != nil {
		return 0, err
	}

	var after int64
	if err := d.db.QueryRow(
		"SELECT COUNT(*) FROM admintools.ddl_dcl_audit_checkpoint",
	).Scan(&after); err != nil {
		return 0, err
	}
	return after - before, nil
}

// snapshotGhostUnits — Шаг 2: ghost = в checkpoint, нет в DBA_OB_UNITS.
// Возвращает set юнитов, которых больше нет в кластере, но строки
// в checkpoint ещё висят.
func (d *DdlDclAuditDao) snapshotGhostUnits() (map[unitKey]struct{}, error) {
	rows, err := d.db.Query(
		"SELECT c.svr_ip, c.svr_port, c.tenant_id " +
			"FROM admintools.ddl_dcl_audit_checkpoint c " +
			"LEFT JOIN oceanbase.DBA_OB_UNITS u " +
			"  ON u.svr_ip = c.svr_ip AND u.svr_port = c.svr_port " +
			" AND u.tenant_id = c.tenant_id AND u.status = 'ACTIVE' " +
			"WHERE u.tenant_id IS NULL",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[unitKey]struct{})
	for rows.Next() {
		var k unitKey
		if err := rows.Scan(&k.svrIp, &k.svrPort, &k.tenantId); err != nil {
			return nil, err
		}
		result[k] = struct{}{}
	}
	return result, rows.Err()
}

// loadCheckpoints — Шаг 4: загружаем все строки checkpoint в память.
func (d *DdlDclAuditDao) loadCheckpoints() ([]unitCheckpoint, error) {
	rows, err := d.db.Query(
		"SELECT svr_ip, svr_port, tenant_id, last_end_time " +
			"FROM admintools.ddl_dcl_audit_checkpoint " +
			"ORDER BY svr_ip, svr_port, tenant_id",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []unitCheckpoint
	for rows.Next() {
		var cp unitCheckpoint
		if err := rows.Scan(&cp.svrIp, &cp.svrPort, &cp.tenantId, &cp.lastEnd); err != nil {
			return nil, err
		}
		result = append(result, cp)
	}
	return result, rows.Err()
}

// getUnitWindow — Шаг 5 (часть): для одного юнита считаем COUNT(*) и
// MAX(request_time + elapsed_time) в окне строк, не охваченных курсором.
//
// Возвращает: (rowsScanned, newEnd, hasNew, err).
// hasNew=false если новых строк нет (MAX вернул NULL).
//
// rowsScanned — это число строк в окне ДО применения DDL/DCL-фильтров.
// Используется как метрика для оценки selectivity (сколько мы вообще
// прошерстили против сколько вставили).
func (d *DdlDclAuditDao) getUnitWindow(cp unitCheckpoint) (int64, int64, bool, error) {
	var count int64
	var maxEnd sql.NullInt64
	err := d.db.QueryRow(
		"SELECT COUNT(*), MAX(request_time + elapsed_time) "+
			"FROM oceanbase.GV$OB_SQL_AUDIT "+
			"WHERE svr_ip = ? AND svr_port = ? AND tenant_id = ? "+
			"  AND is_inner_sql = 0 "+
			"  AND request_time + elapsed_time > ?",
		cp.svrIp, cp.svrPort, cp.tenantId, cp.lastEnd,
	).Scan(&count, &maxEnd)
	if err != nil {
		return 0, 0, false, err
	}
	if !maxEnd.Valid {
		return count, 0, false, nil
	}
	return count, maxEnd.Int64, true, nil
}

// updateCheckpoint — продвинуть курсор для юнита.
func (d *DdlDclAuditDao) updateCheckpoint(svrIp string, svrPort, tenantId, newEnd int64) error {
	_, err := d.db.Exec(
		"UPDATE admintools.ddl_dcl_audit_checkpoint "+
			"SET last_end_time = ?, updated_at = NOW(6) "+
			"WHERE svr_ip = ? AND svr_port = ? AND tenant_id = ?",
		newEnd, svrIp, svrPort, tenantId,
	)
	return err
}

// heartbeatCheckpoint — обновить только updated_at (новых данных нет).
func (d *DdlDclAuditDao) heartbeatCheckpoint(svrIp string, svrPort, tenantId int64) error {
	_, err := d.db.Exec(
		"UPDATE admintools.ddl_dcl_audit_checkpoint "+
			"SET updated_at = NOW(6) "+
			"WHERE svr_ip = ? AND svr_port = ? AND tenant_id = ?",
		svrIp, svrPort, tenantId,
	)
	return err
}

// cleanupGhosts — Шаг 6: удалить ghost-строки из checkpoint.
// На этом прогоне они уже получили свой последний шанс забрать остатки.
func (d *DdlDclAuditDao) cleanupGhosts(ghosts map[unitKey]struct{}) (int64, error) {
	var total int64
	for k := range ghosts {
		res, err := d.db.Exec(
			"DELETE FROM admintools.ddl_dcl_audit_checkpoint "+
				"WHERE svr_ip = ? AND svr_port = ? AND tenant_id = ?",
			k.svrIp, k.svrPort, k.tenantId,
		)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += n
	}
	return total, nil
}

// loadTargets — динамические targets для расширенного аудита.
func (d *DdlDclAuditDao) loadTargets() ([]auditTarget, error) {
	rows, err := d.db.Query(
		"SELECT tenant_id, db_name, object_name " +
			"FROM admintools.ddl_dcl_audit_targets WHERE is_active = 1 ORDER BY id",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []auditTarget
	for rows.Next() {
		var t auditTarget
		if err := rows.Scan(&t.tenantId, &t.dbName, &t.objectName); err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

// ─────────────────────────────────────────────────────────────────────
// Построение SQL
// ─────────────────────────────────────────────────────────────────────

// buildTargetCondition — собирает условие LIKE для одного target.
//
// Значения подставляются прямо в SQL: targets — служебная таблица
// admintools, значения вносит администратор. Кавычки экранируются на
// всякий случай.
func buildTargetCondition(t auditTarget) string {
	obj := strings.ReplaceAll(t.objectName, "'", "\\'")
	if t.dbName.Valid && t.dbName.String != "" {
		dbn := strings.ReplaceAll(t.dbName.String, "'", "\\'")
		return fmt.Sprintf("(query_sql LIKE '%%%s.%s%%' OR (db_name = '%s' AND query_sql LIKE '%%%s%%'))",
			dbn, obj, dbn, obj)
	}
	return fmt.Sprintf("query_sql LIKE '%%%s%%'", obj)
}

// buildInsertSQL — INSERT IGNORE для одного юнита (svr_ip, svr_port, tenant_id).
// 5 placeholder-ов: svr_ip, svr_port, tenant_id, last_end, new_end.
//
// Окно по времени — полуоткрытый интервал по (request_time + elapsed_time):
//
//	(last_end, new_end]
//
// то есть по моменту попадания записи в audit-буфер, а не по моменту
// старта запроса. Это критично для long-running DDL.
func buildInsertSQL(targets []auditTarget) string {
	var sb strings.Builder
	sb.WriteString(
		"INSERT IGNORE INTO admintools.ddl_dcl_audit_log (" +
			"  request_id, svr_ip, tenant_id, tenant_name," +
			"  user_id, user_name, proxy_user," +
			"  client_ip, user_client_ip, sid, db_name," +
			"  stmt_type, query_sql," +
			"  ret_code, affected_rows, request_ts, elapsed_time, retry_cnt" +
			") " +
			"SELECT" +
			"  request_id, svr_ip, tenant_id, tenant_name," +
			"  user_id, user_name, proxy_user," +
			"  client_ip, user_client_ip, sid, db_name," +
			"  stmt_type," +
			"  REGEXP_REPLACE(query_sql, '^[[:space:]]*/[*].*?[*]/[[:space:]]*', '')," +
			"  ret_code, affected_rows, usec_to_time(request_time), elapsed_time, retry_cnt" +
			" FROM oceanbase.GV$OB_SQL_AUDIT" +
			" WHERE svr_ip = ?" +
			"   AND svr_port = ?" +
			"   AND tenant_id = ?" +
			"   AND is_inner_sql = 0" +
			"   AND request_time + elapsed_time >  ?" +
			"   AND request_time + elapsed_time <= ?" +
			"   AND stmt_type NOT IN ('VARIABLE_SET')" +
			// ── Глобальные исключения — наши собственные служебные запросы. ──
			"   AND query_sql NOT LIKE '%INSERT IGNORE INTO admintools.ddl_dcl_audit_log%'" +
			"   AND query_sql NOT LIKE '%UPDATE sessions SET logoff_time%'" +
			"   AND query_sql NOT LIKE '%UPDATE sessions p%JOIN sessions s%'" +
			"   AND query_sql NOT LIKE '%sp_collect_ddl_dcl_audit%'" +
			"   AND query_sql NOT LIKE '%ddl_dcl_audit_checkpoint%'" +
			"   AND (",
	)

	// ── Хардкод DDL/DCL stmt_type ──
	sb.WriteString(
		"     stmt_type IN (" +
			"       'CREATE_TABLE','ALTER_TABLE','DROP_TABLE'," +
			"       'CREATE_INDEX','DROP_INDEX'," +
			"       'CREATE_VIEW','DROP_VIEW'," +
			"       'CREATE_DATABASE','DROP_DATABASE'," +
			"       'TRUNCATE_TABLE','RENAME_TABLE'," +
			"       'CREATE_TENANT','DROP_TENANT'," +
			"       'DROP_USER','RENAME_USER'," +
			"       'GRANT','REVOKE'," +
			"       'ALTER_USER','SET_PASSWORD'" +
			"     )" +
			// ── User management через LIKE (нет отдельного stmt_type) ──
			"     OR (" +
			"         query_sql LIKE '%CREATE USER%'" +
			"         OR query_sql LIKE '%ALTER USER%'" +
			"         OR query_sql LIKE '%lock_user(%'" +
			"         OR query_sql LIKE '%unlock_user(%'" +
			"     )" +
			// ── DELETE/UPDATE таблиц аудита (security tripwire) ──
			"     OR (" +
			"       stmt_type IN ('DELETE', 'UPDATE')" +
			"       AND (" +
			"         query_sql LIKE '%admintools.sessions%'" +
			"         OR query_sql LIKE '%admintools.ddl_dcl_audit_log%'" +
			"         OR (db_name = 'admintools' AND query_sql LIKE '%sessions%')" +
			"         OR (db_name = 'admintools' AND query_sql LIKE '%ddl_dcl_audit_log%')" +
			"         OR query_sql LIKE '%admintools.ddl_dcl_audit_targets%'" +
			"         OR (db_name = 'admintools' AND query_sql LIKE '%ddl_dcl_audit_targets%')" +
			"         OR query_sql LIKE '%admintools.ddl_dcl_audit_checkpoint%'" +
			"         OR (db_name = 'admintools' AND query_sql LIKE '%ddl_dcl_audit_checkpoint%')" +
			"       )" +
			"     )",
	)

	// ── Динамические targets ──
	for _, t := range targets {
		sb.WriteString("\n     OR ")
		sb.WriteString(buildTargetCondition(t))
	}

	sb.WriteString("   )")
	return sb.String()
}
