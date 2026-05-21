package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"obauditor/internal/logging"
)

// DdlDclAuditDao — сбор DDL/DCL событий из GV$OB_SQL_AUDIT в ddl_dcl_audit_log.
//
// Курсор: last_request_time в audit_collector_state (одна глобальная строка id=1).
// Алгоритм:
//  1. last_rt ← audit_collector_state
//  2. new_rt  ← MAX(request_time) из новых строк GV$OB_SQL_AUDIT
//  3. INSERT IGNORE новых DDL/DCL записей
//  4. UPDATE audit_collector_state: last_request_time = new_rt, updated_at = NOW()
//
// Режимы (ddlDclAuditMode):
//
//	0 — не запускаем (проверяется в main)
//	1 — основной: собирает всегда
//	2 — резервный: только если updated_at старше 2 минут (основной упал)
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

// ShouldCollectFallback — для режима 2: проверяем что основной коллектор жив.
func (d *DdlDclAuditDao) ShouldCollectFallback() (bool, error) {
	var updatedAt sql.NullTime
	err := d.db.QueryRow(
		"SELECT updated_at FROM admintools.audit_collector_state WHERE id = 1",
	).Scan(&updatedAt)
	if err == sql.ErrNoRows {
		d.log.Debugf("[DdlDclAuditDao] state row not found — will collect (fallback)")
		return true, nil
	} else if err != nil {
		return false, err
	}
	if !updatedAt.Valid {
		d.log.Debugf("[DdlDclAuditDao] updated_at IS NULL — will collect (fallback)")
		return true, nil
	}
	ageMs := time.Since(updatedAt.Time).Milliseconds()
	stale := ageMs > fallbackThresholdSec*1000
	if stale {
		d.log.Debugf("[DdlDclAuditDao] updated_at age=%d ms → will collect (fallback)", ageMs)
	} else {
		d.log.Debugf("[DdlDclAuditDao] updated_at age=%d ms → skip (primary alive)", ageMs)
	}
	return stale, nil
}

// Collect — основная точка входа. Возвращает количество вставленных строк.
func (d *DdlDclAuditDao) Collect() (int64, error) {
	lastRT, err := d.getLastRequestTime()
	if err != nil {
		return 0, err
	}
	d.log.Debugf("[DdlDclAuditDao] last_request_time=%d", lastRT)

	newRT, hasNew, err := d.getMaxRequestTime(lastRT)
	if err != nil {
		return 0, err
	}
	if !hasNew {
		d.log.Debugf("[DdlDclAuditDao] No new rows in GV$OB_SQL_AUDIT")
		if err := d.updateCollectorState(lastRT); err != nil {
			return 0, err
		}
		return 0, nil
	}
	d.log.Debugf("[DdlDclAuditDao] new_request_time=%d", newRT)

	targets, err := d.loadTargets()
	if err != nil {
		return 0, err
	}
	d.log.Debugf("[DdlDclAuditDao] custom targets: %d", len(targets))

	insertSQL := buildInsertSQL(targets)

	if d.log.IsDebug() {
		debugged := strings.Replace(insertSQL, "request_time > ?",
			fmt.Sprintf("request_time > %d", lastRT), 1)
		debugged = strings.Replace(debugged, "request_time <= ?",
			fmt.Sprintf("request_time <= %d", newRT), 1)
		fmt.Println("[DdlDclAuditDao] SQL:\n" + debugged)
	}

	res, err := d.db.Exec(insertSQL, lastRT, newRT)
	if err != nil {
		return 0, fmt.Errorf("insert audit rows: %w", err)
	}
	inserted, _ := res.RowsAffected()
	if inserted > 0 {
		d.log.Infof("[DdlDclAuditDao] Collected %d DDL/DCL row(s)", inserted)
	}

	if err := d.updateCollectorState(newRT); err != nil {
		return inserted, err
	}
	return inserted, nil
}

func (d *DdlDclAuditDao) getLastRequestTime() (int64, error) {
	var v int64
	err := d.db.QueryRow(
		"SELECT last_request_time FROM admintools.audit_collector_state WHERE id = 1",
	).Scan(&v)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return v, err
}

func (d *DdlDclAuditDao) getMaxRequestTime(lastRT int64) (int64, bool, error) {
	var v sql.NullInt64
	err := d.db.QueryRow(
		"SELECT MAX(request_time) FROM oceanbase.GV$OB_SQL_AUDIT "+
			"WHERE is_inner_sql = 0 AND request_time > ?",
		lastRT,
	).Scan(&v)
	if err != nil {
		return 0, false, err
	}
	if !v.Valid {
		return 0, false, nil
	}
	return v.Int64, true, nil
}

func (d *DdlDclAuditDao) updateCollectorState(newRT int64) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		"UPDATE admintools.audit_collector_state SET last_request_time = ?, updated_at = NOW(6) WHERE id = 1",
		newRT,
	)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

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

// buildTargetCondition — собирает условие LIKE для одного target.
//
// Внимание: значения подставляются прямо в SQL (как в Java-версии).
// Это безопасно потому что targets — служебная таблица admintools,
// и значения туда вносит администратор. Но экранируем кавычки на всякий случай.
func buildTargetCondition(t auditTarget) string {
	obj := strings.ReplaceAll(t.objectName, "'", "\\'")
	if t.dbName.Valid && t.dbName.String != "" {
		db := strings.ReplaceAll(t.dbName.String, "'", "\\'")
		return fmt.Sprintf("(query_sql LIKE '%%%s.%s%%' OR (db_name = '%s' AND query_sql LIKE '%%%s%%'))",
			db, obj, db, obj)
	}
	return fmt.Sprintf("query_sql LIKE '%%%s%%'", obj)
}

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
			" WHERE is_inner_sql = 0" +
			"   AND request_time > ?" +
			"   AND request_time <= ?" +
			"   AND stmt_type NOT IN ('VARIABLE_SET')" +
			// ── Глобальные исключения — наши собственные служебные запросы. ──
			"   AND query_sql NOT LIKE '%INSERT IGNORE INTO admintools.ddl_dcl_audit_log%'" +
			"   AND query_sql NOT LIKE '%UPDATE sessions SET logoff_time%'" +
			"   AND query_sql NOT LIKE '%UPDATE sessions p JOIN sessions s%'" +
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
			// ── user management через LIKE (нет отдельного stmt_type) ──
			"     OR (" +
			"         query_sql LIKE '%CREATE USER%'" +
			"         OR query_sql LIKE '%ALTER USER%'" +
			"         OR query_sql LIKE '%lock_user(%'" +
			"         OR query_sql LIKE '%unlock_user(%'" +
			"     )" +
			// ── DELETE/UPDATE таблиц аудита ──
			"     OR (" +
			"       stmt_type IN ('DELETE', 'UPDATE')" +
			"       AND (" +
			"         query_sql LIKE '%admintools.sessions%'" +
			"         OR query_sql LIKE '%admintools.ddl_dcl_audit_log%'" +
			"         OR (db_name = 'admintools' AND query_sql LIKE '%sessions%')" +
			"         OR (db_name = 'admintools' AND query_sql LIKE '%ddl_dcl_audit_log%')" +
			"         OR query_sql LIKE '%admintools.ddl_dcl_audit_targets%'" +
			"         OR (db_name = 'admintools' AND query_sql LIKE '%ddl_dcl_audit_targets%')" +
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
