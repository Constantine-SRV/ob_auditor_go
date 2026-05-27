// Package db — работа с БД admintools.
package db

import (
	"database/sql"
	"fmt"
	"strings"

	"obauditor/internal/config"
	"obauditor/internal/logging"
)

const targetDB = "admintools"

// Initializer создаёт БД admintools и все нужные таблицы.
type Initializer struct {
	conn *config.ConnectionConfig
	log  *logging.Logger
}

func NewInitializer(conn *config.ConnectionConfig, log *logging.Logger) *Initializer {
	return &Initializer{conn: conn, log: log}
}

type tableDef struct {
	name string
	ddl  string
}

// Initialize создаёт БД и все таблицы.
//
// Порядок: коннектимся к "oceanbase" (системной БД), создаём admintools,
// далее переподключаемся к admintools и создаём все таблицы.
func (i *Initializer) Initialize() error {
	i.log.Debugf("[DbInitializer] Starting DB initialization...")

	// 1) connect to system db, ensure admintools exists
	{
		db, err := sql.Open("mysql", i.conn.DSN(i.conn.Database))
		if err != nil {
			return fmt.Errorf("open system db: %w", err)
		}
		defer db.Close()
		if err := db.Ping(); err != nil {
			return fmt.Errorf("ping system db: %w", err)
		}
		if err := i.ensureDatabase(db, targetDB); err != nil {
			return err
		}
	}

	// 2) connect to admintools, ensure tables
	db, err := sql.Open("mysql", i.conn.DSN(targetDB))
	if err != nil {
		return fmt.Errorf("open admintools: %w", err)
	}
	defer db.Close()

	tables := []tableDef{
		sessionsTable(),
		logfilesTable(),
		auditCollectorStateTable(),    // legacy v1, оставлен для backward compat
		ddlDclAuditLogTable(),
		ddlDclAuditTargetsTable(),
		ddlDclAuditCheckpointTable(),  // v2 per-server-tenant курсор
		rsyslogCursorTable(),
	}
	for _, t := range tables {
		if err := i.ensureTable(db, t); err != nil {
			return err
		}
	}

	// audit_collector_state имеет ровно одну строку id=1 (legacy v1)
	if err := i.ensureAuditCollectorStateRow(db); err != nil {
		return err
	}

	i.log.Debugf("[DbInitializer] Initialization complete.")
	return nil
}

// ─────────────────────────────────────────────────────────────────────

func (i *Initializer) ensureDatabase(db *sql.DB, dbName string) error {
	var cnt int
	err := db.QueryRow(
		"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?",
		dbName).Scan(&cnt)
	if err != nil {
		return fmt.Errorf("check db existence: %w", err)
	}
	if cnt > 0 {
		i.log.Debugf("[DbInitializer] Database '%s' already exists.", dbName)
		return nil
	}
	i.log.Debugf("[DbInitializer] Creating database '%s'...", dbName)
	if _, err := db.Exec("CREATE DATABASE `" + dbName + "` DEFAULT CHARACTER SET utf8mb4"); err != nil {
		return fmt.Errorf("create db %s: %w", dbName, err)
	}
	i.log.Infof("[DbInitializer] Database '%s' created.", dbName)
	return nil
}

func (i *Initializer) ensureTable(db *sql.DB, def tableDef) error {
	var cnt int
	err := db.QueryRow(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		targetDB, def.name).Scan(&cnt)
	if err != nil {
		return fmt.Errorf("check table %s: %w", def.name, err)
	}
	if cnt > 0 {
		i.log.Debugf("[DbInitializer] Table '%s' already exists.", def.name)
		return nil
	}
	i.log.Debugf("[DbInitializer] Creating table '%s'...", def.name)
	if _, err := db.Exec(def.ddl); err != nil {
		return fmt.Errorf("create table %s: %w", def.name, err)
	}
	i.log.Infof("[DbInitializer] Table '%s' created.", def.name)
	return nil
}

func (i *Initializer) ensureAuditCollectorStateRow(db *sql.DB) error {
	res, err := db.Exec(
		"INSERT IGNORE INTO `audit_collector_state` (id, collector_id, last_request_time) VALUES (1, 'ddl_dcl_audit', 0)")
	if err != nil {
		return fmt.Errorf("insert audit_collector_state row: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows > 0 {
		i.log.Infof("[DbInitializer] Inserted initial row into audit_collector_state")
	} else {
		i.log.Debugf("[DbInitializer] audit_collector_state row already exists")
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────
// DDL — определения таблиц
// ─────────────────────────────────────────────────────────────────────

func sessionsTable() tableDef {
	ddl := strings.Join([]string{
		"CREATE TABLE `sessions` (",
		"  `id`             BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,",
		"  `source`         VARCHAR(8)       NOT NULL COMMENT 'SERVER или PROXY',",
		"  `server_ip`      VARCHAR(64)      NOT NULL DEFAULT '' COMMENT 'IP узла-источника лога (для UK)',",
		"  `cluster_name`   VARCHAR(128)     NOT NULL DEFAULT '' COMMENT 'Имя кластера (PROXY) или пустая строка',",
		"  `session_id`     BIGINT UNSIGNED  NOT NULL COMMENT 'sessid (SERVER) или server_sessid (PROXY)',",
		"  `login_time`     DATETIME(6)      NOT NULL COMMENT 'Время логина из лога',",
		"  `logoff_time`    DATETIME(6)          NULL COMMENT 'Время логоффа, NULL = сессия открыта',",
		"  `is_success`     TINYINT(1)       NOT NULL COMMENT '1=LOGIN_OK 0=LOGIN_FAIL',",
		"  `client_ip`      VARCHAR(64)          NULL COMMENT 'IP клиента',",
		"  `tenant_name`    VARCHAR(128)         NULL,",
		"  `user_name`      VARCHAR(128)         NULL,",
		"  `error_code`     INT                  NULL COMMENT 'Код ошибки при FAIL',",
		"  `ssl`            CHAR(1)              NULL COMMENT 'Y/N только для SERVER',",
		"  `client_type`    VARCHAR(16)          NULL COMMENT 'JDBC/JAVA/OCI/OBCLIENT/MYSQL_CLI',",
		"  `proxy_sessid`   BIGINT UNSIGNED      NULL COMMENT 'proxy_sessid',",
		"  `cs_id`          BIGINT UNSIGNED      NULL COMMENT 'Client session id (PROXY)',",
		"  `server_node_ip` VARCHAR(64)          NULL COMMENT 'IP OBServer-узла из тела строки лога',",
		"  `from_proxy`     TINYINT(1)           NULL COMMENT '1=пришёл через OBProxy (SERVER-лог)',",
		"  PRIMARY KEY (`id`),",
		"  UNIQUE KEY `uk_sess` (`source`, `server_ip`, `cluster_name`, `session_id`, `login_time`),",
		"  KEY `idx_login_time`   (`login_time`),",
		"  KEY `idx_user`          (`user_name`),",
		"  KEY `idx_open`          (`logoff_time`),",
		"  KEY `idx_proxy_sessid`  (`proxy_sessid`)",
		") COMMENT = 'OceanBase сессии: логин и логофф в одной строке'",
	}, "\n")
	return tableDef{"sessions", ddl}
}

func logfilesTable() tableDef {
	ddl := strings.Join([]string{
		"CREATE TABLE `logfiles` (",
		"  `id`             BIGINT       NOT NULL AUTO_INCREMENT,",
		"  `collector_id`   VARCHAR(128) NOT NULL COMMENT 'Идентификатор сервиса-коллектора (hostname или IP)',",
		"  `file_dir`       VARCHAR(512) NOT NULL COMMENT 'Директория лог-файла',",
		"  `file_name`      VARCHAR(256) NOT NULL COMMENT 'Имя файла',",
		"  `file_type`      VARCHAR(16)  NOT NULL COMMENT 'SERVER или PROXY',",
		"  `file_size`      BIGINT       NOT NULL DEFAULT 0 COMMENT 'Последний известный размер в байтах',",
		"  `last_line_num`  BIGINT       NOT NULL DEFAULT 0 COMMENT 'Байтовый offset после последней обработанной строки',",
		"  `last_timestamp` VARCHAR(32)      NULL COMMENT 'Временная метка последней обработанной записи',",
		"  `last_tid`       INT              NULL COMMENT 'Thread ID последней обработанной записи',",
		"  `last_trace_id`  VARCHAR(64)      NULL COMMENT 'Trace ID последней обработанной записи',",
		"  `file_ip`        VARCHAR(64)      NULL COMMENT 'IP узла-источника лога',",
		"  PRIMARY KEY (`id`),",
		"  UNIQUE KEY `uq_collector_dir_name` (`collector_id`, `file_dir`(255), `file_name`),",
		"  KEY `idx_file_type` (`file_type`),",
		"  KEY `idx_collector`  (`collector_id`)",
		") COMMENT = 'Состояние обработки лог-файлов OceanBase'",
	}, "\n")
	return tableDef{"logfiles", ddl}
}

// auditCollectorStateTable — legacy v1, оставлена для backward compat.
// v2 использует ddl_dcl_audit_checkpoint вместо неё, но таблицу создаём
// (на случай отката или параллельного запуска SP-варианта).
func auditCollectorStateTable() tableDef {
	ddl := strings.Join([]string{
		"CREATE TABLE `audit_collector_state` (",
		"  `id`                BIGINT       NOT NULL,",
		"  `collector_id`      VARCHAR(64)  NOT NULL COMMENT 'Идентификатор коллектора',",
		"  `last_request_time` BIGINT       NOT NULL DEFAULT 0 COMMENT 'request_time последней обработанной записи (legacy v1)',",
		"  `updated_at`        DATETIME(6)      NULL COMMENT 'Wall-clock время последнего успешного сбора',",
		"  PRIMARY KEY (`id`)",
		") COMMENT = 'Legacy v1 состояние DDL/DCL коллектора (не используется в v2)'",
	}, "\n")
	return tableDef{"audit_collector_state", ddl}
}

func ddlDclAuditLogTable() tableDef {
	ddl := strings.Join([]string{
		"CREATE TABLE `ddl_dcl_audit_log` (",
		"  `id`             BIGINT      NOT NULL AUTO_INCREMENT,",
		"  `collected_at`   DATETIME(6) NOT NULL DEFAULT NOW(6) COMMENT 'Время вставки записи',",
		"  `request_id`     BIGINT      NOT NULL                COMMENT 'Request ID в OB (ключ дедупликации)',",
		"  `svr_ip`         VARCHAR(46) NOT NULL                COMMENT 'IP OBServer-узла',",
		"  `tenant_id`      BIGINT          NULL COMMENT 'ID тенанта',",
		"  `tenant_name`    VARCHAR(64)     NULL COMMENT 'Имя тенанта',",
		"  `user_id`        BIGINT          NULL COMMENT 'ID пользователя',",
		"  `user_name`      VARCHAR(64)     NULL COMMENT 'Имя пользователя',",
		"  `proxy_user`     VARCHAR(128)    NULL COMMENT 'Proxy-пользователь (при proxy-логине)',",
		"  `client_ip`      VARCHAR(46)     NULL COMMENT 'IP OBProxy или клиента при прямом подключении',",
		"  `user_client_ip` VARCHAR(46)     NULL COMMENT 'Реальный IP клиента',",
		"  `sid`            BIGINT UNSIGNED NULL COMMENT 'Session ID',",
		"  `db_name`        VARCHAR(128)    NULL COMMENT 'Контекст базы данных',",
		"  `stmt_type`      VARCHAR(128)    NULL COMMENT 'Тип SQL-оператора',",
		"  `query_sql`      LONGTEXT        NULL COMMENT 'Текст SQL',",
		"  `ret_code`       BIGINT          NULL COMMENT '0=успех, иное=код ошибки OB',",
		"  `affected_rows`  BIGINT          NULL COMMENT 'Затронуто строк',",
		"  `request_ts`     DATETIME(6) NOT NULL COMMENT 'Время начала выполнения',",
		"  `elapsed_time`   BIGINT          NULL COMMENT 'Время выполнения, микросекунды',",
		"  `retry_cnt`      BIGINT          NULL COMMENT 'Количество повторов',",
		"  PRIMARY KEY (`id`),",
		"  UNIQUE KEY `uq_req` (`svr_ip`, `request_id`),",
		"  KEY `idx_request_ts` (`request_ts`),",
		"  KEY `idx_user_name`  (`user_name`),",
		"  KEY `idx_stmt_type`  (`stmt_type`)",
		") COMMENT = 'DDL/DCL аудит из GV$OB_SQL_AUDIT'",
	}, "\n")
	return tableDef{"ddl_dcl_audit_log", ddl}
}

func ddlDclAuditTargetsTable() tableDef {
	ddl := strings.Join([]string{
		"CREATE TABLE `ddl_dcl_audit_targets` (",
		"  `id`          BIGINT       NOT NULL AUTO_INCREMENT,",
		"  `tenant_id`   BIGINT           NULL COMMENT 'NULL = все тенанты',",
		"  `db_name`     VARCHAR(128)     NULL COMMENT 'NULL = любая база',",
		"  `object_name` VARCHAR(128) NOT NULL COMMENT 'Имя таблицы, процедуры, вьюшки',",
		"  `description` VARCHAR(512)     NULL COMMENT 'Описание (для чего аудируем)',",
		"  `is_active`   TINYINT(1)   NOT NULL DEFAULT 1 COMMENT '1=активен, 0=отключён',",
		"  `created_at`  DATETIME(6)  NOT NULL DEFAULT NOW(6),",
		"  PRIMARY KEY (`id`),",
		"  KEY `idx_tenant` (`tenant_id`),",
		"  KEY `idx_active` (`is_active`)",
		") COMMENT = 'Объекты для дополнительного DML-аудита через GV$OB_SQL_AUDIT'",
	}, "\n")
	return tableDef{"ddl_dcl_audit_targets", ddl}
}

// ddlDclAuditCheckpointTable — v2 per-server-tenant курсор.
//
// Одна строка на каждую комбинацию (svr_ip, svr_port, tenant_id) из
// DBA_OB_UNITS. Заполняется и обновляется автоматически в DdlDclAuditDao.
//
// last_end_time — это request_time + elapsed_time (момент попадания записи
// в audit-буфер) последней обработанной строки. Использование «end»-времени
// вместо «start» решает проблему long-running DDL: запрос, стартовавший
// до курсора но завершившийся после, при start-курсоре терялся; при
// end-курсоре он гарантированно попадёт в следующее окно.
func ddlDclAuditCheckpointTable() tableDef {
	ddl := strings.Join([]string{
		"CREATE TABLE `ddl_dcl_audit_checkpoint` (",
		"  `svr_ip`        VARCHAR(46) NOT NULL COMMENT 'IP OBServer-узла',",
		"  `svr_port`      BIGINT      NOT NULL COMMENT 'RPC порт (обычно 2882)',",
		"  `tenant_id`     BIGINT      NOT NULL COMMENT 'ID тенанта',",
		"  `last_end_time` BIGINT      NOT NULL DEFAULT 0 COMMENT 'request_time + elapsed_time последней обработанной записи, мкс от epoch',",
		"  `updated_at`    DATETIME(6)     NULL COMMENT 'Wall-clock последнего прогона коллектора по этому юниту',",
		"  PRIMARY KEY (`svr_ip`, `svr_port`, `tenant_id`)",
		") COMMENT = 'Per server-tenant курсор DDL/DCL аудита (v2)'",
	}, "\n")
	return tableDef{"ddl_dcl_audit_checkpoint", ddl}
}

func rsyslogCursorTable() tableDef {
	ddl := strings.Join([]string{
		"CREATE TABLE `rsyslog_cursor` (",
		"  `event_type` VARCHAR(32)     NOT NULL COMMENT 'login / logoff / ddl',",
		"  `last_id`    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'последний отправленный id',",
		"  `last_time`  VARCHAR(32)         NULL COMMENT 'для logoff: последний отправленный logoff_time',",
		"  `updated_at` DATETIME(6)         NULL COMMENT 'время последней успешной отправки',",
		"  PRIMARY KEY (`event_type`)",
		") COMMENT = 'Курсор пересылки событий аудита в rsyslog'",
	}, "\n")
	return tableDef{"rsyslog_cursor", ddl}
}
