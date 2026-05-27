// OceanBase Auditor — точка входа.
// Запуск: obauditor [config.yaml]
package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"obauditor/internal/config"
	"obauditor/internal/db"
	"obauditor/internal/logging"
	"obauditor/internal/logproc"
)

const defaultConfig = "config.yaml"
const version = "go-20260527-1"

func main() {
	totalStart := time.Now()

	configPath := defaultConfig
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	// 1. Читаем конфиг
	cfg, err := config.Read(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Main] Failed to read config: %v\n", err)
		os.Exit(1)
	}

	log := logging.New(cfg.LogLevel)

	if log.IsInfo() {
		log.Infof("=== OceanBase Auditor ===")
		log.Infof("Config: %s", configPath)
	}

	// 2. Инициализируем список игнорируемых пользователей в парсерах
	logproc.SetServerIgnoredUsers(cfg.IgnoredUsers)
	logproc.SetProxyIgnoredUsers(cfg.IgnoredUsers)

	// 3. Печатаем конфиг (только DEBUG)
	if log.IsDebug() {
		log.Debugf("\n--- Loaded configuration ---")
		log.Debugf("CollectorId        : %s", cfg.CollectorId)
		log.Debugf("LogLevel           : %s", cfg.LogLevel)
		log.Debugf("IgnoredUsers       : %v", cfg.IgnoredUsers)
		log.Debugf("DdlDclAuditMode    : %d", cfg.DdlDclAuditMode)
		log.Debugf("CleanupMinute      : %d", cfg.Cleanup.CleanupMinute)
		log.Debugf("MaxDdlDclAuditRows : %d", cfg.Cleanup.MaxDdlDclAuditRows)
		log.Debugf("MaxSessionsRows    : %d", cfg.Cleanup.MaxSessionsRows)
		log.Debugf("CleanupChunkSize   : %d", cfg.Cleanup.ChunkSize)
		log.Debugf("OBProxy log paths  : %v", cfg.ObProxyLogPaths)
		log.Debugf("OBServer log paths : %v", cfg.ObServerLogPaths)
		log.Debugf("RsyslogHost        : %s", cfg.Rsyslog.Host)
		log.Debugf("RsyslogPort        : %d", cfg.Rsyslog.Port)
		log.Debugf("RsyslogFacility    : %s", cfg.Rsyslog.Facility)
		log.Debugf("RsyslogBatchSize   : %d", cfg.Rsyslog.BatchSize)
		log.Debugf("DB connection      : %s", cfg.SystemTenantConnection.String())
		log.Debugf("----------------------------\n")
	}

	// 4. Инициализация БД
	initializer := db.NewInitializer(&cfg.SystemTenantConnection, log)
	if err := initializer.Initialize(); err != nil {
		log.Errorf("[Main] DB initialization failed: %v", err)
		os.Exit(1)
	}

	// 5. Основная обработка
	conn, err := sql.Open("mysql", cfg.SystemTenantConnection.DSN("admintools"))
	if err != nil {
		log.Errorf("[Main] DB connection failed: %v", err)
		os.Exit(1)
	}
	defer conn.Close()

	// autoCommit в database/sql — это поведение по умолчанию для отдельных Exec.
	// Это предотвращает многочасовые блокировки если процесс упадёт посередине прогона.
	// Для атомарных операций (cleanup, обновление audit_collector_state)
	// используем явные транзакции через conn.Begin().

	if err := conn.Ping(); err != nil {
		log.Errorf("[Main] DB ping failed: %v", err)
		os.Exit(1)
	}

	// 5a. Обработка лог-файлов
	processor := logproc.NewLogFileProcessor(conn, cfg, log)
	if err := processor.ProcessServerDirs(cfg.ObServerLogPaths); err != nil {
		log.Errorf("[Main] processServerDirs: %v", err)
	}
	if err := processor.ProcessProxyDirs(cfg.ObProxyLogPaths); err != nil {
		log.Errorf("[Main] processProxyDirs: %v", err)
	}

	// 5b. Reconciliation PROXY-строк для неудачных логинов
	sessionDao := db.NewSessionDao(conn)
	if _, err := sessionDao.SyncFailedProxySessions(); err != nil {
		log.Errorf("[Main] syncFailedProxySessions: %v", err)
	}

	// 5c. DDL/DCL аудит из GV$OB_SQL_AUDIT
	var ddlDclInserted int64
	if cfg.DdlDclAuditMode > 0 {
		auditDao := db.NewDdlDclAuditDao(conn, log)
		doCollect := false
		switch cfg.DdlDclAuditMode {
		case 1:
			doCollect = true
		case 2:
			fallback, err := auditDao.ShouldCollectFallback()
			if err != nil {
				log.Errorf("[Main] shouldCollectFallback: %v", err)
			}
			doCollect = fallback
		}
		if doCollect {
			n, err := auditDao.Collect()
			if err != nil {
				log.Errorf("[Main] ddl/dcl Collect: %v", err)
			}
			ddlDclInserted = n
		}
	}

	// 5d. Очистка таблиц по расписанию
	var cleanedDdlDcl, cleanedSessions int64
	if cfg.Cleanup.CleanupMinute >= 0 {
		currentMinute := time.Now().Minute()
		if currentMinute == cfg.Cleanup.CleanupMinute {
			log.Infof("[Main] Running scheduled cleanup (minute=%d)", currentMinute)
			cleanupDao := db.NewCleanupDao(conn, log, cfg.Cleanup.ChunkSize)
			if n, err := cleanupDao.CleanDdlDclAuditLog(cfg.Cleanup.MaxDdlDclAuditRows); err == nil {
				cleanedDdlDcl = n
			} else {
				log.Errorf("[Main] cleanDdlDclAuditLog: %v", err)
			}
			if n, err := cleanupDao.CleanSessions(cfg.Cleanup.MaxSessionsRows); err == nil {
				cleanedSessions = n
			} else {
				log.Errorf("[Main] cleanSessions: %v", err)
			}
		}
	}

	// 5e. Пересылка событий в rsyslog
	var rsyslogLogin, rsyslogLogoff, rsyslogDdl int
	if cfg.Rsyslog.Host != "" {
		sender := logproc.NewRsyslogSender(conn,
			cfg.Rsyslog.Host, cfg.Rsyslog.Port,
			cfg.Rsyslog.BatchSize, cfg.Rsyslog.Facility, log)
		rsyslogLogin, rsyslogLogoff, rsyslogDdl = sender.Send()
	}

	totalMs := time.Since(totalStart).Milliseconds()
	fmt.Printf(
		"[Main] Done. v%s Total time: %d ms"+
			" | lines: %d | inserted: %d | logoff: %d | logoffMiss: %d"+
			" | ddlDcl: %d | cleanedDdlDcl: %d | cleanedSessions: %d"+
			" | rsyslogLogin: %d | rsyslogLogoff: %d | rsyslogDdl: %d\n",
		version, totalMs,
		processor.TotalLines(),
		processor.TotalInserted(),
		processor.TotalLogoff(),
		processor.TotalLogoffMiss(),
		ddlDclInserted,
		cleanedDdlDcl,
		cleanedSessions,
		rsyslogLogin,
		rsyslogLogoff,
		rsyslogDdl,
	)
}
