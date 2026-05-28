// OBAuditor — долгоживущий демон.
// Запуск: obauditor [config.yaml]
//
// Архитектура:
//
//   - main: читает конфиг, открывает БД, инициализирует таблицы, запускает
//     три рабочие горутины и watchdog, ждёт SIGTERM/SIGINT.
//
//   - goroutine "logs"    — раз в logsInterval парсит логи OB,
//     синхронизирует sessions, каждые N циклов чистит sessions.
//
//   - goroutine "ddl"     — раз в ddlDclInterval собирает DDL/DCL из
//     GV$OB_SQL_AUDIT. В режиме 2 (резерв) запускает Collect только
//     если мастер молчит дольше staleThreshold.
//     Каждые N циклов чистит ddl_dcl_audit_log.
//
//   - goroutine "rsyslog" — раз в rsyslogInterval шлёт новые события в
//     rsyslog по UDP. Запускается только если rsyslog.host задан.
//
//   - goroutine "watchdog" — раз в watchdogCheckInterval проверяет
//     heartbeat-ы всех рабочих потоков. Если хоть один старше
//     watchdogThreshold — os.Exit(1) → systemd Restart=on-failure.
//
// Sleep после прогона: между концом одного тика и началом следующего —
// всегда interval, независимо от длительности самого тика. Если БД
// тупит и Collect занял 25 сек при ddlDclInterval=20 сек — следующий
// стартанёт через 20 сек ПОСЛЕ конца, то есть полный цикл 45 сек.
// Это намеренно: фиксированная пауза предсказуема и предотвращает
// штормовую нагрузку при медленной БД.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"obauditor/internal/config"
	"obauditor/internal/daemon"
	"obauditor/internal/db"
	"obauditor/internal/logging"
	"obauditor/internal/logproc"
)

const defaultConfig = "config.yaml"
const version = "go-20260528-2"

func main() {
	configPath := defaultConfig
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	// 1. Конфиг
	cfg, err := config.Read(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Main] Failed to read config: %v\n", err)
		os.Exit(1)
	}

	log := logging.New(cfg.LogLevel)
	log.Infof("=== OBAuditor %s starting ===", version)
	log.Infof("Config: %s", configPath)
	log.Infof("Collector: %s, LogLevel: %s, DdlDclMode: %d",
		cfg.CollectorId, cfg.LogLevel, cfg.DdlDclAuditMode)

	if log.IsDebug() {
		log.Debugf("Daemon: logs=%v ddl=%v rsyslog=%v stale=%v cleanupEveryN=%d watchdog=%v",
			cfg.Daemon.LogsInterval, cfg.Daemon.DdlDclInterval,
			cfg.Daemon.RsyslogInterval, cfg.Daemon.DdlDclStaleThreshold,
			cfg.Daemon.CleanupEveryNCycles, cfg.Daemon.WatchdogThreshold)
	}

	// 2. Игнорируемые пользователи в парсерах
	logproc.SetServerIgnoredUsers(cfg.IgnoredUsers)
	logproc.SetProxyIgnoredUsers(cfg.IgnoredUsers)

	// 3. Инициализация БД
	if err := db.NewInitializer(&cfg.SystemTenantConnection, log).Initialize(); err != nil {
		log.Errorf("[Main] DB initialization failed: %v", err)
		os.Exit(1)
	}

	// 4. Основное соединение
	conn, err := sql.Open("mysql", cfg.SystemTenantConnection.DSN("admintools"))
	if err != nil {
		log.Errorf("[Main] DB open failed: %v", err)
		os.Exit(1)
	}
	defer conn.Close()
	if err := conn.Ping(); err != nil {
		log.Errorf("[Main] DB ping failed: %v", err)
		os.Exit(1)
	}

	// 5. Signal handling + контекст для graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// 6. Heartbeat-ы, статистика и watchdog
	hbLogs := daemon.NewHeartbeat("logs")
	hbDdl := daemon.NewHeartbeat("ddl")
	hbRsyslog := daemon.NewHeartbeat("rsyslog")

	stats := daemon.NewStats()

	wd := daemon.NewWatchdog(
		cfg.Daemon.WatchdogThreshold,
		cfg.Daemon.WatchdogCheckInterval,
		log)
	wd.Register(hbLogs)
	wd.Register(hbDdl)
	if cfg.Rsyslog.Host != "" {
		wd.Register(hbRsyslog)
	}

	// 7. Запускаем горутины
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		runLogsLoop(ctx, conn, cfg, log, hbLogs, stats)
	}()

	if cfg.DdlDclAuditMode > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runDdlLoop(ctx, conn, cfg, log, hbDdl, stats)
		}()
	} else {
		log.Infof("[Main] DdlDclAuditMode=0, ddl loop disabled")
	}

	if cfg.Rsyslog.Host != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runRsyslogLoop(ctx, conn, cfg, log, hbRsyslog, stats)
		}()
	} else {
		log.Infof("[Main] rsyslog.host empty, rsyslog loop disabled")
	}

	// Watchdog в отдельной горутине, она НЕ участвует в wg.Wait —
	// она сама дёргает os.Exit при срабатывании и не должна задерживать shutdown.
	go wd.Run(ctx)

	// Горутина сводной статистики [stats] раз в StatsInterval.
	// Тоже вне wg.Wait — она не выполняет полезной работы, только печатает.
	if cfg.Daemon.StatsInterval > 0 {
		go stats.PrintLoop(ctx, cfg.Daemon.StatsInterval, log)
	}

	log.Infof("[Main] All loops started, waiting for signal")

	// 8. Ждём сигнал
	<-ctx.Done()
	log.Infof("[Main] Shutdown signal received, waiting for loops to finish (timeout=%v)",
		cfg.Daemon.ShutdownTimeout)

	if daemon.WaitWithTimeout(&wg, cfg.Daemon.ShutdownTimeout) {
		log.Infof("[Main] Clean shutdown completed")
	} else {
		log.Errorf("[Main] Shutdown timeout exceeded — forcing exit")
		os.Exit(1)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Logs loop — раз в logsInterval: парсинг файлов + sync + cleanup sessions
// ─────────────────────────────────────────────────────────────────────

func runLogsLoop(ctx context.Context, conn *sql.DB, cfg *config.AppConfig,
	log *logging.Logger, hb *daemon.Heartbeat, stats *daemon.Stats) {

	sessionDao := db.NewSessionDao(conn)
	cleanupDao := db.NewCleanupDao(conn, log, cfg.Cleanup.ChunkSize)
	var cycleCount int64

	daemon.RunLoop(ctx, "logs", cfg.Daemon.LogsInterval, hb, log,
		func(ctx context.Context) error {
			cycleCount++
			t0 := time.Now()

			processor := logproc.NewLogFileProcessor(conn, cfg, log)

			if err := processor.ProcessServerDirs(cfg.ObServerLogPaths); err != nil {
				log.Errorf("[logs] ProcessServerDirs: %v", err)
			}
			if err := processor.ProcessProxyDirs(cfg.ObProxyLogPaths); err != nil {
				log.Errorf("[logs] ProcessProxyDirs: %v", err)
			}
			if _, err := sessionDao.SyncFailedProxySessions(); err != nil {
				log.Errorf("[logs] SyncFailedProxySessions: %v", err)
			}

			// Cleanup каждые N циклов.
			var cleanedSessions int64
			if cfg.Cleanup.Enabled &&
				cfg.Daemon.CleanupEveryNCycles > 0 &&
				cycleCount%int64(cfg.Daemon.CleanupEveryNCycles) == 0 {
				n, err := cleanupDao.CleanSessions(cfg.Cleanup.MaxSessionsRows)
				if err != nil {
					log.Errorf("[logs] CleanSessions: %v", err)
				}
				cleanedSessions = n
			}

			lines := processor.TotalLines()
			inserted := processor.TotalInserted()
			logoff := processor.TotalLogoff()
			logoffMiss := processor.TotalLogoffMiss()

			// Копим в сводную статистику ([stats]).
			stats.AddFiles(lines, inserted, logoff, logoffMiss, cleanedSessions)

			// Пер-цикловая строка — только в DEBUG. В INFO остаются
			// только пер-файловые "done" и "Rotation detected" из
			// самого LogFileProcessor.
			log.Debugf("[logs] cycle=%d time=%dms lines=%d inserted=%d logoff=%d logoffMiss=%d cleanedSessions=%d",
				cycleCount, time.Since(t0).Milliseconds(),
				lines, inserted, logoff, logoffMiss, cleanedSessions)
			return nil
		})
}

// ─────────────────────────────────────────────────────────────────────
// DDL loop — раз в ddlDclInterval: DDL/DCL collect + cleanup ddl_log
// ─────────────────────────────────────────────────────────────────────

func runDdlLoop(ctx context.Context, conn *sql.DB, cfg *config.AppConfig,
	log *logging.Logger, hb *daemon.Heartbeat, stats *daemon.Stats) {

	auditDao := db.NewDdlDclAuditDao(conn, log)
	cleanupDao := db.NewCleanupDao(conn, log, cfg.Cleanup.ChunkSize)
	var cycleCount int64

	daemon.RunLoop(ctx, "ddl", cfg.Daemon.DdlDclInterval, hb, log,
		func(ctx context.Context) error {
			cycleCount++
			t0 := time.Now()

			// Решаем, запускать ли Collect в этом тике.
			//   mode=1 (мастер) — всегда
			//   mode=2 (резерв) — только если мастер молчит дольше staleThreshold
			doCollect := false
			switch cfg.DdlDclAuditMode {
			case 1:
				doCollect = true
			case 2:
				stale, err := auditDao.ShouldCollectFallback()
				if err != nil {
					log.Errorf("[ddl] ShouldCollectFallback: %v", err)
				}
				doCollect = stale
			}

			var inserted int64
			if doCollect {
				n, err := auditDao.Collect()
				if err != nil {
					log.Errorf("[ddl] Collect: %v", err)
				}
				inserted = n
			}

			// Cleanup каждые N циклов.
			var cleanedDdl int64
			if cfg.Cleanup.Enabled &&
				cfg.Daemon.CleanupEveryNCycles > 0 &&
				cycleCount%int64(cfg.Daemon.CleanupEveryNCycles) == 0 {
				n, err := cleanupDao.CleanDdlDclAuditLog(cfg.Cleanup.MaxDdlDclAuditRows)
				if err != nil {
					log.Errorf("[ddl] CleanDdlDclAuditLog: %v", err)
				}
				cleanedDdl = n
			}

			// Копим в сводную статистику ([stats]).
			stats.AddDdl(inserted, cleanedDdl)

			log.Debugf("[ddl] cycle=%d time=%dms collect=%v inserted=%d cleanedDdl=%d",
				cycleCount, time.Since(t0).Milliseconds(), doCollect, inserted, cleanedDdl)
			return nil
		})
}

// ─────────────────────────────────────────────────────────────────────
// Rsyslog loop — раз в rsyslogInterval: пересылка событий в syslog
// ─────────────────────────────────────────────────────────────────────

func runRsyslogLoop(ctx context.Context, conn *sql.DB, cfg *config.AppConfig,
	log *logging.Logger, hb *daemon.Heartbeat, stats *daemon.Stats) {

	sender := logproc.NewRsyslogSender(conn,
		cfg.Rsyslog.Host, cfg.Rsyslog.Port,
		cfg.Rsyslog.BatchSize, cfg.Rsyslog.Facility, log)
	var cycleCount int64

	daemon.RunLoop(ctx, "rsyslog", cfg.Daemon.RsyslogInterval, hb, log,
		func(ctx context.Context) error {
			cycleCount++
			t0 := time.Now()
			login, logoff, ddl := sender.Send()

			// Копим в сводную статистику ([stats]).
			stats.AddRsyslog(int64(login), int64(logoff), int64(ddl))

			log.Debugf("[rsyslog] cycle=%d time=%dms login=%d logoff=%d ddl=%d",
				cycleCount, time.Since(t0).Milliseconds(), login, logoff, ddl)
			return nil
		})
}
