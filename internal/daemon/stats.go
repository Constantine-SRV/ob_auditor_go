package daemon

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"obauditor/internal/logging"
)

// Stats — нарастающие итоги работы демона с момента старта.
//
// Все поля — atomic, потому что обновляются из разных рабочих горутин
// (logs / ddl / rsyslog) без общего мьютекса. Печатается раз в период
// отдельной горутиной PrintLoop одной сводной строкой [stats].
//
// Счётчики живут только в памяти процесса и обнуляются при рестарте.
// Это намеренно: [stats] показывает активность ТЕКУЩЕГО запуска демона,
// а полные исторические итоги всегда можно взять из таблиц БД.
type Stats struct {
	startedAt time.Time

	// files (logs-поток)
	filesLines      atomic.Int64
	filesInserted   atomic.Int64
	filesLogoff     atomic.Int64
	filesLogoffMiss atomic.Int64
	filesCleaned    atomic.Int64

	// ddl-поток
	ddlCollected atomic.Int64
	ddlCleaned   atomic.Int64

	// rsyslog-поток
	rsyslogLogin  atomic.Int64
	rsyslogLogoff atomic.Int64
	rsyslogDdl    atomic.Int64
}

func NewStats() *Stats {
	return &Stats{startedAt: time.Now()}
}

// ── Аккумуляция (вызывается из рабочих горутин после каждого тика) ──

func (s *Stats) AddFiles(lines, inserted, logoff, logoffMiss, cleaned int64) {
	s.filesLines.Add(lines)
	s.filesInserted.Add(inserted)
	s.filesLogoff.Add(logoff)
	s.filesLogoffMiss.Add(logoffMiss)
	s.filesCleaned.Add(cleaned)
}

func (s *Stats) AddDdl(collected, cleaned int64) {
	s.ddlCollected.Add(collected)
	s.ddlCleaned.Add(cleaned)
}

func (s *Stats) AddRsyslog(login, logoff, ddl int64) {
	s.rsyslogLogin.Add(login)
	s.rsyslogLogoff.Add(logoff)
	s.rsyslogDdl.Add(ddl)
}

// ── Печать сводки ──

// line формирует строку [stats].
func (s *Stats) line() string {
	uptime := time.Since(s.startedAt).Round(time.Second)
	return fmt.Sprintf(
		"[stats] uptime=%s | files: lines=%d inserted=%d logoff=%d logoffMiss=%d cleaned=%d"+
			" | ddl: collected=%d cleaned=%d"+
			" | rsyslog: login=%d logoff=%d ddl=%d",
		uptime,
		s.filesLines.Load(), s.filesInserted.Load(), s.filesLogoff.Load(),
		s.filesLogoffMiss.Load(), s.filesCleaned.Load(),
		s.ddlCollected.Load(), s.ddlCleaned.Load(),
		s.rsyslogLogin.Load(), s.rsyslogLogoff.Load(), s.rsyslogDdl.Load(),
	)
}

// PrintLoop — блокирующая горутина, печатает [stats] раз в period.
// Печатает напрямую через logger.Infof (не зависит от уровня для INFO).
// Выход по ctx.Done(); при выходе печатает финальную сводку.
func (s *Stats) PrintLoop(ctx context.Context, period time.Duration, log *logging.Logger) {
	if period <= 0 {
		period = 60 * time.Second
	}
	t := time.NewTicker(period)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			// финальная сводка при остановке
			log.Infof("%s (final)", s.line())
			return
		case <-t.C:
			log.Infof("%s", s.line())
		}
	}
}
