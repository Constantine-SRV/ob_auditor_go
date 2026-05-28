// Package daemon — инфраструктура долгоживущего сервиса: цикл "Sleep после прогона",
// watchdog с принудительным выходом, recover-обёртка для panic-а.
//
// Архитектура:
//
//   - На каждый рабочий поток заводится Heartbeat — атомарный timestamp.
//     Поток обновляет его в начале и в конце своего тика.
//   - Watchdog — отдельная горутина, раз в WatchdogCheckInterval проверяет
//     возраст всех Heartbeat. Если хоть один старше WatchdogThreshold —
//     пишет ERROR в лог и вызывает os.Exit(1).
//   - systemd с Restart=on-failure поднимет процесс заново.
//
// В Go нельзя "убить" одну зависшую горутину, можно только убить весь
// процесс. Это сознательное ограничение языка ради безопасности памяти.
// Поэтому watchdog = os.Exit, и весь recovery полагается на systemd.
package daemon

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"obauditor/internal/logging"
)

// Heartbeat — атомарная отметка времени последней активности потока.
type Heartbeat struct {
	name     string
	lastTick atomic.Int64 // unix nanoseconds
}

// NewHeartbeat создаёт heartbeat с именем потока (для логирования).
// Стартовое значение — текущее время, чтобы watchdog не сработал
// до первого реального тика.
func NewHeartbeat(name string) *Heartbeat {
	h := &Heartbeat{name: name}
	h.Touch()
	return h
}

func (h *Heartbeat) Name() string { return h.name }

// Touch обновляет heartbeat на текущее время. Вызывается в начале
// каждого тика рабочего потока.
func (h *Heartbeat) Touch() {
	h.lastTick.Store(time.Now().UnixNano())
}

// Age возвращает время с последнего Touch.
func (h *Heartbeat) Age() time.Duration {
	return time.Since(time.Unix(0, h.lastTick.Load()))
}

// ─────────────────────────────────────────────────────────────────────
// Watchdog
// ─────────────────────────────────────────────────────────────────────

// Watchdog — раз в checkInterval проверяет возраст всех heartbeat-ов.
// Если хоть один старше threshold — os.Exit(1).
//
// Этот тип НЕ thread-safe для регистрации новых heartbeat-ов после старта.
// Используется так: создать → зарегистрировать все heartbeat-ы → Run.
type Watchdog struct {
	threshold     time.Duration
	checkInterval time.Duration
	beats         []*Heartbeat
	log           *logging.Logger
}

func NewWatchdog(threshold, checkInterval time.Duration, log *logging.Logger) *Watchdog {
	return &Watchdog{
		threshold:     threshold,
		checkInterval: checkInterval,
		log:           log,
	}
}

// Register добавляет heartbeat под наблюдение.
// Вызывать только до Run.
func (w *Watchdog) Register(h *Heartbeat) {
	w.beats = append(w.beats, h)
}

// Run — блокирующий метод, работает до ctx.Done().
// При обнаружении зависшего heartbeat-а вызывает os.Exit(1).
func (w *Watchdog) Run(ctx context.Context) {
	if w.checkInterval <= 0 {
		w.checkInterval = 10 * time.Second
	}
	t := time.NewTicker(w.checkInterval)
	defer t.Stop()

	w.log.Infof("[Watchdog] started: threshold=%v, check_interval=%v, beats=%d",
		w.threshold, w.checkInterval, len(w.beats))

	for {
		select {
		case <-ctx.Done():
			w.log.Infof("[Watchdog] stopping")
			return
		case <-t.C:
			for _, hb := range w.beats {
				age := hb.Age()
				if age > w.threshold {
					// КРИТИЧЕСКИЙ ВЫХОД. Поток name висит дольше порога —
					// процесс зависает или взаимный deadlock; пробуждение
					// через systemd Restart=on-failure.
					msg := fmt.Sprintf("[Watchdog] FATAL: goroutine %q heartbeat age %v > threshold %v — exit(1) for systemd restart",
						hb.Name(), age.Round(time.Second), w.threshold)
					// печатаем И в logger (stderr через Errorf), И прямо в stderr — на всякий
					w.log.Errorf("%s", msg)
					fmt.Fprintln(os.Stderr, msg)
					os.Exit(1)
				}
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────────────
// runLoop — цикл "выполнить → подождать → выполнить" с recover
// ─────────────────────────────────────────────────────────────────────

// TickFunc — одна итерация работы потока. Возвращает ошибку только для
// логирования; цикл не прервётся.
type TickFunc func(ctx context.Context) error

// RunLoop — общий цикл "Sleep после прогона" для одного потока.
//
//   - heartbeat.Touch() в начале каждого тика
//   - panic внутри fn ловится recover-ом и логируется
//   - после успешного или неуспешного тика — Sleep(interval)
//   - выход при ctx.Done() (graceful shutdown)
//
// Семантика Sleep: между концом одного прогона и началом следующего —
// всегда interval. Если прогон занял 30с при interval=20с, следующий
// стартанёт через 20с после конца, то есть всего цикл занял 50с.
// Это ровно то поведение, которое попросили.
func RunLoop(ctx context.Context,
	name string,
	interval time.Duration,
	hb *Heartbeat,
	log *logging.Logger,
	fn TickFunc,
) {
	log.Infof("[%s] loop started, interval=%v", name, interval)

	for {
		// Проверяем shutdown ДО прогона.
		select {
		case <-ctx.Done():
			log.Infof("[%s] loop stopped", name)
			return
		default:
		}

		hb.Touch()
		runWithRecover(name, log, func() error { return fn(ctx) })
		hb.Touch() // ещё раз после прогона — на случай если sleep длинный

		// Sleep с прерыванием по ctx.
		select {
		case <-ctx.Done():
			log.Infof("[%s] loop stopped", name)
			return
		case <-time.After(interval):
		}
	}
}

// runWithRecover — выполняет fn, ловит panic, логирует ошибки.
func runWithRecover(name string, log *logging.Logger, fn func() error) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("[%s] PANIC recovered: %v\n%s", name, r, debug.Stack())
		}
	}()
	if err := fn(); err != nil {
		log.Errorf("[%s] tick error: %v", name, err)
	}
}

// ─────────────────────────────────────────────────────────────────────
// WaitGroup helper для shutdown с таймаутом
// ─────────────────────────────────────────────────────────────────────

// WaitWithTimeout ждёт завершения wg или истечения таймаута.
// Возвращает true, если всё завершилось до таймаута.
func WaitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
