// Package config — загрузка конфигурации приложения из YAML.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// LogLevel — уровень логирования.
type LogLevel int

const (
	LevelError LogLevel = iota
	LevelInfo
	LevelDebug
)

func (l LogLevel) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// ParseLogLevel — DEBUG | INFO | ERROR (case-insensitive). Default = INFO.
func ParseLogLevel(s string) LogLevel {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "DEBUG":
		return LevelDebug
	case "ERROR":
		return LevelError
	default:
		return LevelInfo
	}
}

// ConnectionConfig — подключение к OceanBase.
type ConnectionConfig struct {
	Hosts    []string `yaml:"hosts"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
	Database string   `yaml:"database"`
}

// DSN строит data source name для MySQL-драйвера.
// См. предыдущую версию пакета для комментариев по флагам.
func (c *ConnectionConfig) DSN(dbName string) string {
	if dbName == "" {
		dbName = c.Database
	}
	host := ""
	if len(c.Hosts) > 0 {
		host = c.Hosts[0]
	}
	return fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?parseTime=true&loc=Local"+
			"&timeout=5s&readTimeout=30s&writeTimeout=30s"+
			"&interpolateParams=true",
		c.User, c.Password, host, dbName,
	)
}

// String — безопасная репрезентация (без пароля).
func (c *ConnectionConfig) String() string {
	pwd := "<from env>"
	if c.Password != "" {
		pwd = "***"
	}
	return fmt.Sprintf("ConnectionConfig{hosts=%v, user='%s', password=%s, database='%s'}",
		c.Hosts, c.User, pwd, c.Database)
}

// CleanupConfig — параметры удаления старых строк.
//
// В демоне cleanup запускается изнутри рабочих горутин раз в N циклов
// (см. DaemonConfig.CleanupEveryNCycles). Минута часа (cleanupMinute из v1)
// больше не используется.
type CleanupConfig struct {
	Enabled            bool  `yaml:"enabled"`
	MaxDdlDclAuditRows int64 `yaml:"maxDdlDclAuditRows"`
	MaxSessionsRows    int64 `yaml:"maxSessionsRows"`
	ChunkSize          int64 `yaml:"chunkSize"`
}

// RsyslogConfig — настройки rsyslog UDP-пересылки.
type RsyslogConfig struct {
	Host      string `yaml:"host"`
	Port      int    `yaml:"port"`
	BatchSize int    `yaml:"batchSize"`
	Facility  string `yaml:"facility"`
}

// DaemonConfig — параметры работы в режиме сервиса.
type DaemonConfig struct {
	// Sleep после прогона для каждого потока.
	LogsInterval    time.Duration `yaml:"logsInterval"`
	DdlDclInterval  time.Duration `yaml:"ddlDclInterval"`
	RsyslogInterval time.Duration `yaml:"rsyslogInterval"`

	// Для режима ddlDclAuditMode=2: если последний прогон мастера был
	// раньше чем DdlDclStaleThreshold назад — мы (резерв) запускаемся.
	DdlDclStaleThreshold time.Duration `yaml:"ddlDclStaleThreshold"`

	// Каждый N-й цикл рабочего потока запускается cleanup-проверка
	// (COUNT + при необходимости chunked DELETE). 0 = выключено.
	CleanupEveryNCycles int `yaml:"cleanupEveryNCycles"`

	// Watchdog: горутина считается зависшей если её heartbeat старше этого.
	// При зависании — os.Exit(1) для рестарта через systemd.
	WatchdogThreshold time.Duration `yaml:"watchdogThreshold"`

	// Как часто watchdog опрашивает heartbeat-ы.
	WatchdogCheckInterval time.Duration `yaml:"watchdogCheckInterval"`

	// Сколько ждать завершения работающих тиков при SIGTERM.
	// После истечения — os.Exit(1).
	ShutdownTimeout time.Duration `yaml:"shutdownTimeout"`

	// Период печати сводной строки [stats]. 0 = выключить сводку.
	StatsInterval time.Duration `yaml:"statsInterval"`
}

// AppConfig — корневая конфигурация приложения.
type AppConfig struct {
	CollectorId            string           `yaml:"collectorId"`
	LogLevelStr            string           `yaml:"logLevel"`
	IgnoredUsers           []string         `yaml:"ignoredUsers"`
	DdlDclAuditMode        int              `yaml:"ddlDclAuditMode"`
	Cleanup                CleanupConfig    `yaml:"cleanup"`
	Daemon                 DaemonConfig     `yaml:"daemon"`
	ObProxyLogPaths        []string         `yaml:"obProxyLogPaths"`
	ObServerLogPaths       []string         `yaml:"obServerLogPaths"`
	SystemTenantConnection ConnectionConfig `yaml:"systemTenantConnection"`
	Rsyslog                RsyslogConfig    `yaml:"rsyslog"`

	LogLevel LogLevel `yaml:"-"`
}

// defaultsConfig — стартовые значения, перетираются YAML-ом если задано.
func defaultsConfig() *AppConfig {
	return &AppConfig{
		LogLevelStr:     "INFO",
		DdlDclAuditMode: 0,
		IgnoredUsers:    []string{"ocp_monitor", "proxy_ro", "proxyro"},
		Cleanup: CleanupConfig{
			Enabled:            true,
			MaxDdlDclAuditRows: 500000,
			MaxSessionsRows:    500000,
			ChunkSize:          5000,
		},
		Daemon: DaemonConfig{
			LogsInterval:          60 * time.Second,
			DdlDclInterval:        20 * time.Second,
			RsyslogInterval:       10 * time.Second,
			DdlDclStaleThreshold:  60 * time.Second,
			CleanupEveryNCycles:   10,
			WatchdogThreshold:     120 * time.Second,
			WatchdogCheckInterval: 10 * time.Second,
			ShutdownTimeout:       30 * time.Second,
			StatsInterval:         60 * time.Second,
		},
		Rsyslog: RsyslogConfig{
			Port:      514,
			BatchSize: 500,
			Facility:  "user",
		},
	}
}

// Read загружает конфиг из YAML-файла и применяет дефолты.
func Read(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := defaultsConfig()

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	cfg.LogLevel = ParseLogLevel(cfg.LogLevelStr)

	if cfg.CollectorId == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			cfg.CollectorId = "unknown"
		} else {
			cfg.CollectorId = host
		}
	}

	if cfg.IgnoredUsers == nil {
		cfg.IgnoredUsers = []string{"ocp_monitor", "proxy_ro", "proxyro"}
	}

	// Sanity checks для daemon-секции (если кто-то поставил 0).
	d := &cfg.Daemon
	if d.LogsInterval <= 0 {
		d.LogsInterval = 60 * time.Second
	}
	if d.DdlDclInterval <= 0 {
		d.DdlDclInterval = 20 * time.Second
	}
	if d.RsyslogInterval <= 0 {
		d.RsyslogInterval = 10 * time.Second
	}
	if d.DdlDclStaleThreshold <= 0 {
		d.DdlDclStaleThreshold = 60 * time.Second
	}
	if d.CleanupEveryNCycles < 0 {
		d.CleanupEveryNCycles = 0
	}
	if d.WatchdogThreshold <= 0 {
		d.WatchdogThreshold = 120 * time.Second
	}
	if d.WatchdogCheckInterval <= 0 {
		d.WatchdogCheckInterval = 10 * time.Second
	}
	if d.ShutdownTimeout <= 0 {
		d.ShutdownTimeout = 30 * time.Second
	}
	// StatsInterval == 0 допустимо (выключает сводку), отрицательное → дефолт
	if d.StatsInterval < 0 {
		d.StatsInterval = 60 * time.Second
	}

	return cfg, nil
}

// IgnoredUsersSet возвращает Set-подобную map для быстрой проверки.
func (c *AppConfig) IgnoredUsersSet() map[string]struct{} {
	m := make(map[string]struct{}, len(c.IgnoredUsers))
	for _, u := range c.IgnoredUsers {
		m[u] = struct{}{}
	}
	return m
}
