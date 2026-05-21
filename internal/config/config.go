// Package config — загрузка конфигурации приложения из YAML.
package config

import (
	"fmt"
	"os"
	"strings"

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
// Поддерживает несколько хостов для failover.
type ConnectionConfig struct {
	Hosts    []string `yaml:"hosts"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
	Database string   `yaml:"database"`
}

// DSN строит data source name для MySQL-драйвера.
// Используется один хост (первый), остальные хосты пока игнорируются —
// failover на уровне Go-драйвера потребует кастомного dialer.
//
// Параметры подключения:
//
//	parseTime=true        — время в Go time.Time
//	loc=Local             — таймзона
//	timeout=5s            — connect timeout
//	readTimeout=30s       — socket read timeout (заменяет ob_query_timeout)
//	writeTimeout=30s      — socket write timeout
//	tls=false             — без SSL
//	interpolateParams=true — параметры подставляются в драйвере, не на сервере
//	                        (упрощает работу с UNSIGNED BIGINT)
//
// Примечание: ob_query_timeout НЕ задаём через sessionVariables —
// некоторые версии OceanBase отвечают на `SET ob_query_timeout=...`
// ошибкой 1054 "Unknown column 'ob_query_timeout' in 'field_list'".
// Длительные запросы ограничиваются readTimeout на уровне сокета,
// а cleanup идёт чанками по 5000 строк (см. CleanupDao).
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
type CleanupConfig struct {
	CleanupMinute      int   `yaml:"cleanupMinute"`
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

// AppConfig — корневая конфигурация приложения.
type AppConfig struct {
	CollectorId            string           `yaml:"collectorId"`
	LogLevelStr            string           `yaml:"logLevel"`
	IgnoredUsers           []string         `yaml:"ignoredUsers"`
	DdlDclAuditMode        int              `yaml:"ddlDclAuditMode"`
	Cleanup                CleanupConfig    `yaml:"cleanup"`
	ObProxyLogPaths        []string         `yaml:"obProxyLogPaths"`
	ObServerLogPaths       []string         `yaml:"obServerLogPaths"`
	SystemTenantConnection ConnectionConfig `yaml:"systemTenantConnection"`
	Rsyslog                RsyslogConfig    `yaml:"rsyslog"`

	// Производные поля (не из YAML)
	LogLevel LogLevel `yaml:"-"`
}

// Read загружает конфиг из YAML-файла.
// Применяет дефолты для незаданных полей.
func Read(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := &AppConfig{
		// Дефолты — переопределяются если в YAML заданы значения.
		LogLevelStr:     "INFO",
		DdlDclAuditMode: 0,
		Cleanup: CleanupConfig{
			CleanupMinute:      -1,
			MaxDdlDclAuditRows: 500000,
			MaxSessionsRows:    500000,
			ChunkSize:          5000,
		},
		IgnoredUsers: []string{"ocp_monitor", "proxy_ro", "proxyro"},
		Rsyslog: RsyslogConfig{
			Port:      514,
			BatchSize: 500,
			Facility:  "user",
		},
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	cfg.LogLevel = ParseLogLevel(cfg.LogLevelStr)

	// CollectorId: если пусто → hostname.
	if cfg.CollectorId == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			cfg.CollectorId = "unknown"
		} else {
			cfg.CollectorId = host
		}
	}

	// Если ignoredUsers явно задан пустым списком — оставляем пустым.
	// Если ключа в YAML вообще не было — дефолт уже подставлен выше.
	if cfg.IgnoredUsers == nil {
		cfg.IgnoredUsers = []string{"ocp_monitor", "proxy_ro", "proxyro"}
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
