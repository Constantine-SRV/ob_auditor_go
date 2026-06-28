# OBAuditor

OBAuditor — аудитор активности OceanBase. Что он делает:

- **Читает логи OceanBase** (`observer.log*` и `obproxy.log*`) из каталогов
  логов observer'а и obproxy и извлекает события логина/логоффа.
- **Собирает DDL/DCL** из `GV$OB_SQL_AUDIT`.
- **Пишет всё в БД `admintools`** — таблицы `sessions`, `logfiles`,
  `ddl_dcl_audit_log` и служебные. Схему создаёт сам при первом запуске.
- **Пересылает новые события в rsyslog** по UDP (RFC 3164) — login, logoff
  и DDL/DCL, каждый поток с независимым курсором.
- **Чистит старые данные** — обрезает `sessions` и `ddl_dcl_audit_log`
  до заданных лимитов чанками.
- **Работает как долгоживущий сервис** с отдельными горутинами под logs,
  ddl и rsyslog, watchdog'ом и поддержкой схемы мастер/резерв. Подробности
  и установка через systemd — в [DAEMON.md](DAEMON.md).

---

## Требования

Для запуска готового бинарника из релизов ничего ставить не нужно —
это один статический файл без внешних зависимостей. Нужен только сетевой
доступ:

- Сетевой доступ к узлу OceanBase (порт 2881 по умолчанию)
- Доступ к каталогам с логами OB (локально, по NFS/SMB-маунту или UNC-пути)
- Если включена пересылка в rsyslog — UDP-доступ до syslog-хоста (порт 514)

OceanBase MySQL-совместима, поэтому используется стандартный MySQL-драйвер
`github.com/go-sql-driver/mysql`.

---

## Структура проекта

```
ob-auditor/
├── cmd/obauditor/main.go      # точка входа
├── internal/
│   ├── config/                # парсинг config.yaml
│   ├── model/                 # POJO: LoginEvent, LogFileRecord
│   ├── db/                    # DAO: init, sessions, logfiles, ddl/dcl, cleanup
│   ├── logproc/               # парсеры логов, обработчик, rsyslog sender
│   └── logging/               # уровни логирования (DEBUG/INFO/ERROR)
├── config.yaml                # конфигурация
├── go.mod
└── README.md
```

---

## Запуск

Готовый бинарник лежит в [релизах на GitHub](../../releases) — сборка
делается автоматически через GitHub Actions.

Конфиг по умолчанию — `config.yaml` рядом с бинарником:

```cmd
obauditor.exe
```

Либо явный путь:

```cmd
obauditor.exe C:\path\to\config.yaml
```

Это долгоживущий процесс: отдельные горутины обрабатывают логи, собирают
DDL/DCL и шлют события в rsyslog по своим интервалам, watchdog следит за
зависаниями. Установка и эксплуатация как systemd-сервиса описаны в
[DAEMON.md](DAEMON.md).

---

## Конфигурация

`config.yaml` — YAML-конфигурация приложения.

### Параметры

| Поле | Значение по умолчанию | Описание |
|---|---|---|
| `collectorId` | hostname | Идентификатор экземпляра (входит в UK таблицы logfiles) |
| `logLevel` | INFO | DEBUG / INFO / ERROR |
| `ignoredUsers` | ocp_monitor, proxy_ro, proxyro | Пользователи, исключаемые из аудита |
| `ddlDclAuditMode` | 0 | 0=выкл, 1=мастер (Collect каждый тик), 2=резерв (только если мастер молчит) |
| `daemon.logsInterval` | 60s | Пауза после прогона парсера лог-файлов |
| `daemon.ddlDclInterval` | 20s | Пауза после прогона DDL/DCL коллектора (частота `Collect`) |
| `daemon.rsyslogInterval` | 10s | Пауза после прогона пересылки в rsyslog |
| `daemon.ddlDclStaleThreshold` | 60s | Режим 2: резерв включается, если мастер молчит дольше этого |
| `daemon.cleanupEveryNCycles` | 10 | Cleanup-проверка каждые N циклов потока (0=выкл) |
| `daemon.watchdogThreshold` | 120s | Поток считается зависшим, если heartbeat старше |
| `daemon.watchdogCheckInterval` | 10s | Как часто watchdog опрашивает heartbeat-ы |
| `daemon.shutdownTimeout` | 30s | Сколько ждать тики на SIGTERM перед force exit |
| `daemon.statsInterval` | 60s | Период сводной строки [stats] (0=выкл) |
| `cleanup.enabled` | true | Включить очистку старых данных |
| `cleanup.maxDdlDclAuditRows` | 500000 | Макс. строк в ddl_dcl_audit_log |
| `cleanup.maxSessionsRows` | 500000 | Макс. строк в sessions |
| `cleanup.chunkSize` | 5000 | Размер чанка при удалении |
| `obServerLogPaths` | — | Пути до каталогов с observer.log |
| `obProxyLogPaths` | — | Пути до каталогов с obproxy.log |
| `systemTenantConnection` | — | Хосты, user, password, database |
| `rsyslog.host` | "" | Хост rsyslog (пусто = пересылка выкл.) |
| `rsyslog.port` | 514 | UDP-порт |
| `rsyslog.batchSize` | 500 | Размер батча отправки |
| `rsyslog.facility` | user | RFC 3164: kern, user, mail, daemon, auth, local0–local7 |

### Пути к логам OceanBase

`obServerLogPaths` и `obProxyLogPaths` — это списки каталогов, в которых лежат
`observer.log*` и `obproxy.log*` соответственно. Указывать нужно именно каталог,
а не сам файл — приложение само подхватывает текущий файл и ротированные.

По умолчанию (стандартная установка через OBD) логи лежат в `log/` внутри
рабочего каталога компонента:

```yaml
obServerLogPaths:
  - /home/admin/oceanbase/log      # observer.log, observer.log.wf

obProxyLogPaths:
  - /home/admin/obproxy/log        # obproxy.log, obproxy.log.wf
```

Точный путь зависит от того, как развёрнут кластер — это `<home_path>/log`,
где `<home_path>` задавался при установке (для obproxy его можно посмотреть
в `obproxy_config` / параметре запуска). Можно указать несколько каталогов,
если на узле несколько инстансов.

### Пароль

В текущей версии пароль читается прямо из `config.yaml`. Если нужно убрать его
из файла — можно дописать чтение из переменной окружения; сейчас этот блок
не реализован, ради простоты.

---

## Что делает приложение

```
1. Читает config.yaml
2. DbInitializer создаёт БД admintools и 6 таблиц (если их нет)
3. Открывает соединение к admintools (autoCommit=true)
4. LogFileProcessor:
   - SERVER-логи → парсит MySQL LOGIN / connection close
   - PROXY-логи  → парсит server session born / update_cmd_stats /
                   client session do_io_close / handle_server_connection_break
   - INSERT IGNORE в sessions, UPDATE logoff_time при logoff
5. SessionDao.SyncFailedProxySessions — закрывает PROXY-строки для
   неудачных логинов, зафиксированных SERVER-ом
6. DdlDclAuditDao.Collect (mode > 0) — выгружает новые DDL/DCL
   из GV$OB_SQL_AUDIT в ddl_dcl_audit_log
7. CleanupDao (если minute == cleanupMinute) — удаляет лишние строки
   чанками по chunkSize
8. RsyslogSender (если rsyslog.host задан) — шлёт login/logoff/ddl
   события по UDP с независимыми курсорами в rsyslog_cursor
9. Финальная строка статистики
```

Финал:

```
[Main] Done. vgo-20260521-1 Total time: 943 ms | lines: 37490 | inserted: 3 | logoff: 3 | logoffMiss: 0 | ddlDcl: 1 | cleanedDdlDcl: 0 | cleanedSessions: 0 | rsyslogLogin: 5 | rsyslogLogoff: 4 | rsyslogDdl: 1
```

---

## Известные ограничения

- DSN использует только первый хост из `systemTenantConnection.hosts`.
  Если нужен реальный failover между OB-нодами — придётся переписать
  на dial-loop с попытками подключения по очереди.
