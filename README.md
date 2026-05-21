# OBAuditor (Go port)

Порт Java-проекта OBAuditor на Go. Читает логи OceanBase (observer.log\* и
obproxy.log\*), извлекает события логина/логоффа, собирает DDL/DCL из
`GV$OB_SQL_AUDIT`, пишет всё в БД `admintools` и пересылает новые события
в rsyslog по UDP (RFC 3164).

Логика и SQL-запросы перенесены 1-в-1 с Java-версии. Подробное описание
алгоритмов и схемы БД — в исходном `ARCHITECTURE.md`.

---

## Требования

- **Go 1.22+** (Google Go под Windows подходит)
- Сетевой доступ к узлу OceanBase (порт 2881 по умолчанию)
- Доступ к каталогам с логами OB — на Windows через SMB-шары (UNC-пути)
- Если включена пересылка в rsyslog — UDP-доступ до syslog-хоста (порт 514)

OceanBase MySQL-совместима, поэтому используется стандартный
`github.com/go-sql-driver/mysql` — отдельный JDBC-драйвер не нужен.

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

## Сборка

```cmd
cd ob-auditor
go mod tidy
go build -o obauditor.exe ./cmd/obauditor
```

Получится один статический `.exe` без внешних DLL.

---

## Запуск

Конфиг по умолчанию — `config.yaml` рядом с бинарником:

```cmd
obauditor.exe
```

Либо явный путь:

```cmd
obauditor.exe C:\path\to\config.yaml
```

Один прогон = один проход по всем логам, синхронизация, опционально cleanup
и rsyslog, выход. Для регулярного запуска используйте Task Scheduler / cron
(оригинальная Java-версия так же предполагала внешний планировщик).

---

## Конфигурация

`config.yaml` — YAML-аналог исходного `config.xml`. Структура сохранена.

### Параметры

| Поле | Значение по умолчанию | Описание |
|---|---|---|
| `collectorId` | hostname | Идентификатор экземпляра (входит в UK таблицы logfiles) |
| `logLevel` | INFO | DEBUG / INFO / ERROR |
| `ignoredUsers` | ocp_monitor, proxy_ro, proxyro | Пользователи, исключаемые из аудита |
| `ddlDclAuditMode` | 0 | 0=выкл, 1=основной, 2=резервный |
| `cleanup.cleanupMinute` | -1 | Минута часа для очистки (-1=выкл) |
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

### Пути к логам OceanBase под Windows (SMB-шары)

В YAML обратные слэши — обычные символы, экранирование не нужно, но безопаснее
заворачивать UNC-пути в одинарные кавычки:

```yaml
obProxyLogPaths:
  - '\\192.168.55.200\obproxy_log'

obServerLogPaths:
  - '\\192.168.55.205\oceanbase_log'
```

Перед запуском убедитесь, что шары доступны для текущего пользователя — попробуйте
открыть путь в Проводнике или `dir \\192.168.55.205\oceanbase_log`. Если Windows
просит авторизацию — сохраните учётки через `cmdkey /add:192.168.55.205 ...`
или примонтируйте шару через `net use`.

### Пароль

В текущей версии пароль читается прямо из `config.yaml`. Если нужно убрать его
из файла — можно дописать чтение из переменной окружения (как в Java-версии
через `PasswordEnricher`); сейчас этот блок не портирован, ради простоты.

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

## Отладка в VS Code

Установите расширение **Go** (golang.go) — оно само подтянет `gopls`, `dlv`
и прочее при первом открытии `.go`-файла.

Готовая конфигурация запуска уже лежит в `.vscode/launch.json`. F5 запустит
бинарник под debugger с локальным `config.yaml`.

Если хочется отлаживать с другим конфигом — поправьте `args` в launch.json:

```json
"args": ["C:\\path\\to\\config.yaml"]
```

Точки останова работают в любом файле, включая `internal/logproc/*` и
парсеры — это самое полезное место для проверки регулярок на реальных логах.

---

## Различия с Java-версией

Логика и SQL — те же. Технические отличия:

- **BIGINT UNSIGNED** хранится как `uint64` нативно. В Java приходилось
  работать через `Long.toUnsignedString` / `Long.parseUnsignedLong`.
- **autoCommit** управляется параметром DSN `interpolateParams=true` +
  явными `db.Begin()` / `tx.Commit()` для операций требующих транзакции
  (cleanup, обновление `audit_collector_state`). По умолчанию
  `database/sql` пишет в autoCommit-режиме — поведение совпадает с Java.
- **PasswordEnricher не портирован** — пароль берётся из YAML напрямую.
- **Парсеры** один в один, регулярки идентичны.
- **Offset-трекинг** немного консервативнее: учитываются только полные
  `\n`-терминированные строки. Java-версия через `channel.position()`
  могла захватить частичную последнюю строку — на практике это не
  происходит при работе с ротированными observer.log/obproxy.log.

---

## Известные ограничения (унаследованы из Java-версии)

- `last_timestamp` в `logfiles` не обновляется во время штатной обработки —
  только сбрасывается в null при детекте ротации. Соответственно ветка
  «давно не было активности — перечитать сначала» в текущем виде почти
  не срабатывает. Это поведение Java-оригинала, сохранено без изменений.
- DSN использует только первый хост из `systemTenantConnection.hosts`.
  Если нужен реальный failover между OB-нодами — придётся переписать
  на dial-loop с попытками подключения по очереди.

---

## Полезные команды

```cmd
:: Проверить, что код компилируется и тесты есть
go vet ./...
go build ./...

:: Запустить с DEBUG-логом (отредактируйте logLevel в config.yaml)
obauditor.exe

:: Посмотреть, что записалось в БД
mysql -h 192.168.55.205 -P 2881 -u ocp_monitor@sys -p admintools
SELECT COUNT(*), MAX(login_time) FROM sessions;
SELECT * FROM logfiles;
SELECT * FROM rsyslog_cursor;
```
