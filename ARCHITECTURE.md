# OBAuditor — архитектура Go-реализации

Документ описывает, как устроена Go-версия OBAuditor. Цель — собирать в одной
базе `admintools` информацию о подключениях к OceanBase и о DDL/DCL-операциях,
по событиям рассылать syslog-сообщения.

Документ описывает:

- общую модель выполнения и жизненный цикл одного прогона
- структуру пакетов и зависимостей между ними
- схему БД и роль каждой таблицы
- алгоритм парсинга логов observer.log и obproxy.log
- логику курсоров и идемпотентность
- хранение прогресса и устойчивость к перезапускам

Где удобно, в скобках указываются файлы исходников.

---

## 1. Модель выполнения

OBAuditor — это **batch-приложение с одним прогоном**. Один запуск процесса:

1. читает конфиг,
2. читает все доступные новые строки из observer.log\* и obproxy.log\*,
3. дозаписывает новые DDL/DCL в `ddl_dcl_audit_log`,
4. опционально (по расписанию) запускает cleanup,
5. опционально пересылает накопившиеся события в rsyslog,
6. выходит с финальной строкой статистики.

Никаких daemon-петель, watch-режимов или горутин в фоновом режиме нет. Для
регулярного запуска используется внешний планировщик (Task Scheduler / cron /
DBMS_SCHEDULER в OB). Это сильно упрощает поведение: между прогонами процесс
не держит состояние, всё переживаемое — в БД.

**Однопоточность.** Внутри одного прогона приложение однопоточное. Лог-файлы
обрабатываются последовательно, в БД пишется через одно соединение. Это даёт
предсказуемые гарантии:

- между двумя строками в логе сохраняется их физический порядок;
- запросы к OceanBase идут друг за другом, без конкуренции за tx-локи в `admintools`;
- offset-трекинг в `logfiles` всегда согласован с фактически записанными строками
  в `sessions`.

**autoCommit.** Открытое подключение работает в autoCommit-режиме (поведение
`database/sql` по умолчанию). Каждое `Exec` коммитится сразу. Это означает:

- падение процесса посередине прогона не оставит подвешенных транзакций;
- частично записанные данные = частично продвинутый offset (см. §5 про
  идемпотентность INSERT IGNORE).

Явные транзакции (`db.Begin()` → `tx.Commit()`) применяются точечно:

- при чанковой очистке (`internal/db/cleanup.go`) — каждый чанк-DELETE в
  своей транзакции;
- при обновлении курсора DDL/DCL (`internal/db/ddldcl.go`) — чтобы
  `last_request_time` и `updated_at` обновились атомарно.

---

## 2. Структура проекта

```
ob-auditor/
├── cmd/
│   └── obauditor/main.go          # точка входа, оркестрация
├── internal/
│   ├── config/                    # YAML-конфиг + ConnectionConfig.DSN
│   ├── model/                     # LoginEvent, LogFileRecord (общие POJO)
│   ├── logging/                   # Logger с уровнями DEBUG/INFO/ERROR
│   ├── db/                        # DAO: init, sessions, logfiles, ddldcl, cleanup
│   └── logproc/                   # парсеры, обработчик строк, processor, rsyslog
├── config.yaml                    # конфигурация
├── go.mod
├── go.sum
└── README.md
```

### 2.1. Внешние зависимости

- `github.com/go-sql-driver/mysql` — драйвер для подключения к OceanBase
  (OceanBase MySQL-совместима, поэтому отдельный JDBC-аналог не нужен).
- `gopkg.in/yaml.v3` — парсинг конфига.
- `filippo.io/edwards25519` — транзитивная зависимость mysql-драйвера.

### 2.2. Граф зависимостей пакетов

```
cmd/obauditor/main.go
  ↓ использует все
  config        ← logging
  logging       ← config
  model         (без зависимостей)
  db            ← config, logging, model
  logproc       ← config, logging, model, db
```

Идея простая: `config` и `model` — нижний уровень, без зависимостей друг
от друга. `logging` зависит только от `config` (нужен LogLevel-тип). `db`
держит DAO, не знает про логику парсинга. `logproc` собирает всё вместе.

Импортов друг друга снизу вверх нет — `model` не знает о `db`, `db` не
знает о `logproc`. Это позволяет менять реализацию парсеров не трогая
SQL-слой и наоборот.

---

## 3. Жизненный цикл прогона

`cmd/obauditor/main.go` запускает следующую последовательность:

```
1. config.Read("config.yaml")
   └─ применяет дефолты, определяет LogLevel и CollectorId

2. logging.New(cfg.LogLevel)

3. logproc.SetServerIgnoredUsers / SetProxyIgnoredUsers
   └─ глобальный фильтр пользователей для обоих парсеров

4. db.NewInitializer(...).Initialize()
   ├─ Open("mysql", DSN("oceanbase"))     -- системная БД
   ├─ ensureDatabase("admintools")        -- CREATE DATABASE IF NOT EXISTS
   ├─ Open("mysql", DSN("admintools"))
   ├─ ensureTable() × 6                   -- DDL для всех таблиц
   └─ INSERT IGNORE seed row в audit_collector_state (id=1)

5. sql.Open("mysql", DSN("admintools")) + Ping()
   └─ основное соединение для всех последующих DAO

6. logproc.NewLogFileProcessor(...)
   ├─ ProcessServerDirs(cfg.ObServerLogPaths)
   └─ ProcessProxyDirs(cfg.ObProxyLogPaths)

7. sessionDao.SyncFailedProxySessions()
   └─ закрывает PROXY-строки для неудачных логинов по информации из SERVER

8. db.NewDdlDclAuditDao(...).Collect()       -- если ddlDclAuditMode > 0
   └─ переносит новые строки из GV$OB_SQL_AUDIT в ddl_dcl_audit_log

9. cleanupDao.CleanDdlDclAuditLog / CleanSessions
   └─ только если текущая минута == cleanup.cleanupMinute

10. rsyslog.NewRsyslogSender(...).Send()
    └─ только если rsyslog.host задан

11. Печать финальной статистики
```

Каждый шаг изолирован: ошибка в одном (например, недоступный логов-каталог)
не отменяет следующие. Это упрощает диагностику — финальная строка всё равно
напечатается, видно, что отработало.

---

## 4. Схема БД (`admintools`)

База создаётся автоматически при первом запуске. Схема — 6 таблиц. Все они
определены в `internal/db/init.go` в виде функций `*Table()`, возвращающих
DDL-строку.

### 4.1. `sessions`

Основная таблица аудита подключений. Одна строка = одна попытка входа (как
успешная, так и неудачная). Логин и логофф пишутся в одну и ту же строку
(`logoff_time IS NULL` означает «сессия открыта»).

Ключевые поля:

| Поле | Тип | Смысл |
|---|---|---|
| `id` | BIGINT UNSIGNED auto | PK |
| `source` | VARCHAR(8) | `SERVER` или `PROXY` — из какого лога пришло событие |
| `server_ip` | VARCHAR(64) | IP узла-источника лога |
| `cluster_name` | VARCHAR(128) | имя кластера (PROXY); SERVER всегда пустая строка |
| `session_id` | BIGINT UNSIGNED | sessid (SERVER) или server_sessid (PROXY) |
| `login_time` | DATETIME(6) | время логина из лога |
| `logoff_time` | DATETIME(6) NULL | NULL = сессия открыта |
| `is_success` | TINYINT(1) | 1 = LOGIN_OK, 0 = LOGIN_FAIL |
| `proxy_sessid` | BIGINT UNSIGNED NULL | связующее значение между SERVER и PROXY строками |
| `cs_id` | BIGINT UNSIGNED NULL | client session id (только PROXY) |
| `server_node_ip` | VARCHAR(64) NULL | какой OBServer обслуживал сессию |
| `from_proxy` | TINYINT(1) NULL | флаг from_proxy из SERVER-лога |

Ключи:

- `UNIQUE KEY uk_sess (source, server_ip, cluster_name, session_id, login_time)`
  — обеспечивает идемпотентность `INSERT IGNORE`. Повторная обработка тех
  же строк лога не создаст дубликатов;
- `idx_proxy_sessid` — для быстрого поиска при `UpdateLogoff`;
- `idx_login_time` — для пагинации rsyslog;
- `idx_open (logoff_time)` — для `LoadOpenProxySessids` и
  `SyncFailedProxySessions`.

### 4.2. `logfiles`

Прогресс обработки лог-файлов. Один файл = одна строка.

| Поле | Тип | Смысл |
|---|---|---|
| `collector_id` | VARCHAR(128) | hostname машины, на которой работает OBAuditor |
| `file_dir` | VARCHAR(512) | директория лог-файла |
| `file_name` | VARCHAR(256) | имя файла (например, `observer.log` или `observer.log.20260321`) |
| `file_type` | VARCHAR(16) | `SERVER` или `PROXY` |
| `file_size` | BIGINT | размер при последней обработке |
| `last_line_num` | BIGINT | **байтовый offset** после последней обработанной строки |
| `file_ip` | VARCHAR(64) NULL | IP узла-источника (для SERVER — из первой строки, для PROXY — из `server session born`) |
| `last_timestamp`, `last_tid`, `last_trace_id` | — | резерв для будущего, в текущей версии не используется активно |

Ключ `UNIQUE KEY uq_collector_dir_name` гарантирует, что одна и та же
машина в одной и той же директории видит ровно одну запись на файл.

Поле `last_line_num` — это **точная позиция в байтах**, не номер строки.
Это позволяет вместо построчного перебора делать `file.Seek(offset)` и
сразу продолжать чтение с нужного места — критично на больших ротированных
файлах (по несколько сотен мегабайт).

### 4.3. `audit_collector_state`

Состояние коллектора DDL/DCL. Ровно одна строка `id=1`.

| Поле | Тип | Смысл |
|---|---|---|
| `id` | BIGINT | всегда 1 |
| `collector_id` | VARCHAR(64) | служебное имя, по факту 'ddl_dcl_audit' |
| `last_request_time` | BIGINT | курсор: последний обработанный `request_time` из `GV$OB_SQL_AUDIT` |
| `updated_at` | DATETIME(6) | wall-clock время последнего успешного прохода |

Seed-строку создаёт инициализатор: `INSERT IGNORE ... VALUES (1, 'ddl_dcl_audit', 0)`.

`updated_at` обновляется на каждый успешный прогон, даже если новых строк
не было. Это позволяет fallback-режиму понимать «жив ли основной коллектор»
(см. §6).

### 4.4. `ddl_dcl_audit_log`

Целевая таблица для DDL/DCL-событий, перенесённых из `GV$OB_SQL_AUDIT`.

Колонки повторяют структуру `GV$OB_SQL_AUDIT` с небольшими переименованиями:
`request_time` → `request_ts` (с преобразованием через `usec_to_time`).
Дедупликация — через `UNIQUE KEY uq_req (svr_ip, request_id)`. Повторный
прогон по тому же временному окну не создаст дубликатов.

### 4.5. `ddl_dcl_audit_targets`

Динамическая конфигурация: дополнительные объекты, аудируемые по
шаблону `query_sql LIKE '%name%'`. Базовый набор DDL/DCL-операций
зашит в коде и работает всегда; targets — это **расширение** для DML
по конкретным таблицам (например, отслеживать любые UPDATE на
`finance.invoices`).

| Поле | Тип |
|---|---|
| `tenant_id` | BIGINT NULL — NULL = все тенанты |
| `db_name` | VARCHAR(128) NULL — NULL = любая база |
| `object_name` | VARCHAR(128) NOT NULL — имя таблицы / процедуры / вьюшки |
| `is_active` | TINYINT(1) — 1 = активен |

### 4.6. `rsyslog_cursor`

Три строки — по одной на каждый поток событий (login / logoff / ddl).

| Поле | Тип | Смысл |
|---|---|---|
| `event_type` | VARCHAR(32) PK | 'login', 'logoff' или 'ddl' |
| `last_id` | BIGINT UNSIGNED | курсор по `id` (для login/ddl, и как tiebreaker для logoff) |
| `last_time` | VARCHAR(32) NULL | только для `logoff`: курсор по `logoff_time` |
| `updated_at` | DATETIME(6) NULL | время последней успешной отправки |

Почему logoff устроен особо — см. §7.2.

---

## 5. Обработка лог-файлов

Основной поток — в `internal/logproc/processor.go`. Эта секция объясняет,
как читаются файлы, как продвигается offset и как происходит дедупликация.

### 5.1. Сканирование директории

`processDirectory` для каждой настроенной директории:

1. `dao.LoadByDir(collectorId, fileDir)` — поднимает из БД все строки
   `logfiles` для текущего коллектора и директории. Получается
   `map[fileName]*LogFileRecord`.
2. `os.ReadDir(dirPath)` — список файлов. Фильтр по префиксу:
   - SERVER: `observer.log` или `observer.log.*`
   - PROXY: `obproxy.log` или `obproxy.log.*`
3. Сортировка: ротированные файлы (с расширением) идут **первыми**,
   активный (без расширения) — **последним**. Это важно: события в
   ротированных файлах хронологически раньше, поэтому их нужно
   обработать первыми, чтобы порядок логин/логофф в `sessions`
   совпадал с реальным.

### 5.2. Обработка одного файла

`processFile` для каждого файла:

1. Если файл новый → создаёт `LogFileRecord` с offset=0.
2. Если файл уже был и виден признак ротации
   (`currentSize < oldSize` или `last_timestamp` старше 10 минут) →
   сбрасывает offset в 0.
3. Если `currentSize == oldSize` и `offset >= currentSize` →
   ничего не делает (никаких изменений с прошлого раза).
4. Определяет `serverIp`:
   - для SERVER — из первой строки файла (там есть `address: "ip:port"`);
   - для PROXY — заполняется лениво по ходу чтения (см. ниже).
5. `LoadOpenProxySessids(serverIp)` — поднимает set открытых сессий
   (`logoff_time IS NULL AND is_success=1 AND proxy_sessid IS NOT NULL`)
   для этого узла. Этот set нужен `LogLineHandler`-у, чтобы не
   эмитить logoff для сессий, которых нет в БД.
6. Открывает файл, делает `Seek(startOffset)`, начинает читать через
   `bufio.Reader.ReadString('\n')`.
7. После обработки обновляет `record.FileSize`, `record.LastLineNum`,
   `record.FileIp` и пишет в `logfiles`.

### 5.3. Подсчёт offset-а

Это **самое тонкое место** в работе с файлом. Логика:

```go
for {
    line, err := reader.ReadString('\n')
    if line != "" {
        bytesRead += int64(len(line))   // временно учли байты
        if err == nil {
            // полная строка с \n — обрабатываем
            handler.Handle(stripCR(strings.TrimSuffix(line, "\n")))
        } else {
            // EOF без \n — это незавершённая строка, откатываем счётчик
            bytesRead -= int64(len(line))
        }
    }
    if err == io.EOF {
        break
    }
}
record.LastLineNum = bytesRead
```

Ключевая идея: **обработанной считается только полная строка, заканчивающаяся
`\n`**. Если файл оборвался посередине строки (потому что писатель в OB не
успел дописать), эта строка НЕ учитывается в offset. На следующем запуске
мы вернёмся к её началу и попробуем прочитать ещё раз — возможно, к тому
моменту строка уже допишется.

Это даёт гарантию: между двумя запусками никогда не возникнет ситуации,
когда мы получили половину строки и пытались её распарсить — половина не
парсится, событие потерялось.

Дополнительно: `stripCR` снимает `\r` на конце, если файл записан с
Windows-окончаниями (на практике у OceanBase такого не бывает, но
проверка дешёвая).

### 5.4. Поздний поиск IP в PROXY

OBProxy в строке `server session born` пишет `local_ip:{IP:port}` — это IP,
по которому OBProxy слушает клиента. Этот IP — наш `file_ip` для PROXY.

Но в первой строке файла его обычно нет. Поэтому в `readAndProcess`
параллельно с обработкой строк работает дешёвый параллельный поиск:

```go
needProxyIp := record.FileType == "PROXY" && record.FileIp == ""
...
if needProxyIp && strings.Contains(processed, "server session born") {
    if m := pProxyLocalIp.FindStringSubmatch(processed); m != nil {
        record.FileIp = m[1]
        needProxyIp = false
        dao.UpdateFileIp(record)   // фиксируем сразу
        handler.SetServerIp(record.FileIp)
    }
}
```

Как только IP найден — он немедленно сохраняется в БД и пробрасывается
в handler. Это позволяет последующим строкам того же файла уже знать
ip и правильно писать `server_ip` в `sessions`.

### 5.5. Идемпотентность

Все вставки в `sessions` идут через `INSERT IGNORE`. Уникальный ключ —
`(source, server_ip, cluster_name, session_id, login_time)`. Это значит:

- если мы случайно перечитали кусок файла дважды (например, после
  крэша или ручного сброса offset), дубликатов в БД не появится;
- если ротация была частичной и часть строк мы пропустили — это видно
  только в финальной статистике, но БД останется консистентной.

`logoff_time` обновляется через `UPDATE`, а не `INSERT`. Идемпотентность
обеспечена условием `WHERE logoff_time IS NULL` — повторный logoff не
перезапишет уже зафиксированное время.

---

## 6. Парсинг строк

Парсеры — `internal/logproc/server_parser.go` и `proxy_parser.go`. Оба
возвращают `*model.LoginEvent` (или `nil` если строка нерелевантна).
`LogLineHandler` (`handler.go`) диспетчеризует строки в нужный парсер
по `fileType`.

### 6.1. SERVER-парсер: stateless

`ParseObServerLine` сразу распознаёт две формы:

```
MySQL LOGIN(direct_client_ip="…", client_ip=…, tenant_name=…, user_name=…,
            sessid=…, proxy_sessid=…, use_ssl=…, proc_ret=…,
            from_proxy=…, from_jdbc_client=…, conn->client_type_=…)

connection close(sessid=…, proxy_sessid=…, tenant_id=…, from_proxy=…)
```

Для LOGIN: `proc_ret=0` → `LOGIN_OK`, иначе → `LOGIN_FAIL` с ErrorCode.

Тип клиента (`client_type` в БД) определяется иерархически:
`from_jdbc_client > from_java_client > from_oci_client > conn->client_type_`
(числовое поле, маппится в OBCLIENT / JDBC / MYSQL_CLI / TYPE_N).

Игнорируемые пользователи (`ocp_monitor`, `proxy_ro`, `proxyro` по
умолчанию) отбрасываются на этом этапе — `parseServerLogin` возвращает
nil. LOGOFF-события не фильтруются по пользователю, потому что в строке
`connection close` имени пользователя нет.

### 6.2. PROXY-парсер: stateful

`ObProxyLineParser` хранит две in-memory мапы на время обработки одного
файла:

- `bornMap[cs_id]` — данные сессии (`cluster`, `tenant`, `user`),
  полученные из строки `server session born`. Эти данные нужны позже,
  когда строка `update_cmd_stats` подтвердит результат логина.
- `failMap[cs_id]` — legacy-путь для старых версий OBProxy: фиксирует
  `error_transfer`, чтобы при последующем `do_io_close` правильно
  эмитить `LOGIN_FAIL`.

Главный сигнал результата логина — это **`client_response_bytes`** в
строке `update_cmd_stats sql=OB_MYSQL_COM_LOGIN`:

- `≤ 60 байт` — MySQL OK packet, считаем `LOGIN_OK`;
- `> 60 байт` — MySQL ERR packet (`Access denied for user '...'`),
  считаем `LOGIN_FAIL`.

Логофф (`handle_server_connection_break` + `COM_QUIT`) — одна строка,
без накопления состояния.

Тонкость: stateful парсер живёт в пределах одного файла. Между файлами
состояние не переносится. Это нормально: ротация obproxy.log происходит
по размеру (обычно 1ГБ), и пары born/cmd_stats всегда укладываются
в один файл (это секунды на одну сессию).

### 6.3. Фильтрация служебных пользователей

`SetServerIgnoredUsers` / `SetProxyIgnoredUsers` инициализируют
глобальные `sync.RWMutex`-защищённые мапы. В обоих парсерах есть
фильтрация: SERVER — в `parseServerLogin`, PROXY — в `handleBorn`.

Список идёт в обе мапы из одного источника (`cfg.IgnoredUsers`).
Дефолт — `ocp_monitor, proxy_ro, proxyro`.

---

## 7. DAO слой

`internal/db/` — все взаимодействия с БД, кроме DDL-инициализации
которая живёт там же.

### 7.1. SessionDao

`internal/db/session.go` — четыре основных метода:

```go
InsertLogin(event, fileServerIp)             // INSERT IGNORE
LoadOpenProxySessids(serverIp) → map         // открытые сессии для одного OB-узла
UpdateLogoff(proxySessid, time)              // закрытие через proxy_sessid
UpdateLogoffDirect(sessionId, serverIp, time) // закрытие прямой сессии (без прокси)
SyncFailedProxySessions()                    // reconciliation между SERVER и PROXY
```

`UpdateLogoff` обновляет **обе строки** (SERVER и PROXY) одним запросом,
потому что у них общий `proxy_sessid`. Это атомарно — нет окна, когда
SERVER-строка закрыта, а PROXY-строка ещё нет.

`SyncFailedProxySessions` решает проблему «PROXY зафиксировал успешное
открытие, но SERVER потом отверг логин (например, по сертификату)». В
этом случае PROXY-строка остаётся `logoff_time IS NULL` хотя на самом
деле сессии уже нет. JOIN-UPDATE по `proxy_sessid` находит такие
несоответствия и закрывает PROXY-строку с error_code от SERVER.

### 7.2. UNSIGNED BIGINT

`proxy_sessid`, `session_id`, `cs_id` могут превышать `int64.MaxValue`
(они генерируются как hash или timestamp-based в OB). В Java приходилось
работать через `Long.toUnsignedString` / `Long.parseUnsignedLong`; в Go
всё проще: `uint64` нативно, драйвер `go-sql-driver/mysql` корректно
пишет и читает их при включённом `interpolateParams=true` в DSN.

### 7.3. DdlDclAuditDao

`internal/db/ddldcl.go` — отдельный сборщик, читающий `GV$OB_SQL_AUDIT`
и переносящий новые DDL/DCL в `ddl_dcl_audit_log`.

Алгоритм одного прохода:

1. `last_rt = SELECT last_request_time FROM audit_collector_state WHERE id=1`
2. `new_rt = SELECT MAX(request_time) FROM oceanbase.GV$OB_SQL_AUDIT
             WHERE is_inner_sql = 0 AND request_time > last_rt`
3. Если `new_rt IS NULL` — нет новых строк, делаем heartbeat
   (обновляем `updated_at`) и выходим.
4. `loadTargets()` — поднимаем активные `ddl_dcl_audit_targets`.
5. `buildInsertSQL(targets)` — формирует один большой `INSERT IGNORE ... SELECT`
   с фильтрами:
   - `request_time > last_rt AND request_time <= new_rt`
   - `stmt_type IN (CREATE_TABLE, ALTER_TABLE, ..., GRANT, REVOKE, ...)` —
     основной список DDL/DCL операций;
   - `OR query_sql LIKE '%CREATE USER%'` и т.п. — там, где у OB нет
     отдельного `stmt_type`;
   - `OR stmt_type IN ('DELETE','UPDATE') AND query_sql LIKE '%admintools.sessions%' ...`
     — отслеживание модификаций наших же таблиц аудита (security);
   - `OR <target.condition>` — динамические targets из таблицы;
   - **исключения**: собственные INSERT/UPDATE запросов OBAuditor по сигнатуре
     (`%INSERT IGNORE INTO admintools.ddl_dcl_audit_log%` и др.) — чтобы
     приложение не аудировало само себя в бесконечном цикле.
6. `Exec` с `(last_rt, new_rt)` параметрами.
7. В транзакции обновляем `audit_collector_state`:
   `last_request_time = new_rt, updated_at = NOW(6)`.

**Режимы (`ddlDclAuditMode` из конфига):**

- `0` — выключено;
- `1` — основной (запускается всегда при каждом прогоне);
- `2` — резервный: запускается только если `updated_at` старше 2 минут
  (т.е. основной коллектор где-то лежит). Это нужно, когда у вас несколько
  машин с OBAuditor — на одной mode=1, на остальных mode=2.

### 7.4. CleanupDao

`internal/db/cleanup.go` — удаление старых строк по достижении лимита.
Применяется к двум таблицам: `sessions` и `ddl_dcl_audit_log`.

Стратегия:

1. `COUNT(*)` — если ≤ `maxRows`, выходим.
2. `targetRows = maxRows * 0.9` — оставляем 90% от лимита, чтобы не запускать
   очистку повторно сразу же.
3. `boundary = SELECT id ... ORDER BY id ASC LIMIT 1 OFFSET (count - targetRows)`
   — находим id «границы».
4. Цикл: `DELETE WHERE id < boundary LIMIT chunkSize`, каждый чанк в своей
   транзакции. Пока `ROWS AFFECTED == 0` — выходим.

**Зачем чанки.** При шторме (например, аномалия в каком-то приложении
породила миллион логинов в час) в таблице могут оказаться сотни тысяч
лишних строк. Один большой DELETE может упереться в server-side
ограничения по времени и откатиться целиком. Чанки гарантируют
прогресс: даже если третий чанк откатится, первые два уже зафиксированы.

Cleanup запускается **только если** текущая минута часа совпадает с
`cleanup.cleanupMinute`. Если OBAuditor дёргается каждую минуту, очистка
будет работать раз в час; если каждые 30 секунд — два раза в час и т.п.
Значение `-1` отключает очистку.

---

## 8. Пересылка в rsyslog

`internal/logproc/rsyslog.go` — UDP-отправка событий по RFC 3164.

Три независимых потока:

```
login   → курсор: last_id  (rsyslog_cursor[event_type='login'])
logoff  → курсор: (last_time, last_id)  (rsyslog_cursor[event_type='logoff'])
ddl     → курсор: last_id  (rsyslog_cursor[event_type='ddl'])
```

Каждый поток — пагинация батчами по `rsyslog.batchSize` (500 по умолчанию).
Цикл: `SELECT ... WHERE id > cursor ORDER BY id ASC LIMIT batchSize`, отправить
все, обновить курсор, повторить. Если в батче меньше `batchSize` строк —
выходим.

### 8.1. Почему logoff устроен особо

Для login и ddl курсор — простой `id`, потому что эти события вставляют
новые строки в порядке возрастания id. А logoff — это **UPDATE существующей
строки**: сессия с `id=50` может закрыться позже, чем сессия с `id=200`.
Поэтому курсор по id один не работает.

Решение — пара `(logoff_time, id)`:

```sql
SELECT id, ... FROM sessions
WHERE logoff_time IS NOT NULL
  AND (logoff_time > ? OR (logoff_time = ? AND id > ?))
ORDER BY logoff_time ASC, id ASC
LIMIT ?
```

`logoff_time` — главное поле сортировки; `id` — tiebreaker для случаев,
когда у нескольких сессий совпала секунда (точность DATETIME(6)
микросекундная, такие коллизии редки, но возможны).

При продвижении курсора важная деталь: курсор обновляется на значение
из **последней** строки батча (то есть с максимальным `logoff_time` в
батче), **не** на максимальный id. Это потому что внутри батча id могут
идти в любом порядке (сессия с поздним id могла закрыться раньше).

### 8.2. Формат сообщений

PRI считается как `facility*8 + 6` (severity=info). Хедер:

```
<PRI>Mar  3 14:25:18 hostname obauditor: <message>
```

Тело — текстовое, key=value:

```
LOGIN result=OK source=PROXY user=app1 tenant=biz client_ip=10.0.0.5 \
  session_id=12345 client_type=JDBC time=2026-03-03 14:25:18.123456
```

Если сообщение длиннее 1024 байт (RFC 3164 limit) — обрезается.

`query_sql` в DDL-событиях обрезается до 256 символов, переносы строк
заменяются на пробелы — чтобы syslog-приёмник не путался с
многострочными SQL-запросами.

---

## 9. Конфигурация

`internal/config/config.go`. YAML, читается через `gopkg.in/yaml.v3`.

Структура повторяет XML-аналог из Java-версии (`config.xml`), сохраняя
имена ключей в camelCase.

Дефолты применяются **до** парсинга YAML (через предзаполненный struct),
поэтому отсутствующий ключ → дефолт, явно заданный пустой список → пустой.

### 9.1. DSN

`ConnectionConfig.DSN(dbName)` строит строку подключения для
`go-sql-driver/mysql`:

```
user:password@tcp(host)/db?parseTime=true&loc=Local
   &timeout=5s&readTimeout=30s&writeTimeout=30s
   &interpolateParams=true
```

Параметры:

- `parseTime=true` — DATETIME → `time.Time`;
- `interpolateParams=true` — параметры подставляются в самом драйвере,
  не на сервере. Это критично для корректной работы с `uint64` и
  упрощает чтение debug-логов;
- `timeout`/`readTimeout`/`writeTimeout` — сокет-таймауты на уровне Go;
- **отсутствует `sessionVariables=ob_query_timeout=...`** — некоторые
  версии OceanBase отвечают на `SET ob_query_timeout=N` ошибкой 1054
  («Unknown column 'ob_query_timeout' in 'field_list'»), поскольку
  парсят это выражение как column reference. Server-side timeout мы
  не настраиваем; защиту от зависших запросов даёт `readTimeout`
  на сокете и чанкование cleanup-а.

Из нескольких хостов в `hosts` используется только первый. Failover на
уровне Go-драйвера потребовал бы кастомного dialer-а — пока не реализовано.

### 9.2. CollectorId

Если не задан в YAML — автоматически проставляется hostname машины.
Этот идентификатор уходит в `logfiles.collector_id` и определяет
«scope видимости» каждого экземпляра OBAuditor: коллектор не видит
прогресс других коллекторов в той же БД. Это позволяет одной БД
обслуживать несколько источников.

---

## 10. Логирование

`internal/logging/logger.go`. Простой logger с тремя уровнями:

- **ERROR** — всегда в `stderr`, печатается независимо от уровня;
- **INFO** — в `stdout`, если уровень >= INFO;
- **DEBUG** — в `stdout`, если уровень == DEBUG.

Уровень читается из `config.yaml: logLevel`. Для прогона в кроне
рекомендуется `INFO` (5–10 строк за прогон). DEBUG генерирует много
вывода — полезен при отладке регулярок и offset-логики.

Финальная строка статистики печатается через прямой `fmt.Printf`
(не через logger), потому что её всегда нужно видеть независимо от
уровня.

---

## 11. Безопасность и устойчивость

### 11.1. Что переживается при перезапуске

- Краш посередине обработки файла: offset в `logfiles` не обновлён,
  но в `sessions` уже есть `INSERT IGNORE` для обработанных строк.
  Следующий запуск перечитает с того же offset, дубликаты не появятся
  (UNIQUE KEY).
- Краш во время cleanup: каждый чанк — отдельная транзакция,
  закоммиченные чанки остаются, незакоммиченный откатывается.
- Краш во время DDL/DCL Collect: `INSERT IGNORE` отрабатывает целиком
  или не отрабатывает; курсор `last_request_time` обновляется в
  отдельной транзакции **после** INSERT. Если INSERT прошёл, а
  обновление курсора нет — следующий прогон повторит INSERT, но
  дубликатов не будет благодаря `UNIQUE KEY uq_req`.
- Краш во время rsyslog Send: курсор обновляется батчами; повторно
  отправляется максимум один батч (вероятный лишний дубликат в
  syslog-приёмнике, что обычно приемлемо).

### 11.2. Что НЕ переживается

- Изменение схемы БД на лету (нужно ронять старые таблицы и стартовать заново).
- Удаление seed-строки из `audit_collector_state` — рекомендуется
  делать через `UPDATE`, не `DELETE`.
- Сильное расхождение часов между машиной OBAuditor и OBServer —
  логофф-курсор работает по `logoff_time` из БД (генерируется
  OB-сервером при `UPDATE`), а порог `stale=2min` для fallback-режима
  считается по local clock OBAuditor.

### 11.3. Что считается секретом

Пароль от OceanBase — в `config.yaml` в открытом виде. Это сознательное
решение: внешний планировщик и так держит секреты в env. Если нужно
вытащить пароль из файла — есть штатные пути: env var или внешний
secrets-vault, но в текущей версии порт `PasswordEnricher` из Java не
сделан. Файл следует защищать средствами ОС (`icacls` на Windows,
`chmod 600` на Linux).

---

## 12. Тонкие места и компромиссы

| Решение | Причина | Альтернатива |
|---|---|---|
| Однопроцессный batch без daemon-режима | Простота, переживаемость через БД, отсутствие гонок | Daemon с watcher-ами файлов — сложнее, нужны блокировки в БД |
| `interpolateParams=true` в DSN | Корректная работа с UNSIGNED BIGINT, читаемые debug-логи | Server-side prepared statements — быстрее для тысяч одинаковых запросов, но нам не нужно |
| Один host из `systemTenantConnection.hosts` | Простота; failover в Go-драйвере не штатный | Кастомный `mysql.DialContextFunc` с round-robin |
| Offset считается только для полных строк | Безопасность: нет полу-распарсенных событий | Можно было бы использовать `file.Tell()` после ReadString — но тогда EOF посередине строки пропустился бы и она потерялась |
| Игнорируемые пользователи — глобальная mutex-защищённая map | Дешевле передачи через параметр; SetXxx вызывается один раз на старте | Передавать как поле LogFileProcessor — больше boilerplate, никаких других плюсов |
| stateful PROXY-парсер живёт в пределах файла | OBProxy ротирует не чаще раза в сутки; born+cmd_stats укладываются в секунды | Хранить bornMap в БД — сложно и не нужно |
| logoff курсор по `(logoff_time, id)` | UPDATE существующей строки нарушает порядок по id | Можно было бы добавить `logoff_sequence` колонку — больше места и индексов |
| ob_query_timeout НЕ выставляется через DSN | Несовместимость с некоторыми версиями OB | На стороне сервера через `SET GLOBAL ob_query_timeout = N` (если есть права) |

---

## 13. Финальная статистика

В конце каждого прогона печатается одна строка:

```
[Main] Done. vgo-20260521-1 Total time: 943 ms | lines: 37490 | inserted: 3 \
  | logoff: 3 | logoffMiss: 0 | ddlDcl: 1 | cleanedDdlDcl: 0 | cleanedSessions: 0 \
  | rsyslogLogin: 5 | rsyslogLogoff: 4 | rsyslogDdl: 1
```

- `lines` — общее число обработанных строк во всех файлах;
- `inserted` — `INSERT IGNORE` в `sessions` (могут быть LOGIN_OK и LOGIN_FAIL);
- `logoff` — успешных закрытий сессий;
- `logoffMiss` — LOGOFF для сессий, которые не были в in-memory set (legacy
  закрытия после рестарта — норма, ненулевое значение не баг);
- `ddlDcl` — новых строк в `ddl_dcl_audit_log`;
- `cleanedDdlDcl` / `cleanedSessions` — удалённых при cleanup;
- `rsyslogLogin/Logoff/Ddl` — отправлено в rsyslog.

Эту строку можно скармливать любому log-аналитику для тренда: рост `lines`
показывает нагрузку, ненулевые `cleaned*` — что приближаемся к лимитам,
несоответствие `inserted` и `rsyslogLogin` на длинном горизонте — что
syslog-приёмник недоступен.
