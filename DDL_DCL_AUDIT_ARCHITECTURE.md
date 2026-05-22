# DDL/DCL аудит OceanBase — описание процесса и хранимой процедуры

Документ описывает процесс аудита DDL/DCL операций в OceanBase, реализованный
в виде хранимой процедуры `admintools.sp_collect_ddl_dcl_audit`. Логика
полностью самодостаточна и работает внутри SQL-сервера: все входные данные
читаются из БД, все результаты пишутся в БД.

Документ описывает:

- что такое DDL/DCL аудит и зачем он нужен;
- источник данных (`GV$OB_SQL_AUDIT`);
- задействованные таблицы в `admintools`;
- алгоритм работы хранимой процедуры по шагам;
- логику курсора `last_request_time` и идемпотентность;
- состав фильтров (зашитые + динамические);
- ограничения и тонкие места.

---

## 1. Что такое DDL/DCL аудит

**DDL** (Data Definition Language) — операции изменения схемы БД:
`CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `CREATE INDEX`, и так далее.

**DCL** (Data Control Language) — операции управления доступом и
пользователями: `CREATE USER`, `ALTER USER`, `GRANT`, `REVOKE`,
`SET PASSWORD`.

Эти операции обычно происходят редко (минуты или часы между ними), но
имеют высокую значимость для безопасности и compliance: важно знать
кто, когда и какой именно DDL/DCL запустил, особенно если речь о
правах пользователей или изменении структуры production-таблиц.

В OceanBase сам сервер не ведёт отдельного DDL-аудит-лога. Вместо
этого все выполненные запросы (включая обычный DML) попадают во
**внутреннюю системную view `GV$OB_SQL_AUDIT`**, откуда мы их и
выбираем по фильтру.

### 1.1. Что ещё аудируется кроме чистых DDL/DCL

Помимо собственно DDL/DCL процедура также собирает:

- **управление пользователями через LIKE-фильтры** — `CREATE USER`,
  `ALTER USER`, `lock_user()`, `unlock_user()`. В OB нет отдельных
  `stmt_type` для всех этих случаев, поэтому ловим по тексту запроса;
- **изменения собственных таблиц аудита** (`admintools.sessions`,
  `admintools.ddl_dcl_audit_log`, `admintools.ddl_dcl_audit_targets`)
  через `DELETE`/`UPDATE`. Это security-аудит: любая попытка
  подделать или удалить записи аудита фиксируется в том же аудит-логе;
- **произвольные объекты** из таблицы `ddl_dcl_audit_targets` — это
  расширение для DML по конкретным критичным таблицам. Например, если
  нужно отслеживать любые `UPDATE` на `finance.invoices`, заводим
  target и аудитуем.

---

## 2. Источник данных: `GV$OB_SQL_AUDIT`

`oceanbase.GV$OB_SQL_AUDIT` — внутренняя view OceanBase, в которую
сервер пишет информацию о каждом выполненном запросе. View объединяет
данные со всех узлов кластера (префикс `GV$` = global view).

Ключевые поля, которые использует процедура:

| Поле | Тип | Использование |
|---|---|---|
| `request_id` | BIGINT | уникальный id запроса в рамках узла; ключ дедупликации |
| `svr_ip` | VARCHAR | IP OBServer-узла, выполнившего запрос |
| `request_time` | BIGINT | timestamp выполнения в **микросекундах от epoch** (используется как курсор) |
| `is_inner_sql` | TINYINT | 1 = внутренний запрос OB, 0 = пользовательский — фильтруем `= 0` |
| `stmt_type` | VARCHAR | тип запроса (`CREATE_TABLE`, `ALTER_USER` и т.п.); основной критерий отбора |
| `query_sql` | LONGTEXT | сам текст SQL; используется в LIKE-фильтрах |
| `tenant_id` / `tenant_name` | | тенант |
| `user_id` / `user_name` | | пользователь |
| `db_name` | | текущая схема при выполнении запроса |
| `client_ip` / `user_client_ip` | | IP клиента (OBProxy / реальный) |
| `proxy_user` | | пользователь при proxy-подключении |
| `sid` | | session id |
| `ret_code` | BIGINT | 0 = успех, иначе код ошибки |
| `affected_rows` | BIGINT | строк затронуто (для DML) |
| `elapsed_time` | BIGINT | время выполнения в микросекундах |
| `retry_cnt` | BIGINT | счётчик повторов |

### 2.1. Особенности `request_time`

Это **микросекунды от epoch** (Unix timestamp × 1_000_000), а не DATETIME.
Из-за этого:

- значения большие (порядка `1.7e15` для современных дат);
- для отображения нужна функция `usec_to_time(request_time)` → DATETIME;
- сравнение `>`, `<` работает напрямую, потому что значения монотонны;
- именно это поле используется как курсор продвижения — оно есть на
  каждой строке, всегда заполнено, монотонно возрастает.

### 2.2. Время жизни данных в `GV$OB_SQL_AUDIT`

`GV$OB_SQL_AUDIT` — это **memory-resident** view с **circular buffer
поведением**: OceanBase хранит конечный объём истории (обычно несколько
сот мегабайт на узел), после чего старые записи вытесняются новыми.

Из этого следует:

- если процедура долго не запускалась, часть строк уже могла исчезнуть —
  это нормально, аудит «best effort», но не «100% гарантированный»;
- регулярный запуск (раз в минуту-две) практически гарантирует, что мы
  ничего не упустим в нормальных условиях;
- если узел кластера перезагружался — его буфер пуст, данные за период
  до рестарта потеряны (для этого узла).

---

## 3. Задействованные таблицы

Все три таблицы — в БД `admintools`, созданы единым инициализатором.

### 3.1. `admintools.audit_collector_state`

Состояние коллектора. **Ровно одна строка с `id=1`**.

```sql
CREATE TABLE `audit_collector_state` (
  `id`                BIGINT       NOT NULL,
  `collector_id`      VARCHAR(64)  NOT NULL COMMENT 'Идентификатор коллектора',
  `last_request_time` BIGINT       NOT NULL DEFAULT 0 COMMENT 'request_time последней обработанной записи GV$OB_SQL_AUDIT',
  `updated_at`        DATETIME(6)      NULL COMMENT 'Wall-clock время последнего успешного сбора',
  PRIMARY KEY (`id`)
) COMMENT = 'Состояние DDL/DCL коллектора';
```

| Поле | Смысл |
|---|---|
| `id` | Всегда `1`. Зарезервировано на случай, если в будущем понадобится несколько коллекторов в одной БД (каждый со своим id). |
| `collector_id` | Семантический идентификатор. Зафиксировано как `'ddl_dcl_audit'`. Сейчас не используется логикой, нужен для читаемости. |
| `last_request_time` | **Курсор**. Микросекунды от epoch. Хранит `request_time` последней обработанной строки. На следующем прогоне берём строки с `request_time > last_request_time`. |
| `updated_at` | **Heartbeat**. Wall-clock время последнего успешного прогона. Обновляется даже если новых строк не было. |

Seed-строка создаётся при инициализации БД:
```sql
INSERT IGNORE INTO audit_collector_state (id, collector_id, last_request_time)
VALUES (1, 'ddl_dcl_audit', 0);
```

Стартовое значение `last_request_time = 0` означает «прогнать всю
историю с начала времён», что на первом запуске даст весь доступный
буфер `GV$OB_SQL_AUDIT`.

### 3.2. `admintools.ddl_dcl_audit_log`

Целевая таблица — здесь оседают собранные события.

```sql
CREATE TABLE `ddl_dcl_audit_log` (
  `id`             BIGINT      NOT NULL AUTO_INCREMENT,
  `collected_at`   DATETIME(6) NOT NULL DEFAULT NOW(6) COMMENT 'Время вставки записи',
  `request_id`     BIGINT      NOT NULL                COMMENT 'Request ID в OB (ключ дедупликации)',
  `svr_ip`         VARCHAR(46) NOT NULL                COMMENT 'IP OBServer-узла',
  `tenant_id`      BIGINT          NULL,
  `tenant_name`    VARCHAR(64)     NULL,
  `user_id`        BIGINT          NULL,
  `user_name`      VARCHAR(64)     NULL,
  `proxy_user`     VARCHAR(128)    NULL,
  `client_ip`      VARCHAR(46)     NULL COMMENT 'IP OBProxy или клиента',
  `user_client_ip` VARCHAR(46)     NULL COMMENT 'Реальный IP клиента',
  `sid`            BIGINT UNSIGNED NULL,
  `db_name`        VARCHAR(128)    NULL,
  `stmt_type`      VARCHAR(128)    NULL,
  `query_sql`      LONGTEXT        NULL,
  `ret_code`       BIGINT          NULL,
  `affected_rows`  BIGINT          NULL,
  `request_ts`     DATETIME(6) NOT NULL COMMENT 'Время начала выполнения',
  `elapsed_time`   BIGINT          NULL,
  `retry_cnt`      BIGINT          NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_req` (`svr_ip`, `request_id`),
  KEY `idx_request_ts` (`request_ts`),
  KEY `idx_user_name`  (`user_name`),
  KEY `idx_stmt_type`  (`stmt_type`)
) COMMENT = 'DDL/DCL аудит из GV$OB_SQL_AUDIT';
```

Принципиальные моменты:

- **`UNIQUE KEY uq_req (svr_ip, request_id)`** — обеспечивает
  идемпотентность: `INSERT IGNORE` повторно пришедшую запись
  тихо отбросит. Сочетание `svr_ip + request_id` уникально, потому
  что `request_id` уникален в пределах одного узла, а `svr_ip`
  различает узлы кластера.
- `request_ts` — DATETIME(6), полученный из `request_time` (BIGINT
  микросекунды) через `usec_to_time()`. Удобнее для запросов и индексов.
- `collected_at` — wall-clock время вставки (отличается от `request_ts`
  на величину задержки коллектора, обычно секунды).
- `query_sql` хранится в LONGTEXT, **но** перед вставкой обрезается
  ведущий комментарий-хинт (`/*+ ... */`) через REGEXP_REPLACE
  (см. §4.5).
- Индексы по `request_ts`, `user_name`, `stmt_type` — для типичных
  запросов аналитики и отчётности.

### 3.3. `admintools.ddl_dcl_audit_targets`

Динамические правила — какие ещё запросы аудитовать дополнительно к
встроенным DDL/DCL.

```sql
CREATE TABLE `ddl_dcl_audit_targets` (
  `id`          BIGINT       NOT NULL AUTO_INCREMENT,
  `tenant_id`   BIGINT           NULL COMMENT 'NULL = все тенанты',
  `db_name`     VARCHAR(128)     NULL COMMENT 'NULL = любая база',
  `object_name` VARCHAR(128) NOT NULL COMMENT 'Имя таблицы, процедуры, вьюшки',
  `description` VARCHAR(512)     NULL,
  `is_active`   TINYINT(1)   NOT NULL DEFAULT 1,
  `created_at`  DATETIME(6)  NOT NULL DEFAULT NOW(6),
  PRIMARY KEY (`id`),
  KEY `idx_tenant` (`tenant_id`),
  KEY `idx_active` (`is_active`)
);
```

| Поле | Смысл |
|---|---|
| `tenant_id` | Зарезервировано на будущее. Сейчас процедура **не использует** это поле (см. §6.2). |
| `db_name` | Если задано — условие строится для конкретной базы (`db_name = '...' AND query_sql LIKE '%object%'` ИЛИ `query_sql LIKE '%db.object%'`). Если NULL/пусто — ловим объект в любой базе. |
| `object_name` | Имя объекта (таблица, view, процедура и т.п.). Обязательное. Используется в `LIKE '%object_name%'`. |
| `description` | Свободный текст, для документирования. |
| `is_active` | `1` = правило применяется, `0` = выключено. Удобно временно отключать без удаления. |

**Примеры:**

```sql
-- Аудитовать любые запросы упоминающие таблицу invoices в БД finance
INSERT INTO admintools.ddl_dcl_audit_targets (db_name, object_name, description)
VALUES ('finance', 'invoices', 'Финансовый аудит счетов-фактур');

-- Аудитовать любые упоминания таблицы api_keys в ЛЮБОЙ базе
INSERT INTO admintools.ddl_dcl_audit_targets (db_name, object_name, description)
VALUES (NULL, 'api_keys', 'Любое обращение к ключам API');

-- Временно отключить правило
UPDATE admintools.ddl_dcl_audit_targets SET is_active = 0 WHERE id = 5;
```

Условие LIKE сравнивает по `query_sql` подстроку, поэтому будут
ловиться SELECT, UPDATE, DELETE, JOIN, DROP — всё, что упоминает
имя объекта в тексте запроса. Это сознательный широкий захват для
high-value таблиц.

---

## 4. Хранимая процедура: `sp_collect_ddl_dcl_audit`

Сигнатура:

```sql
CREATE PROCEDURE admintools.sp_collect_ddl_dcl_audit(
    OUT p_inserted          BIGINT,
    OUT p_new_request_time  BIGINT
)
    SQL SECURITY INVOKER
    COMMENT 'Перенос новых DDL/DCL событий из GV$OB_SQL_AUDIT в ddl_dcl_audit_log'
```

Два OUT-параметра:

- `p_inserted` — сколько строк реально вставилось (после INSERT IGNORE);
- `p_new_request_time` — новое значение курсора после прогона.

Вызов:

```sql
CALL admintools.sp_collect_ddl_dcl_audit(@inserted, @new_rt);
SELECT @inserted AS inserted, @new_rt AS new_request_time;
```

`SQL SECURITY INVOKER` — процедура выполняется с правами вызывающего,
**не** с правами создателя. Это значит, что у вызывающего пользователя
должны быть:

- `SELECT` на `oceanbase.GV$OB_SQL_AUDIT`;
- `SELECT`, `INSERT`, `UPDATE` на `admintools.*`;
- `EXECUTE` на саму процедуру.

Если процедура создаётся под админом (`root@sys`), а вызывается под
обычным пользователем (`ocp@%`), то именно у `ocp` должны быть эти права.

### 4.1. Локальные переменные

```sql
DECLARE v_last_rt        BIGINT  DEFAULT 0;
DECLARE v_new_rt         BIGINT  DEFAULT NULL;
DECLARE v_inserted       BIGINT  DEFAULT 0;

DECLARE v_tenant_id      BIGINT;
DECLARE v_db_name        VARCHAR(128);
DECLARE v_obj_name       VARCHAR(128);
DECLARE v_db_esc         VARCHAR(256);
DECLARE v_obj_esc        VARCHAR(256);
DECLARE v_done           TINYINT DEFAULT 0;
DECLARE v_dyn_targets    LONGTEXT DEFAULT '';

DECLARE cur_targets CURSOR FOR
    SELECT tenant_id, db_name, object_name
    FROM   admintools.ddl_dcl_audit_targets
    WHERE  is_active = 1
    ORDER  BY id;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;
```

- `v_last_rt`, `v_new_rt` — границы окна обработки;
- `v_inserted` — счётчик для OUT-параметра;
- `v_*` для целей курсора по targets;
- `v_dyn_targets` — накапливаемая строка `OR query_sql LIKE '%...%' OR ...`;
- `cur_targets` — курсор по активным targets;
- handler `NOT FOUND` — выставляет `v_done=1` когда FETCH дойдёт до конца.

### 4.2. Шаг 1: чтение курсора

```sql
SELECT last_request_time
  INTO v_last_rt
  FROM admintools.audit_collector_state
 WHERE id = 1;

IF v_last_rt IS NULL THEN
    SET v_last_rt = 0;
END IF;
```

В нормальной ситуации seed-строка существует и `last_request_time` —
большое число (микросекунды). Защита `IF v_last_rt IS NULL` срабатывает
только если кто-то удалил/обнулил seed-строку.

### 4.3. Шаг 2: верхняя граница окна

```sql
SELECT MAX(request_time)
  INTO v_new_rt
  FROM oceanbase.GV$OB_SQL_AUDIT
 WHERE is_inner_sql = 0
   AND request_time > v_last_rt;
```

Зачем явный `MAX` отдельным запросом, а не одна большая `INSERT...SELECT`:

1. Нужна **фиксированная** верхняя граница для INSERT — иначе если за
   время выполнения INSERT в `GV$OB_SQL_AUDIT` прилетят новые строки,
   они могут попасть в текущий батч, и потом мы их пропустим
   на следующем прогоне.
2. Нужно понять «есть ли вообще что обрабатывать» — если новых строк
   нет, мы экономим на построении динамического SQL и `PREPARE`.
3. Простой `MAX(request_time)` дёшев — индекс по `request_time` в
   `GV$OB_SQL_AUDIT` есть, запрос отрабатывает за миллисекунды.

`is_inner_sql = 0` — отсев служебных внутренних запросов OB. Они нам
неинтересны и просто шумят в аудите.

### 4.4. Шаг 3: нет новых строк → heartbeat и выход

```sql
IF v_new_rt IS NULL THEN
    UPDATE admintools.audit_collector_state
       SET updated_at = NOW(6)
     WHERE id = 1;

    SET p_inserted         = 0;
    SET p_new_request_time = v_last_rt;
ELSE
    -- ... основная ветка
END IF;
```

`v_new_rt IS NULL` означает «`MAX` вернул NULL» — то есть строк с
`request_time > v_last_rt` вообще нет. В этом случае мы **только
обновляем `updated_at`** и выходим.

Зачем нужен heartbeat: для будущего fallback-режима (когда есть
несколько коллекторов и один из них работает только если основной
давно не отчитывался). Сейчас режимы не используются в SP-варианте,
но поле полезно и для мониторинга — можно по `updated_at` понять,
жив ли коллектор.

`p_new_request_time = v_last_rt` — отдаём наружу старое значение
курсора, чтобы у вызывающего была однозначная информация.

### 4.5. Шаг 4: построение динамической части WHERE

```sql
OPEN cur_targets;
target_loop: LOOP
    FETCH cur_targets INTO v_tenant_id, v_db_name, v_obj_name;
    IF v_done = 1 THEN
        LEAVE target_loop;
    END IF;

    SET v_obj_esc = REPLACE(v_obj_name, '''', '''''');

    IF v_db_name IS NOT NULL AND v_db_name <> '' THEN
        SET v_db_esc = REPLACE(v_db_name, '''', '''''');
        SET v_dyn_targets = CONCAT(
            v_dyn_targets,
            '     OR (query_sql LIKE ''%', v_db_esc, '.', v_obj_esc, '%''',
            ' OR (db_name = ''', v_db_esc, ''' AND query_sql LIKE ''%', v_obj_esc, '%''))'
        );
    ELSE
        SET v_dyn_targets = CONCAT(
            v_dyn_targets,
            '     OR query_sql LIKE ''%', v_obj_esc, '%'''
        );
    END IF;
END LOOP target_loop;
CLOSE cur_targets;
SET v_done = 0;
```

Что здесь происходит:

1. **Открываем курсор** по `ddl_dcl_audit_targets WHERE is_active = 1`.
2. **Для каждой строки** строим кусок SQL и приклеиваем к
   накапливающейся переменной `v_dyn_targets`.
3. **Экранирование** — одинарные кавычки удваиваются (`'` → `''`).
   Это стандартное SQL-экранирование. Защищает от того, что кто-то
   занёс в target имя объекта вроде `O'Brien'); DROP TABLE--`.
4. **Две формы условия**:
   - С базой: `query_sql LIKE '%db.object%' OR (db_name = 'db' AND query_sql LIKE '%object%')`.
     Первая часть ловит fully-qualified имя (`SELECT * FROM finance.invoices`),
     вторая — короткое имя при активной базе данных (`USE finance; SELECT * FROM invoices`).
   - Без базы (db_name NULL/пусто): просто `query_sql LIKE '%object%'`.
5. **Сброс `v_done = 0`** в конце — на случай если процедура будет
   расширяться вторым курсором, чтобы handler не сработал преждевременно.

### 4.6. Шаг 5: сборка и выполнение `INSERT IGNORE` через PREPARE

```sql
SET @last_rt = v_last_rt;
SET @new_rt  = v_new_rt;

SET @sql = CONCAT(
    'INSERT IGNORE INTO admintools.ddl_dcl_audit_log (',
    '  request_id, svr_ip, tenant_id, tenant_name,',
    '  user_id, user_name, proxy_user,',
    '  client_ip, user_client_ip, sid, db_name,',
    '  stmt_type, query_sql,',
    '  ret_code, affected_rows, request_ts, elapsed_time, retry_cnt',
    ') ',
    'SELECT',
    '  request_id, svr_ip, tenant_id, tenant_name,',
    '  user_id, user_name, proxy_user,',
    '  client_ip, user_client_ip, sid, db_name,',
    '  stmt_type,',
    '  REGEXP_REPLACE(query_sql, ''^[[:space:]]*/[*].*?[*]/[[:space:]]*'', ''''),',
    '  ret_code, affected_rows, usec_to_time(request_time), elapsed_time, retry_cnt',
    ' FROM oceanbase.GV$OB_SQL_AUDIT',
    ' WHERE is_inner_sql = 0',
    '   AND request_time >  ?',
    '   AND request_time <= ?',
    '   AND stmt_type NOT IN (''VARIABLE_SET'')',
    '   AND query_sql NOT LIKE ''%INSERT IGNORE INTO admintools.ddl_dcl_audit_log%''',
    '   AND query_sql NOT LIKE ''%UPDATE sessions SET logoff_time%''',
    '   AND query_sql NOT LIKE ''%UPDATE sessions p JOIN sessions s%''',
    '   AND query_sql NOT LIKE ''%sp_collect_ddl_dcl_audit%''',
    '   AND (',
    '     stmt_type IN (',
    '       ''CREATE_TABLE'',''ALTER_TABLE'',''DROP_TABLE'',',
    '       ''CREATE_INDEX'',''DROP_INDEX'',',
    '       ''CREATE_VIEW'',''DROP_VIEW'',',
    '       ''CREATE_DATABASE'',''DROP_DATABASE'',',
    '       ''TRUNCATE_TABLE'',''RENAME_TABLE'',',
    '       ''CREATE_TENANT'',''DROP_TENANT'',',
    '       ''DROP_USER'',''RENAME_USER'',',
    '       ''GRANT'',''REVOKE'',',
    '       ''ALTER_USER'',''SET_PASSWORD''',
    '     )',
    '     OR (',
    '         query_sql LIKE ''%CREATE USER%''',
    '         OR query_sql LIKE ''%ALTER USER%''',
    '         OR query_sql LIKE ''%lock_user(%''',
    '         OR query_sql LIKE ''%unlock_user(%''',
    '     )',
    '     OR (',
    '       stmt_type IN (''DELETE'', ''UPDATE'')',
    '       AND (',
    '         query_sql LIKE ''%admintools.sessions%''',
    '         OR query_sql LIKE ''%admintools.ddl_dcl_audit_log%''',
    '         OR (db_name = ''admintools'' AND query_sql LIKE ''%sessions%'')',
    '         OR (db_name = ''admintools'' AND query_sql LIKE ''%ddl_dcl_audit_log%'')',
    '         OR query_sql LIKE ''%admintools.ddl_dcl_audit_targets%''',
    '         OR (db_name = ''admintools'' AND query_sql LIKE ''%ddl_dcl_audit_targets%'')',
    '       )',
    '     )',
    v_dyn_targets,
    '   )'
);

PREPARE  stmt FROM @sql;
EXECUTE  stmt USING @last_rt, @new_rt;
SET      v_inserted = ROW_COUNT();
DEALLOCATE PREPARE stmt;
```

Здесь самое интересное. Разберём блок за блоком.

#### 4.6.1. Зачем PREPARE/EXECUTE

Динамический WHERE можно построить только во время выполнения (после
курсора по targets), поэтому делаем `CONCAT` → `PREPARE` → `EXECUTE`.

Параметры `?, ?` подставляются на этапе EXECUTE через user-variables
`@last_rt, @new_rt`. Это безопаснее, чем `CONCAT`-ить числа прямо в
текст, и даёт лучшее планирование.

Почему через `@last_rt`, а не напрямую `EXECUTE stmt USING v_last_rt`:
в MySQL/OceanBase `EXECUTE USING` принимает **только** user-variables
(те, что с `@`), не локальные переменные процедуры. Поэтому мы
копируем `v_last_rt → @last_rt` перед вызовом.

#### 4.6.2. SELECT-часть — что переносим

Колонки переносятся практически 1:1 из `GV$OB_SQL_AUDIT`, с двумя
преобразованиями:

1. **`query_sql` → `REGEXP_REPLACE(query_sql, '^[[:space:]]*/[*].*?[*]/[[:space:]]*', '')`**
   — отрезаем ведущий комментарий-хинт. В OceanBase многие
   приложения и сама OB прокидывают хинты вида `/*+ READ_CONSISTENCY(WEAK) */`
   в начало запроса. Они визуально шумят в аудите и мешают группировке
   запросов. Регулярка съедает пробелы, `/* ... */` (non-greedy через
   `.*?`), и завершающие пробелы.

2. **`request_time` (BIGINT микросекунды) → `usec_to_time(request_time)`** —
   получаем DATETIME, который лезет в поле `request_ts DATETIME(6)`.

Остальные колонки переносятся как есть. Если поле в `GV$OB_SQL_AUDIT`
было NULL, оно станет NULL в нашей таблице. Для типа: `BIGINT UNSIGNED`
из `sid` отображается на `BIGINT UNSIGNED NULL` в нашей таблице — без
конверсии.

#### 4.6.3. WHERE-часть — окно по времени

```sql
WHERE is_inner_sql = 0
  AND request_time >  ?       -- v_last_rt
  AND request_time <= ?       -- v_new_rt
```

Полуоткрытый интервал `(last_rt, new_rt]`:

- **строго `>` слева** — `last_rt` уже был обработан на прошлом прогоне;
- **`<=` справа** — `new_rt` — это `MAX(request_time)`, мы знаем что
  такие строки есть и хотим их включить.

`is_inner_sql = 0` — отсев служебных внутренних запросов OB.

#### 4.6.4. Глобальные исключения

```sql
AND stmt_type NOT IN ('VARIABLE_SET')
AND query_sql NOT LIKE '%INSERT IGNORE INTO admintools.ddl_dcl_audit_log%'
AND query_sql NOT LIKE '%UPDATE sessions SET logoff_time%'
AND query_sql NOT LIKE '%UPDATE sessions p JOIN sessions s%'
AND query_sql NOT LIKE '%sp_collect_ddl_dcl_audit%'
```

- **`VARIABLE_SET`** — всякие `SET autocommit=1`, `SET names utf8`
  и так далее. Шум, не нужен в аудите.
- **`INSERT IGNORE INTO admintools.ddl_dcl_audit_log`** — наш
  собственный INSERT. Без этого исключения каждый прогон процедуры
  поймал бы свой же INSERT и попытался его записать снова → попадание
  в рекурсивную петлю и фейерверк строк в логе.
- **`UPDATE sessions SET logoff_time`** — UPDATE-ы которые делает
  Go-часть OBAuditor при закрытии сессий. Они не относятся к DDL/DCL,
  это штатная работа коллектора.
- **`UPDATE sessions p JOIN sessions s`** — reconciliation-запрос
  `SyncFailedProxySessions` из Go-части. Тоже служебный.
- **`sp_collect_ddl_dcl_audit`** — любые упоминания самой процедуры
  (например, `CALL` или `CREATE PROCEDURE`). Само-исключение, чтобы
  отладка не засоряла аудит.

Это сделано через `query_sql NOT LIKE`, потому что в OB нет понятия
«мой собственный запрос» в данных audit-view — приходится опознавать
по тексту. Маленький минус — кто-то может намеренно встроить эти
строки в свой SQL и таким образом скрыться из аудита; но это
требует знания внутренностей коллектора и в рамках threat-модели
приемлемо.

#### 4.6.5. Основные критерии включения

После всех `AND` фильтров идёт большой блок `AND ( ... )` который
содержит **дизъюнкцию (OR-список) критериев включения**. То есть строка
попадёт в аудит, если выполняется **хотя бы одно** из:

##### (1) Зашитый список DDL/DCL `stmt_type`:

```sql
stmt_type IN (
    'CREATE_TABLE','ALTER_TABLE','DROP_TABLE',
    'CREATE_INDEX','DROP_INDEX',
    'CREATE_VIEW','DROP_VIEW',
    'CREATE_DATABASE','DROP_DATABASE',
    'TRUNCATE_TABLE','RENAME_TABLE',
    'CREATE_TENANT','DROP_TENANT',
    'DROP_USER','RENAME_USER',
    'GRANT','REVOKE',
    'ALTER_USER','SET_PASSWORD'
)
```

Это «канонические» DDL/DCL. Если OceanBase разметил запрос как один из
этих типов — пишем в аудит. Самый быстрый и точный критерий.

##### (2) Управление пользователями через LIKE:

```sql
OR (
    query_sql LIKE '%CREATE USER%'
    OR query_sql LIKE '%ALTER USER%'
    OR query_sql LIKE '%lock_user(%'
    OR query_sql LIKE '%unlock_user(%'
)
```

В OceanBase у `CREATE USER` есть отдельный `stmt_type` (вроде
`CREATE_USER`), но мы для надёжности добавляем LIKE-проверку — на
случай, если в какой-то версии OB тип не выставляется или если
запрос идёт через хранимку. Аналогично для `ALTER USER`.

`lock_user()` и `unlock_user()` — встроенные функции для блокировки
учётных записей. Они проходят как CALL и не имеют отдельного
DDL/DCL `stmt_type`, ловим только по тексту.

##### (3) Изменения собственных таблиц аудита:

```sql
OR (
    stmt_type IN ('DELETE', 'UPDATE')
    AND (
        query_sql LIKE '%admintools.sessions%'
        OR query_sql LIKE '%admintools.ddl_dcl_audit_log%'
        OR (db_name = 'admintools' AND query_sql LIKE '%sessions%')
        OR (db_name = 'admintools' AND query_sql LIKE '%ddl_dcl_audit_log%')
        OR query_sql LIKE '%admintools.ddl_dcl_audit_targets%'
        OR (db_name = 'admintools' AND query_sql LIKE '%ddl_dcl_audit_targets%')
    )
)
```

Two-tier защита: ловим `DELETE`/`UPDATE`, направленный на любую из
трёх наших таблиц. Условие проверяет и fully-qualified имя
(`admintools.sessions`), и короткое имя при активной БД `admintools`.

Зачем нужно: это **security tripwire**. Если злоумышленник получил
доступ и пытается удалить следы (свой логин из `sessions` или свой
DDL из `ddl_dcl_audit_log`) — попытка сама попадёт в аудит, и в
syslog-приёмнике мы это увидим.

Конечно, если злоумышленник полностью контролирует БД, он может
обойти и это. Но для типичных сценариев compromise это полезное
сито.

##### (4) Динамические targets:

```sql
v_dyn_targets    -- здесь подставляется накопленная строка с OR'ами
```

Это то, что мы собрали в шаге 4. Например, если в `ddl_dcl_audit_targets`
есть строка `(db_name='finance', object_name='invoices')`, то
`v_dyn_targets` будет содержать:

```sql
     OR (query_sql LIKE '%finance.invoices%' OR (db_name = 'finance' AND query_sql LIKE '%invoices%'))
```

И эта строка вклеивается в основной WHERE как ещё один OR.

Если targets нет (таблица пустая или все `is_active=0`) — `v_dyn_targets`
останется пустой строкой, и в WHERE ничего не добавится.

#### 4.6.6. ROW_COUNT() после EXECUTE

```sql
SET v_inserted = ROW_COUNT();
```

`ROW_COUNT()` возвращает число строк, **затронутых последним
запросом**. Для `INSERT IGNORE` это число реально вставленных строк
(дубликаты, отброшенные UNIQUE KEY, не считаются). Это и есть наша
основная метрика — «сколько новых событий добавили в аудит за этот
прогон».

`DEALLOCATE PREPARE` освобождает ресурсы prepared statement. Важно
делать всегда, иначе при частых вызовах может накопиться значительное
количество подготовленных stmt в сессии.

### 4.7. Шаг 6: обновление курсора

```sql
UPDATE admintools.audit_collector_state
   SET last_request_time = v_new_rt,
       updated_at        = NOW(6)
 WHERE id = 1;

SET p_inserted         = v_inserted;
SET p_new_request_time = v_new_rt;
```

После успешного INSERT обновляем курсор. **Важно: обновление курсора
происходит ПОСЛЕ INSERT-а, не до**. Если INSERT упадёт по какой-то
причине, курсор не сдвинется, и на следующем прогоне мы попробуем те
же строки ещё раз.

Это обеспечивает **at-least-once семантику**: каждая строка из
`GV$OB_SQL_AUDIT` будет хотя бы один раз обработана. А дедупликация
через `UNIQUE KEY uq_req` превращает это в **effectively-exactly-once**
для тех строк, которые ещё есть в circular buffer view.

Атомарность update — на уровне OB-сервера (одна строка, одна UPDATE).
В отличие от Go-версии, здесь не нужна explicit transaction `BEGIN`/`COMMIT`,
потому что вся процедура выполняется как одна логическая единица
работы клиента.

---

## 5. Идемпотентность и устойчивость

### 5.1. Что переживается при сбое

**Сценарий 1: процедура упала во время INSERT (network drop, timeout)**

- Курсор `last_request_time` НЕ обновился.
- В `ddl_dcl_audit_log` могли частично попасть строки (если падение
  было после части INSERT — но `INSERT IGNORE SELECT` обычно атомарен).
- Следующий вызов попробует ту же выборку. UNIQUE KEY `uq_req`
  отбросит уже вставленные строки, новые встанут.
- Курсор продвинется.

**Сценарий 2: INSERT прошёл, UPDATE курсора упал**

- В `ddl_dcl_audit_log` есть свежие строки.
- Курсор остался старым.
- Следующий вызов снова обработает то же окно. INSERT IGNORE
  отбросит дубликаты, `ROW_COUNT()` будет 0 или близко к 0.
- Курсор продвинется.

**Сценарий 3: процедура давно не запускалась, часть истории `GV$OB_SQL_AUDIT` вытеснилась**

- На следующем вызове `MAX(request_time) > last_rt` найдёт всё что
  ещё есть в буфере.
- Строки между `last_rt` и началом текущего буфера потеряны
  безвозвратно — но это ограничение `GV$OB_SQL_AUDIT`, не наше.
- Курсор продвинется на конец того, что было.

### 5.2. Что НЕ переживается

- **Удаление seed-строки из `audit_collector_state`**. Если её нет,
  `v_last_rt` упадёт в 0, и следующий прогон попытается обработать
  всю историю заново. Дубликаты отсеются UNIQUE KEY, так что
  данные не испортятся, но прогон будет долгим (несколько секунд
  на сотни тысяч строк). Лечится `UPDATE`, не `DELETE`.
- **Изменение схемы `GV$OB_SQL_AUDIT`** при апгрейде OB. Если в новой
  версии переименуется колонка или изменится тип — процедуру нужно
  пересобрать.
- **Параллельный вызов процедуры** в две сессии одновременно. Не
  гарантирована корректность: обе сессии прочитают тот же
  `last_rt`, обе попытаются вставить пересекающиеся диапазоны.
  UNIQUE KEY защитит от дубликатов в `ddl_dcl_audit_log`, но
  курсор будет обновляться непредсказуемо. **Рекомендация**: вызывать
  процедуру только из одного места одновременно (внешний планировщик
  с lock-ом, либо просто только из одного хоста OBAuditor).

---

## 6. Известные ограничения и тонкие места

### 6.1. LIKE-фильтры — производительность

Все условия `query_sql LIKE '%...%'` — это полный скан текста запроса.
На больших объёмах `GV$OB_SQL_AUDIT` (миллионы строк) это может быть
медленно. Но:

- основной отсев делает узкое окно `(last_rt, new_rt]` — обычно это
  секунды-минуты, тысячи строк;
- `stmt_type IN (...)` идёт первой проверкой — большинство строк
  отсеивается без LIKE;
- сами LIKE-условия проверяются только для оставшихся.

В худшем случае (огромный буфер, первый запуск) первый прогон может
быть медленным. Можно искусственно «прогреть» курсор:

```sql
UPDATE admintools.audit_collector_state
   SET last_request_time = UNIX_TIMESTAMP(NOW()) * 1000000
 WHERE id = 1;
```

### 6.2. `tenant_id` в targets не используется

В таблице `ddl_dcl_audit_targets` есть поле `tenant_id`, но процедура
сейчас его не учитывает при построении WHERE — мы только читаем
`db_name` и `object_name`. Это потенциальное место расширения: для
multi-tenant сценариев можно добавить условие `tenant_id = ...`.

Курсор всё равно читает поле через `FETCH ... INTO v_tenant_id`, чтобы
по добавлении логики не пришлось менять структуру.

### 6.3. Регулярка REGEXP_REPLACE

```sql
REGEXP_REPLACE(query_sql, '^[[:space:]]*/[*].*?[*]/[[:space:]]*', '')
```

Срабатывает только на **ведущий** комментарий-хинт. Если в запросе
есть `/* ... */` где-то в середине или несколько подряд — обрежется
только первый. Это сознательное решение: основной use case в OB —
именно префиксные хинты, остальные комментарии редки и для текста
запроса не критичны.

### 6.4. Размер `query_sql` в `GV$OB_SQL_AUDIT`

OceanBase усекает `query_sql` в audit-view (обычно до 4КБ или 8КБ).
Длинные запросы (например, большие `INSERT ... VALUES (...), (...)...`)
будут видны частично. Это ограничение OB, мы с этим ничего не делаем.

### 6.5. Циклические запросы и SELF-аудит

Сама процедура состоит из SELECT/UPDATE/INSERT, которые попадают в
`GV$OB_SQL_AUDIT`. Мы исключили их по тексту (`%sp_collect_ddl_dcl_audit%`,
`%INSERT IGNORE INTO admintools.ddl_dcl_audit_log%`), но если кто-то
вызовет процедуру через wrapper или CALL без упоминания имени —
теоретически может прорваться.

Также `EXECUTE stmt USING @last_rt, @new_rt` в OB может попасть в
audit с подставленными значениями — это нормально, такой запрос
отсеется по `INSERT IGNORE INTO admintools.ddl_dcl_audit_log`.

---

## 7. Примеры использования

### 7.1. Установка

```bash
obclient -h 192.168.55.205 -P 2881 -u root@sys -p admintools \
    < sp_collect_ddl_dcl_audit.sql

# Выдать права обычному пользователю
GRANT CREATE ROUTINE, EXECUTE ON admintools.* TO 'ocp'@'%';
```

### 7.2. Боевой запуск

```sql
CALL admintools.sp_collect_ddl_dcl_audit(@inserted, @new_rt);
SELECT @inserted, FROM_UNIXTIME(@new_rt/1000000) AS new_cursor_time;
```

### 7.3. Просмотр текущего состояния

```sql
-- Что сейчас в state
SELECT id, collector_id, last_request_time,
       FROM_UNIXTIME(last_request_time/1000000) AS last_event_time,
       updated_at,
       TIMESTAMPDIFF(SECOND, updated_at, NOW(6)) AS age_seconds
FROM admintools.audit_collector_state
WHERE id = 1;

-- Сколько событий собрали за последний час
SELECT COUNT(*), MIN(request_ts), MAX(request_ts)
FROM admintools.ddl_dcl_audit_log
WHERE collected_at > NOW() - INTERVAL 1 HOUR;

-- Топ-10 пользователей по числу DDL/DCL за день
SELECT user_name, COUNT(*) AS n
FROM admintools.ddl_dcl_audit_log
WHERE request_ts > NOW() - INTERVAL 1 DAY
GROUP BY user_name
ORDER BY n DESC LIMIT 10;

-- Активные targets
SELECT * FROM admintools.ddl_dcl_audit_targets WHERE is_active = 1;
```

### 7.4. Управление targets

```sql
-- Добавить
INSERT INTO admintools.ddl_dcl_audit_targets
    (tenant_id, db_name, object_name, description, is_active)
VALUES (NULL, 'finance', 'invoices', 'SOX-compliance', 1);

-- Отключить временно
UPDATE admintools.ddl_dcl_audit_targets SET is_active = 0 WHERE id = 3;

-- Включить обратно
UPDATE admintools.ddl_dcl_audit_targets SET is_active = 1 WHERE id = 3;

-- Удалить совсем
DELETE FROM admintools.ddl_dcl_audit_targets WHERE id = 5;
```

### 7.5. Сброс / откат курсора

```sql
-- Прогнать всё, что есть в текущем буфере GV$OB_SQL_AUDIT
UPDATE admintools.audit_collector_state SET last_request_time = 0 WHERE id = 1;

-- Перемотать на 1 час назад
UPDATE admintools.audit_collector_state
SET last_request_time = (UNIX_TIMESTAMP(NOW()) - 3600) * 1000000
WHERE id = 1;
```

### 7.6. Превью-режим (отладка)

В файле есть вторая процедура `sp_collect_ddl_dcl_audit_preview()` —
не делает INSERT, просто показывает что бы выполнилось:

```sql
CALL admintools.sp_collect_ddl_dcl_audit_preview();
-- вернёт result set из трёх колонок:
--   last_request_time, new_request_time, generated_sql
```

Полезно когда нужно проверить, что динамические targets правильно
вклеились в WHERE, или посчитать предполагаемый объём через
`SELECT COUNT(*) FROM (<generated_sql без INSERT>) sub`.

---

## 8. Что ещё стоит знать про DDL/DCL аудит в OB

### 8.1. `GV$OB_SQL_AUDIT` vs `V$OB_SQL_AUDIT`

- `V$...` — данные **только текущего узла**.
- `GV$...` — данные **всех узлов кластера** (объединённое представление).

Мы используем `GV$`, потому что DDL мог быть выполнен на любом узле, а
нам нужна полная картина. Чуть дороже по производительности, но
правильнее по семантике.

### 8.2. Поведение при разных stmt_type

OceanBase для большинства DDL/DCL выставляет понятный `stmt_type`,
но есть нюансы:

- `CREATE_USER` — может выставляться, может нет (зависит от версии).
  Мы дополнительно ловим по LIKE.
- `SET_PASSWORD` — старая форма (`SET PASSWORD FOR ...`); новая
  `ALTER USER ... IDENTIFIED BY ...` идёт как `ALTER_USER`.
- `RENAME_USER` — отдельный тип.
- Хранимые процедуры (`CALL my_proc()`) идут как `CALL`. Если внутри
  процедуры есть DDL, он в audit отдельно НЕ всплывёт — увидим только
  внешний CALL. Это известное ограничение OB.

### 8.3. Циклы и рекурсивные запросы

Если процедура `sp_collect_ddl_dcl_audit` сама попадёт в `GV$OB_SQL_AUDIT`
(а она там точно будет), наши NOT LIKE фильтры её отсеют. Это важно,
потому что без отсева получится:

1. Прогон процедуры
2. Её INSERT IGNORE попадает в GV$OB_SQL_AUDIT
3. Следующий прогон ловит свой собственный INSERT
4. Пишет в `ddl_dcl_audit_log` запись «INSERT INTO ddl_dcl_audit_log...»
5. Goto 2 — рекурсивное наращивание

С фильтрами этой проблемы нет.

---

## 9. Что НЕ входит в процедуру (важно)

Процедура **не** делает:

- Не пересылает события в rsyslog. Это отдельная работа `RsyslogSender`-а
  в Go-приложении, читающего `ddl_dcl_audit_log` и продвигающего курсор
  `rsyslog_cursor['ddl']`. Процедура только наполняет таблицу.
- Не удаляет старые записи (cleanup). Это работа `CleanupDao` в Go-части,
  опять же читающей `ddl_dcl_audit_log` и удаляющей чанками по достижении
  лимита.
- Не парсит логи OceanBase (observer.log / obproxy.log) — это совсем
  другой источник данных, обрабатывается в `LogFileProcessor` Go-приложения
  и наполняет таблицу `sessions`, не `ddl_dcl_audit_log`.
- Не управляет правами/пользователями — только аудит того, что сделано.

Процедура — это **один из источников** для `ddl_dcl_audit_log`. На данный
момент единственный, но в будущем рядом могут появиться другие коллекторы
(например, из `oceanbase.gv$audit_actions` для FGA-аудита, если такой
включён).
