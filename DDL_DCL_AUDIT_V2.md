# DDL/DCL аудит OceanBase — v2

Документ описывает вторую версию процесса аудита DDL/DCL операций в OceanBase,
реализованную в виде набора хранимых процедур в схеме `admintools`. v2 решает
ключевую проблему первой версии — некорректную обработку длительных запросов
и риск рассинхронизации курсора при рестартах observer-а.

## Содержание

1. [Что было не так в v1](#что-было-не-так-в-v1)
2. [Ключевая идея v2](#ключевая-идея-v2)
3. [Эмпирические подтверждения](#эмпирические-подтверждения)
4. [Структура таблиц](#структура-таблиц)
5. [Процедуры](#процедуры)
6. [Алгоритм работы](#алгоритм-работы)
7. [Производительность и идеи оптимизации](#производительность-и-идеи-оптимизации)
8. [Установка и совместимость](#установка-и-совместимость)
9. [Примеры использования](#примеры-использования)
10. [Известные ограничения](#известные-ограничения)

---

## Что было не так в v1

v1 использовала один глобальный курсор `last_request_time` для всего кластера
и продвигала его по `MAX(request_time)` — то есть по моменту **старта**
запроса. Это создавало две проблемы.

### Проблема 1 — long-running запросы теряются

`request_time` присваивается в момент **начала** выполнения. Но запись
появляется в `GV$OB_SQL_AUDIT` только в момент **завершения**. Между ними
могут пройти секунды или минуты — для тяжёлых `ALTER TABLE` это совершенно
реалистично.

Сценарий:
- Запрос A стартует в `T0`, выполняется 120 секунд, завершается в `T0+120s`
- Запрос B стартует в `T0+30s`, выполняется 0.001 секунды, завершается мгновенно

Если процедура запустится в `T0+35s`, то в `GV$OB_SQL_AUDIT` уже будет
запись B (с `request_time = T0+30s`), но **не** будет записи A (он ещё
выполняется). `MAX(request_time)` вернёт `T0+30s`, курсор продвинется туда.

Когда A завершится в `T0+120s`, его `request_time = T0` < нового курсора
`T0+30s` — и следующий прогон процедуры **не увидит** этот запрос. DDL
теряется навсегда.

### Проблема 2 — рестарт observer-а ломает per-server курсор

`request_id` сбрасывается при рестарте observer-а. Если бы мы привязались
к `request_id` вместо `request_time` — после рестарта новые запросы получат
малые id, которые меньше последнего обработанного, и тоже потеряются.

### Проблема 3 — один курсор на всё

С одним глобальным курсором мы не можем независимо обрабатывать разные
тенанты или разные observer-узлы. Это станет критичным в multi-zone
production кластере.

---

## Ключевая идея v2

### Триггер — `request_time + elapsed_time`

Курсор продвигается не по моменту **старта** запроса, а по моменту его
**попадания в audit-буфер** — то есть по `request_time + elapsed_time`
(момент завершения, "end time" / "end_calc").

Это значение монотонно возрастает в порядке появления записей в буфере:
- in-flight запрос ещё не имеет записи в буфере — `MAX(end)` его не видит
- когда запрос завершится, его `end` будет больше всех уже видимых `end`
- на следующем прогоне процедура захватит его в окне `end > last_end`

`safety_lag` не нужен. Wraparound `request_id` не страшен —
`request_time + elapsed_time` это wall-clock микросекунды, монотонные
независимо от рестартов observer-а.

### Per-server-tenant курсор

Вместо одной строки состояния — таблица с курсором для каждой комбинации
`(svr_ip, svr_port, tenant_id)`. Это даёт:

- независимое продвижение для каждого тенанта на каждом observer-е
- корректная работа в multi-zone кластере (каждый observer ведёт свой
  audit-буфер, и нам нужен per-observer курсор)
- использование PRIMARY KEY `(svr_ip, svr_port, tenant_id, request_id)`
  internal-таблицы `__all_virtual_sql_audit` как RANGE-scan префикса

### Sync списка юнитов и ghost-handling

Перед каждым прогоном:

1. **Sync** — `INSERT IGNORE` из `DBA_OB_UNITS` добавляет новые
   server-tenant комбинации в checkpoint со стартовым `last_end_time = 0`.
2. **Ghost snapshot** — фиксируем какие записи в checkpoint больше не
   соответствуют живым юнитам (тенант удалили, сервер вывели).
3. **Цикл** — обрабатываем ВСЕ строки checkpoint одинаково, и живых, и
   ghost. Ghost получают свой последний прогон, забирают остатки.
4. **Cleanup** — удаляем строки которые были ghost на момент snapshot.

---

## Эмпирические подтверждения

### Тест 1 — структура источника

Через `SHOW CREATE TABLE oceanbase.__all_virtual_sql_audit`:

```
PRIMARY KEY (svr_ip, svr_port, tenant_id, request_id)
```

`EXPLAIN` подтверждает: запрос с предикатом по тройке
`(svr_ip=, svr_port=, tenant_id=)` использует `TABLE RANGE SCAN` по
PK с узким диапазоном `(205,2882,1002,MIN ; 205,2882,1002,MAX)`,
а не FULL SCAN.

### Тест 2 — long-running запрос

Запустили `SELECT SLEEP(120)` в сессии A, через 6 секунд короткий `SELECT 1`
в сессии B. Через 60 секунд (long ещё крутится) проверили audit — там был
только short. После завершения long проверили снова:

| query | request_time (start) | elapsed | end_calc (rt+el) |
|---|---|---|---|
| short#2 | 16:32:42 | 0s | 16:32:42 |
| **long** | **15:58:51** | **120s** | **16:00:51** |

`request_id` у long оказался **больше** чем у всех short — несмотря на
ранний `request_time`. Это подтверждает: запись попадает в буфер в момент
**завершения**, и `request_time + elapsed_time` — корректный порядок
появления в буфере.

### Тест 3 — последовательные запросы

5 коротких `SELECT 1` подряд показали строго монотонные `request_time` и
`end_calc` в порядке выполнения. Базовая линия чистая.

---

## Структура таблиц

### `admintools.ddl_dcl_audit_checkpoint`

Per-server-tenant курсор.

```sql
CREATE TABLE admintools.ddl_dcl_audit_checkpoint (
    svr_ip         VARCHAR(46) NOT NULL,
    svr_port       BIGINT      NOT NULL,
    tenant_id      BIGINT      NOT NULL,
    last_end_time  BIGINT      NOT NULL DEFAULT 0,
    updated_at     DATETIME(6)     NULL,
    PRIMARY KEY (svr_ip, svr_port, tenant_id)
);
```

| Поле | Смысл |
|---|---|
| `svr_ip`, `svr_port` | OBServer-узел (RPC порт обычно 2882) |
| `tenant_id` | ID тенанта |
| `last_end_time` | `request_time + elapsed_time` последней обработанной записи, мкс от epoch |
| `updated_at` | Wall-clock последнего прогона (heartbeat) |

### `admintools.ddl_dcl_audit_ghost_buffer`

Технический буфер для snapshot ghost-юнитов между фазами процедуры.
Заполняется на Шаге 2, читается на Шаге 5 (определение статуса юнита),
очищается на Шаге 6.

```sql
CREATE TABLE admintools.ddl_dcl_audit_ghost_buffer (
    svr_ip    VARCHAR(46) NOT NULL,
    svr_port  BIGINT      NOT NULL,
    tenant_id BIGINT      NOT NULL,
    PRIMARY KEY (svr_ip, svr_port, tenant_id)
);
```

### `admintools.ddl_dcl_audit_last_run_stats`

Per-unit детализация последнего прогона. Обнуляется в начале каждого
`CALL`. Полезно для отладки и мониторинга работы процедуры.

```sql
CREATE TABLE admintools.ddl_dcl_audit_last_run_stats (
    seq             BIGINT      NOT NULL AUTO_INCREMENT,
    svr_ip          VARCHAR(46) NOT NULL,
    svr_port        BIGINT      NOT NULL,
    tenant_id       BIGINT      NOT NULL,
    status          VARCHAR(16) NOT NULL,  -- 'LIVE' / 'GHOST_PURGED'
    last_end_before BIGINT      NOT NULL,
    last_end_after  BIGINT          NULL,
    rows_scanned    BIGINT      NOT NULL DEFAULT 0,
    rows_inserted   BIGINT      NOT NULL DEFAULT 0,
    duration_us     BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (seq),
    KEY idx_unit (svr_ip, svr_port, tenant_id)
);
```

### Таблицы из v1 которые остаются

- `admintools.ddl_dcl_audit_log` — основная таблица аудита.
  `UNIQUE KEY (svr_ip, request_id)` обеспечивает дедупликацию между v1 и v2.
- `admintools.ddl_dcl_audit_targets` — динамические правила аудита.
- `admintools.audit_collector_state` — курсор v1, **не трогается**.

Обе версии могут работать параллельно — UNIQUE KEY предотвращает дубликаты.

---

## Процедуры

### `sp_collect_ddl_dcl_audit_v2(OUT ...)`

Основная процедура с 7 OUT-параметрами. Для Go-приложения, OCP, любого
программного клиента.

```sql
CALL admintools.sp_collect_ddl_dcl_audit_v2(
    @inserted, @rows_scanned, @units_total, @units_new,
    @units_with_data, @units_ghost, @duration_ms);
```

| Параметр | Описание |
|---|---|
| `p_inserted` | Сколько строк вставлено в `ddl_dcl_audit_log` |
| `p_rows_scanned` | Сколько строк прошло окно `end > last_end` до DDL/DCL фильтров |
| `p_units_total` | Юнитов в checkpoint после sync |
| `p_units_new` | Сколько новых юнитов добавлено на этом прогоне |
| `p_units_with_data` | У скольких юнитов было что-то новое (`new_end ≠ NULL`) |
| `p_units_ghost` | Сколько ghost-юнитов удалено |
| `p_duration_ms` | Длительность работы процедуры в миллисекундах |

### `sp_collect_ddl_dcl_audit_v2_run()`

Обёртка без параметров для удобного ручного вызова. Внутри вызывает
основную процедуру и автоматически выводит два result set'а:

```sql
CALL admintools.sp_collect_ddl_dcl_audit_v2_run();
```

Result set 1 — агрегаты прогона (одна строка):
```
+----------+--------------+-------------+-----------+-----------------+-------------+-------------+
| inserted | rows_scanned | units_total | units_new | units_with_data | units_ghost | duration_ms |
+----------+--------------+-------------+-----------+-----------------+-------------+-------------+
|       42 |         1234 |           3 |         0 |               2 |           0 |         156 |
+----------+--------------+-------------+-----------+-----------------+-------------+-------------+
```

Result set 2 — per-unit детализация (из `ddl_dcl_audit_last_run_stats`):
```
+-----+----------------+----------+-----------+--------+---------------------+---------------------+--------------+---------------+-------------+
| seq | svr_ip         | svr_port | tenant_id | status | last_end_before_ts  | last_end_after_ts   | rows_scanned | rows_inserted | duration_ms |
+-----+----------------+----------+-----------+--------+---------------------+---------------------+--------------+---------------+-------------+
|   1 | 192.168.55.205 |     2882 |         1 | LIVE   | 2026-05-27 18:00:00 | 2026-05-27 18:01:30 |         1100 |            38 |       142.5 |
|   2 | 192.168.55.205 |     2882 |      1002 | LIVE   | 2026-05-27 18:00:00 | 2026-05-27 18:01:30 |          134 |             4 |        13.2 |
|   3 | 192.168.55.205 |     2882 |      1004 | LIVE   | 2026-05-27 18:00:00 | NULL                |            0 |             0 |         0.5 |
+-----+----------------+----------+-----------+--------+---------------------+---------------------+--------------+---------------+-------------+
```

### `sp_collect_ddl_dcl_audit_v2_preview()`

Preview без побочных эффектов. Не выполняет INSERT, не обновляет
checkpoint. Возвращает список юнитов которые **будут** обработаны
(с пометкой LIVE/GHOST/NEW) и схематичный текст INSERT.

Используется для отладки динамических targets и проверки списка юнитов
перед боевым запуском.

---

## Алгоритм работы

### Шаг 0 — обнуление статистики прошлого прогона

```sql
DELETE FROM admintools.ddl_dcl_audit_last_run_stats WHERE 1=1;
```

### Шаг 1 — sync списка юнитов

Считаем количество строк ДО, потом `INSERT IGNORE` из `DBA_OB_UNITS`,
потом снова COUNT. Разница даёт `units_new`.

```sql
INSERT IGNORE INTO admintools.ddl_dcl_audit_checkpoint
    (svr_ip, svr_port, tenant_id, last_end_time)
SELECT svr_ip, svr_port, tenant_id, 0
  FROM oceanbase.DBA_OB_UNITS
 WHERE status = 'ACTIVE';
```

Новые юниты появляются автоматически. Для существующих INSERT IGNORE
ничего не делает из-за PK conflict.

### Шаг 2 — snapshot ghost-юнитов

Ghost = есть в checkpoint, нет в `DBA_OB_UNITS`. Запоминаем их в
`ddl_dcl_audit_ghost_buffer`, чтобы изменения `DBA_OB_UNITS` во время
выполнения процедуры не влияли на cleanup на Шаге 6.

### Шаг 3 — сборка динамических targets

Один курсор по `ddl_dcl_audit_targets WHERE is_active=1` собирает
строку `v_dyn_targets` с дополнительными `OR query_sql LIKE ...`.
Делается **один раз** перед циклом по юнитам, потом вклеивается в
prepared statement.

### Шаг 4 — подготовка INSERT prepared statement

`PREPARE stmt_ins FROM @ins_sql` где `@ins_sql` содержит INSERT с
плейсхолдерами `?` для (`svr_ip`, `svr_port`, `tenant_id`, `last_end`,
`new_end`). Динамические targets вклеены в текст один раз. PREPARE
выполняется **один раз** перед циклом — EXECUTE будет в цикле с
разными параметрами.

### Шаг 5 — цикл по checkpoint

Курсор по всем строкам checkpoint (живые + ghost). Для каждой:

1. Определить `status` (`LIVE` / `GHOST_PURGED`) проверкой в ghost_buffer.
2. Статический SELECT с локальными переменными процедуры в WHERE:
   ```sql
   SELECT COUNT(*), MAX(request_time + elapsed_time)
     INTO v_rows_scanned_unit, v_new_end
     FROM oceanbase.GV$OB_SQL_AUDIT
    WHERE svr_ip=v_svr_ip AND svr_port=v_svr_port
      AND tenant_id=v_tenant_id
      AND is_inner_sql = 0
      AND request_time + elapsed_time > v_last_end;
   ```
   Это RANGE SCAN по PK. Возвращает и количество строк (для метрики
   `rows_scanned`), и новую границу окна.
3. Если `v_new_end IS NOT NULL`:
   - EXECUTE INSERT с окном `(v_last_end ; v_new_end]`
   - UPDATE checkpoint
4. Иначе — только heartbeat в checkpoint.
5. INSERT строки в `ddl_dcl_audit_last_run_stats` с метриками юнита.

### Шаг 6 — cleanup ghost-юнитов

```sql
DELETE c FROM ddl_dcl_audit_checkpoint c
  JOIN ddl_dcl_audit_ghost_buffer g
    ON g.svr_ip=c.svr_ip AND g.svr_port=c.svr_port AND g.tenant_id=c.tenant_id;
DELETE FROM ddl_dcl_audit_ghost_buffer WHERE 1=1;
```

Удаляем строки которые были ghost на момент Шага 2. На Шаге 5 они уже
получили свой последний прогон и забрали остатки из буфера.

### Корректность при сбоях

- **EXECUTE INSERT упал** — курсор НЕ обновился. Следующий прогон
  повторит то же окно. UNIQUE KEY `(svr_ip, request_id)` отбросит уже
  вставленные строки.
- **UPDATE checkpoint упал** — INSERT прошёл, но курсор старый.
  Следующий прогон INSERT IGNORE-нёт всё, ROW_COUNT будет 0 или близко,
  курсор продвинется.
- **Процедура давно не запускалась** — часть истории в `GV$OB_SQL_AUDIT`
  вытеснилась. Это ограничение source-системы, не наше.

### Параллельные запуски

Параллельный запуск процедуры в двух сессиях одновременно НЕ
рекомендуется. Обе прочитают тот же `last_end_time`, обе сделают
пересекающиеся INSERT. UNIQUE KEY защитит от дубликатов в
`ddl_dcl_audit_log`, но курсор будет обновляться непредсказуемо.
Регулярный запуск — только из одного места (cron + lock-файл, или
один экземпляр Go-приложения).

---

## Производительность и идеи оптимизации

На текущей версии типичная производительность (по результатам нагрузочного
теста с 1000 DDL):

- 1000 DDL за два прогона по 500 — отловлены все
- 700 мс на прогон при `rows_scanned ≈ 2900` (selectivity ~19%)
- Основное время — INSERT с LIKE-фильтрами и `REGEXP_REPLACE`

Сейчас не оптимизируется — производительность приемлемая. Но когда
понадобится — есть несколько направлений.

### Где время тратится

Для каждого юнита процедура делает **два прохода** по audit-буферу:
1. `COUNT + MAX` для определения окна (RANGE SCAN, дешёво — несколько мс)
2. `INSERT IGNORE с SELECT` — основное время

В INSERT:
- 7 `NOT LIKE '%...%'` для исключения служебных запросов
- 4 `LIKE '%CREATE USER%'`-подобных для user-management через текст
- `REGEXP_REPLACE` для отрезания leading hint-комментария
- Все LIKE и regex применяются к **каждой** строке прошедшей окно по
  `request_time + elapsed_time`, включая SELECT/INSERT/UPDATE которые
  отвалятся на DDL/DCL фильтре

### Идея 1 — pre-filter по `stmt_type` перед LIKE

70-80% строк в обычном буфере имеют `stmt_type IN ('SELECT', 'INSERT',
'UPDATE')`. Если перенести условие на эти типы в начало WHERE (через
переформулировку фильтра), оптимизатор сможет отсеять их **до** дорогих
LIKE-проверок.

Оценка ускорения: 2-3x.

Риск: нужно перепроверить корректность всех веток фильтра. Особенно
ветка "DELETE/UPDATE таблиц аудита" — она должна продолжать ловить
попытки подделки.

### Идея 2 — убрать `REGEXP_REPLACE` из INSERT

`REGEXP_REPLACE(query_sql, '^[[:space:]]*/[*].*?[*]/[[:space:]]*', '')`
выполняется на каждой матчующейся строке. Регекс в OB — дорогая операция.

**Вариант 2а:** Перенести очистку leading-комментария на сторону Go-
приёмника. Аудит-таблица хранит оригинальный текст, потребитель
(`RsyslogSender`) чистит при отправке в syslog. Это снимает нагрузку
с горячего пути процедуры.

**Вариант 2б:** Заменить regex на простой `CASE WHEN query_sql LIKE '/*%'
THEN SUBSTR(query_sql, INSTR(query_sql, '*/') + 2) ELSE query_sql END`.
Работает только для prefix-хинта (как сейчас), но в разы дешевле.

Оценка ускорения: 1.5-2x на DDL-heavy прогонах.

### Идея 3 — объединить per-unit INSERT в один UNION ALL

В текущей реализации EXECUTE stmt_ins вызывается N раз, по разу на каждый
юнит. На single-zone кластере (3 юнита) — несущественно. На production
multi-zone (9-12 юнитов = 3 зоны × 3-4 тенанта) — overhead PREPARE/EXECUTE
становится заметным.

Идея: построить один большой INSERT с UNION ALL подзапросов, по одному
на каждый юнит, и выполнить одним EXECUTE.

Оценка ускорения: 50-100 мс на 9 юнитов.

Сложность: динамическое построение UNION ALL из cursor checkpoint — это
дополнительный CONCAT в коде процедуры.

### Идея 4 — прямое подключение к observer вместо OBProxy

`OBProxy:2883` → `observer:2881` добавляет 1-2 мс на каждый round-trip.
Процедура делает порядка 30 RT за прогон (CALL + внутренние COUNT/MAX/
INSERT/UPDATE по юнитам). Прямое подключение даст экономию 15-30 мс.

Тривиально, бесплатно. Стоит делать для cron-задачи которая дёргает
процедуру.

### Идея 5 — DELETE служебных по id range вместо NOT LIKE в INSERT

Альтернативный архитектурный сдвиг. Сейчас 7 `NOT LIKE '%admintools.xxx%'`
отсевают нашу собственную служебку **внутри** INSERT.

Идея: убрать эти NOT LIKE, INSERT возьмёт чуть больше, потом сделать
`DELETE FROM ddl_dcl_audit_log WHERE id > (last_max_id_before_run) AND
query_sql LIKE '%сами_себя%'` по узкому диапазону id.

7 LIKE на 2900 строках = тысячи операций сравнения.
DELETE по id range + 7 LIKE на ~10 свежих строках = десятки операций.

Оценка ускорения: 50-100 мс при большом `rows_scanned`.

Сложность: нужно запоминать `last_max_id` до и после INSERT, плюс
дополнительный DELETE — это +1 round trip.

### Что НЕ улучшит производительность

- ❌ Изменить RANGE SCAN — он уже оптимален по PK
- ❌ Убрать COUNT+MAX — без него потеряем строки, если только INSERT
  не вернёт нам `new_end` (что не предусмотрено в OB)
- ❌ Заменить `GV$OB_SQL_AUDIT` на `V$OB_SQL_AUDIT` — даст ту же
  производительность на single-observer, но сломает multi-observer

### Приоритеты применения

Если возникнет реальная проблема производительности (например,
накладывающиеся запуски при cron каждые 30 секунд), применять в порядке:

1. **Идея 1** — pre-filter по stmt_type (наибольшее улучшение, средняя
   сложность)
2. **Идея 2а** — снять `REGEXP_REPLACE` на сторону Go (умеренное
   улучшение, низкая сложность, но требует синхронного изменения
   потребителя)
3. **Идея 4** — прямое подключение (мелочь, но бесплатная)
4. **Идея 3** — UNION ALL для multi-zone (актуально с 6+ юнитов)
5. **Идея 5** — DELETE по id range (умеренное улучшение, средняя сложность)

---

## Установка и совместимость

### Требования

- OceanBase CE 4.3.5+ (тестировалось на 4.3.5.5; виртуальные таблицы
  и `DBA_OB_UNITS` имеют ту же структуру в 4.4.x)
- Существующие таблицы из v1: `ddl_dcl_audit_log`, `ddl_dcl_audit_targets`
- У вызывающего пользователя: `SELECT` на `oceanbase.GV$OB_SQL_AUDIT`,
  `SELECT` на `oceanbase.DBA_OB_UNITS`, полные права на `admintools.*`

### Установка

⚠️ **Важно про collation.** Скрипт содержит много строковых литералов
внутри тела процедур (динамический SQL). Если установка идёт через
mysql 8.x клиент с default `collation_connection = utf8mb4_0900_ai_ci`,
литералы запекаются в процедуру с этой collation, и при выполнении
JOIN с системными view (которые в `utf8mb4_general_ci`) возникает
`ERROR 1267: Illegal mix of collations`.

Решение — устанавливать через `obclient` (он использует серверный
default, корректный) либо через mysql с явным флагом:

```bash
# Вариант 1: obclient
obclient -h <host> -P 2881 -u root@sys -p admintools < ddldclaudit_v2.sql

# Вариант 2: mysql с правильной collation
mysql -h <host> -P 2883 -u root@sys -p \
      --default-character-set=utf8mb4 \
      --init-command="SET NAMES utf8mb4 COLLATE utf8mb4_general_ci" \
      -A -D admintools < ddldclaudit_v2.sql
```

Проверить collation процедуры после установки:

```sql
SHOW CREATE PROCEDURE admintools.sp_collect_ddl_dcl_audit_v2\G
-- ожидаем: collation_connection: utf8mb4_general_ci
```

### Совместимость с v1

v1 процедура и состояние не трогаются. Обе версии могут работать
параллельно на одной БД — UNIQUE KEY `(svr_ip, request_id)` в
`ddl_dcl_audit_log` предотвращает дубликаты.

Можно использовать как стратегию миграции:
1. Установить v2, оставив v1 работать
2. Пустить v2 в shadow-режиме, сравнить с v1 за неделю
3. Когда подтвердится корректность — отключить v1
4. Удалить v1 артефакты (`sp_collect_ddl_dcl_audit*`,
   `audit_collector_state`)

---

## Примеры использования

### Удобный ручной запуск

```sql
CALL admintools.sp_collect_ddl_dcl_audit_v2_run();
-- два result set'а: агрегаты + per-unit детализация
```

### Программный запуск (для Go)

```sql
CALL admintools.sp_collect_ddl_dcl_audit_v2(
    @inserted, @rows_scanned, @units_total, @units_new,
    @units_with_data, @units_ghost, @duration_ms);
SELECT @inserted, @rows_scanned, @units_total, @units_new,
       @units_with_data, @units_ghost, @duration_ms;
SELECT * FROM admintools.ddl_dcl_audit_last_run_stats;
```

### Preview без побочных эффектов

```sql
CALL admintools.sp_collect_ddl_dcl_audit_v2_preview();
```

### Текущее состояние всех курсоров

```sql
SELECT svr_ip, svr_port, tenant_id,
       last_end_time,
       usec_to_time(last_end_time) AS last_end_ts,
       updated_at,
       TIMESTAMPDIFF(SECOND, updated_at, NOW(6)) AS age_seconds
  FROM admintools.ddl_dcl_audit_checkpoint
 ORDER BY svr_ip, svr_port, tenant_id;
```

### Диагностика рассинхронизации checkpoint vs DBA_OB_UNITS

```sql
SELECT 'in_checkpoint_but_not_in_units' AS issue,
       c.svr_ip, c.svr_port, c.tenant_id
  FROM admintools.ddl_dcl_audit_checkpoint c
  LEFT JOIN oceanbase.DBA_OB_UNITS u
    ON u.svr_ip=c.svr_ip AND u.svr_port=c.svr_port
   AND u.tenant_id=c.tenant_id AND u.status='ACTIVE'
 WHERE u.tenant_id IS NULL
UNION ALL
SELECT 'in_units_but_not_in_checkpoint' AS issue,
       u.svr_ip, u.svr_port, u.tenant_id
  FROM oceanbase.DBA_OB_UNITS u
  LEFT JOIN admintools.ddl_dcl_audit_checkpoint c
    ON c.svr_ip=u.svr_ip AND c.svr_port=u.svr_port
   AND c.tenant_id=u.tenant_id
 WHERE u.status='ACTIVE' AND c.tenant_id IS NULL;
```

### Сброс курсоров для повторной обработки буфера

```sql
UPDATE admintools.ddl_dcl_audit_checkpoint SET last_end_time = 0;
```

UNIQUE KEY `(svr_ip, request_id)` в `ddl_dcl_audit_log` отфильтрует
дубликаты при повторном INSERT.

### Полная чистка состояния v2

```sql
DELETE FROM admintools.ddl_dcl_audit_checkpoint     WHERE 1=1;
DELETE FROM admintools.ddl_dcl_audit_ghost_buffer   WHERE 1=1;
DELETE FROM admintools.ddl_dcl_audit_last_run_stats WHERE 1=1;
-- на следующем CALL список юнитов соберётся заново
```

### Регулярный запуск через cron

```bash
# /etc/cron.d/ddl-dcl-audit-v2
* * * * * costa obclient -h 192.168.55.205 -P 2881 \
    -u ocp@sys -pqaz123 admintools \
    -e "CALL admintools.sp_collect_ddl_dcl_audit_v2_run();" \
    >> /var/log/ddl-dcl-audit.log 2>&1
```

---

## Известные ограничения

### Из источника данных (`GV$OB_SQL_AUDIT`)

- **In-memory circular buffer.** При длительном простое процедуры или
  рестарте observer-а часть истории теряется безвозвратно.
- **`query_sql` усекается** до 4-8 KB. Длинные `INSERT ... VALUES (...)`
  видны частично.
- **`CREATE USER` и `ALTER USER` в CE 4.4.1 не записываются** в
  `GV$OB_SQL_AUDIT` (известное ограничение CE). На 4.3.5.5 пишутся.
  Если работаем на 4.4.1 — эти DDL фиксируем через LIKE-фильтр (что
  уже сделано).
- **`audit_log_*` (full audit) — EE only.** В CE есть только
  `GV$OB_SQL_AUDIT` и log-файлы.

### Из логики процедуры

- **Параллельный запуск не поддерживается.** Защита через внешний
  lock (cron-файл, distributed lock в Go-приложении).
- **`tenant_id` в `ddl_dcl_audit_targets` не используется.**
  Зарезервировано на будущее.
- **`REGEXP_REPLACE` только для leading hint.** Не очищает inline
  и trailing комментарии.

### Из collation-механики OB

- Установка должна происходить в сессии с
  `collation_connection = utf8mb4_general_ci`. Иначе процедура
  запечётся с несовместимой collation и упадёт при выполнении
  с `ERROR 1267`.

---

## История изменений

| Версия | Дата | Изменения |
|---|---|---|
| v1 | 2025-... | Один глобальный курсор `last_request_time`, продвижение по моменту старта. Известная проблема с long-running запросами. |
| v2 | 2026-05-27 | Per-server-tenant курсор по `request_time + elapsed_time`. Sync списка юнитов из `DBA_OB_UNITS`. Ghost-handling. Расширенная диагностика. Обёртка `*_run()` для удобного ручного вызова. |
