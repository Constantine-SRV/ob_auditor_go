-- =============================================================================
--  admintools.sp_collect_ddl_dcl_audit_v2
--
--  Новая версия с per-server-tenant курсором по (request_time + elapsed_time).
--
--  Ключевые отличия от v1:
--    - чекпойнт хранится per (svr_ip, svr_port, tenant_id), не один на всё
--    - курсор продвигается по MAX(request_time + elapsed_time) — то есть по
--      моменту ПОЯВЛЕНИЯ записи в GV$OB_SQL_AUDIT, не по моменту старта
--      запроса. Это даёт корректную обработку длительных запросов: пока
--      запрос не завершился, его в audit-буфере НЕТ; когда завершится —
--      запишется с end_calc > всех предыдущих end_calc, попадёт в окно.
--    - safety_lag не нужен: in-flight запросы по определению не в буфере
--    - wraparound request_id не страшен: используем wall-clock мкс, монотонные
--      независимо от рестартов observer-а
--    - sync списка юнитов из DBA_OB_UNITS перед каждым прогоном:
--        * новые юниты подхватываются автоматически (INSERT IGNORE)
--        * пропавшие юниты ("ghosts") обрабатываются последний раз,
--          забирают остатки из буфера, потом удаляются из checkpoint
--
--  Старая sp_collect_ddl_dcl_audit + audit_collector_state не трогаются.
--  Обе могут работать параллельно — UNIQUE KEY (svr_ip, request_id) в
--  ddl_dcl_audit_log предотвращает дубликаты.
--
--  Способы вызова:
--    A. Через обёртку (без параметров, удобно для ручного вызова):
--         CALL admintools.sp_collect_ddl_dcl_audit_v2_run();
--       → автоматически выведет два result set'а:
--         (1) агрегаты прогона
--         (2) per-unit детализация
--
--    B. Через основную процедуру (для Go-приложения, с OUT-параметрами):
--         CALL admintools.sp_collect_ddl_dcl_audit_v2(
--             @inserted, @rows_scanned, @units_total, @units_new,
--             @units_with_data, @units_ghost, @duration_ms);
--         SELECT @inserted, @rows_scanned, @units_total, @units_new,
--                @units_with_data, @units_ghost, @duration_ms;
--         SELECT * FROM admintools.ddl_dcl_audit_last_run_stats;
--
--  Установка (важно: collation_connection сессии должен быть utf8mb4_general_ci):
--    obclient -h <host> -P 2881 -u root@sys -p admintools < ddldclaudit_v2.sql
--    либо
--    mysql ... --init-command="SET NAMES utf8mb4 COLLATE utf8mb4_general_ci" ...
-- =============================================================================


-- ─── 0. DDL новых таблиц ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS admintools.ddl_dcl_audit_checkpoint (
    svr_ip         VARCHAR(46) NOT NULL COMMENT 'IP OBServer-узла',
    svr_port       BIGINT      NOT NULL COMMENT 'RPC порт (обычно 2882)',
    tenant_id      BIGINT      NOT NULL COMMENT 'ID тенанта',
    last_end_time  BIGINT      NOT NULL DEFAULT 0
                              COMMENT 'request_time + elapsed_time последней обработанной записи, мкс',
    updated_at     DATETIME(6)     NULL COMMENT 'Wall-clock последнего прогона процедуры',
    PRIMARY KEY (svr_ip, svr_port, tenant_id)
) COMMENT='Per server-tenant курсор DDL/DCL аудита (v2 с end_time)';


CREATE TABLE IF NOT EXISTS admintools.ddl_dcl_audit_ghost_buffer (
    svr_ip    VARCHAR(46) NOT NULL,
    svr_port  BIGINT      NOT NULL,
    tenant_id BIGINT      NOT NULL,
    PRIMARY KEY (svr_ip, svr_port, tenant_id)
) COMMENT='Технический буфер ghost-юнитов между фазами sp_collect_ddl_dcl_audit_v2';


CREATE TABLE IF NOT EXISTS admintools.ddl_dcl_audit_last_run_stats (
    seq             BIGINT      NOT NULL AUTO_INCREMENT COMMENT 'Порядок обработки в текущем прогоне',
    svr_ip          VARCHAR(46) NOT NULL,
    svr_port        BIGINT      NOT NULL,
    tenant_id       BIGINT      NOT NULL,
    status          VARCHAR(16) NOT NULL COMMENT 'LIVE / GHOST_PURGED',
    last_end_before BIGINT      NOT NULL COMMENT 'Курсор до обработки',
    last_end_after  BIGINT          NULL COMMENT 'Курсор после (NULL если новых не было)',
    rows_scanned    BIGINT      NOT NULL DEFAULT 0
                               COMMENT 'Сколько строк прошло окно до DDL/DCL фильтров',
    rows_inserted   BIGINT      NOT NULL DEFAULT 0
                               COMMENT 'Сколько строк вставлено в ddl_dcl_audit_log',
    duration_us     BIGINT      NOT NULL DEFAULT 0
                               COMMENT 'Длительность обработки этого юнита, мкс',
    PRIMARY KEY (seq),
    KEY idx_unit (svr_ip, svr_port, tenant_id)
) COMMENT='Детализация последнего прогона sp_collect_ddl_dcl_audit_v2 (обнуляется при каждом CALL)';


-- ─── 1. Основная процедура (с OUT-параметрами, для Go-приложения) ────────

DELIMITER $$

DROP PROCEDURE IF EXISTS admintools.sp_collect_ddl_dcl_audit_v2 $$

CREATE PROCEDURE admintools.sp_collect_ddl_dcl_audit_v2(
    OUT p_inserted          BIGINT,
    OUT p_rows_scanned      BIGINT,
    OUT p_units_total       BIGINT,
    OUT p_units_new         BIGINT,
    OUT p_units_with_data   BIGINT,
    OUT p_units_ghost       BIGINT,
    OUT p_duration_ms       BIGINT
)
    SQL SECURITY INVOKER
    COMMENT 'DDL/DCL аудит v2: per-server-tenant курсор + расширенная диагностика'
BEGIN
    -- ─── локальные переменные ───────────────────────────────────────────
    DECLARE v_done                TINYINT  DEFAULT 0;

    -- агрегаты прогона
    DECLARE v_inserted_total      BIGINT   DEFAULT 0;
    DECLARE v_rows_scanned_total  BIGINT   DEFAULT 0;
    DECLARE v_units_total         BIGINT   DEFAULT 0;
    DECLARE v_units_new           BIGINT   DEFAULT 0;
    DECLARE v_units_with_data     BIGINT   DEFAULT 0;
    DECLARE v_units_ghost         BIGINT   DEFAULT 0;
    DECLARE v_ckpt_before         BIGINT   DEFAULT 0;
    DECLARE v_ckpt_after          BIGINT   DEFAULT 0;
    DECLARE v_started_at_usec     BIGINT   DEFAULT 0;

    -- per-unit
    DECLARE v_unit_started_usec   BIGINT;
    DECLARE v_svr_ip              VARCHAR(46);
    DECLARE v_svr_port            BIGINT;
    DECLARE v_tenant_id           BIGINT;
    DECLARE v_last_end            BIGINT;
    DECLARE v_new_end             BIGINT;
    DECLARE v_rows_scanned_unit   BIGINT;
    DECLARE v_inserted_unit       BIGINT;
    DECLARE v_is_ghost            BIGINT;
    DECLARE v_status              VARCHAR(16);

    -- сборка динамического WHERE из targets
    DECLARE v_dyn_targets         LONGTEXT DEFAULT '';
    DECLARE v_tgt_tenant_id       BIGINT;
    DECLARE v_tgt_db_name         VARCHAR(128);
    DECLARE v_tgt_obj_name        VARCHAR(128);
    DECLARE v_db_esc              VARCHAR(256);
    DECLARE v_obj_esc             VARCHAR(256);

    DECLARE cur_targets CURSOR FOR
        SELECT tenant_id, db_name, object_name
          FROM admintools.ddl_dcl_audit_targets
         WHERE is_active = 1
         ORDER BY id;

    DECLARE cur_checkpoint CURSOR FOR
        SELECT svr_ip, svr_port, tenant_id, last_end_time
          FROM admintools.ddl_dcl_audit_checkpoint
         ORDER BY svr_ip, svr_port, tenant_id;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;


    -- ═══ старт замера времени ════════════════════════════════════════════
    SET v_started_at_usec = time_to_usec(NOW(6));


    -- ═══ Шаг 0: обнуление статистики прошлого прогона ════════════════════
    DELETE FROM admintools.ddl_dcl_audit_last_run_stats WHERE 1=1;


    -- ═══ Шаг 1: sync списка юнитов из DBA_OB_UNITS ═══════════════════════
    SELECT COUNT(*) INTO v_ckpt_before FROM admintools.ddl_dcl_audit_checkpoint;

    INSERT IGNORE INTO admintools.ddl_dcl_audit_checkpoint
        (svr_ip, svr_port, tenant_id, last_end_time)
    SELECT svr_ip, svr_port, tenant_id, 0
      FROM oceanbase.DBA_OB_UNITS
     WHERE status = 'ACTIVE';

    SELECT COUNT(*) INTO v_ckpt_after FROM admintools.ddl_dcl_audit_checkpoint;
    SET v_units_new   = v_ckpt_after - v_ckpt_before;
    SET v_units_total = v_ckpt_after;


    -- ═══ Шаг 2: запоминание ghost-юнитов (snapshot) ══════════════════════
    DELETE FROM admintools.ddl_dcl_audit_ghost_buffer WHERE 1=1;

    INSERT INTO admintools.ddl_dcl_audit_ghost_buffer (svr_ip, svr_port, tenant_id)
    SELECT c.svr_ip, c.svr_port, c.tenant_id
      FROM admintools.ddl_dcl_audit_checkpoint c
      LEFT JOIN oceanbase.DBA_OB_UNITS u
        ON u.svr_ip    = c.svr_ip
       AND u.svr_port  = c.svr_port
       AND u.tenant_id = c.tenant_id
       AND u.status    = 'ACTIVE'
     WHERE u.tenant_id IS NULL;

    SELECT COUNT(*) INTO v_units_ghost FROM admintools.ddl_dcl_audit_ghost_buffer;


    -- ═══ Шаг 3: собираем динамический WHERE из targets ════════════════════
    OPEN cur_targets;
    target_loop: LOOP
        FETCH cur_targets INTO v_tgt_tenant_id, v_tgt_db_name, v_tgt_obj_name;
        IF v_done = 1 THEN LEAVE target_loop; END IF;

        SET v_obj_esc = REPLACE(v_tgt_obj_name, '''', '''''');

        IF v_tgt_db_name IS NOT NULL AND v_tgt_db_name <> '' THEN
            SET v_db_esc = REPLACE(v_tgt_db_name, '''', '''''');
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


    -- ═══ Шаг 4: подготовка INSERT-prepared statement один раз ═════════════
    SET @ins_sql = CONCAT(
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
        ' WHERE svr_ip    = ?',
        '   AND svr_port  = ?',
        '   AND tenant_id = ?',
        '   AND is_inner_sql = 0',
        '   AND request_time + elapsed_time >  ?',
        '   AND request_time + elapsed_time <= ?',
        '   AND stmt_type NOT IN (''VARIABLE_SET'')',
        '   AND query_sql NOT LIKE ''%INSERT IGNORE INTO admintools.ddl_dcl_audit_log%''',
        '   AND query_sql NOT LIKE ''%UPDATE sessions SET logoff_time%''',
        '   AND query_sql NOT LIKE ''%UPDATE sessions p JOIN sessions s%''',
        '   AND query_sql NOT LIKE ''%sp_collect_ddl_dcl_audit%''',
        '   AND query_sql NOT LIKE ''%ddl_dcl_audit_checkpoint%''',
        '   AND query_sql NOT LIKE ''%ddl_dcl_audit_ghost_buffer%''',
        '   AND query_sql NOT LIKE ''%ddl_dcl_audit_last_run_stats%''',
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
        '         OR query_sql LIKE ''%admintools.ddl_dcl_audit_checkpoint%''',
        '         OR (db_name = ''admintools'' AND query_sql LIKE ''%ddl_dcl_audit_checkpoint%'')',
        '       )',
        '     )',
        v_dyn_targets,
        '   )'
    );

    PREPARE stmt_ins FROM @ins_sql;


    -- ═══ Шаг 5: цикл по всем строкам checkpoint ══════════════════════════
    OPEN cur_checkpoint;
    ckpt_loop: LOOP
        FETCH cur_checkpoint INTO v_svr_ip, v_svr_port, v_tenant_id, v_last_end;
        IF v_done = 1 THEN LEAVE ckpt_loop; END IF;

        SET v_unit_started_usec = time_to_usec(NOW(6));

        SELECT COUNT(*) INTO v_is_ghost
          FROM admintools.ddl_dcl_audit_ghost_buffer
         WHERE svr_ip    = v_svr_ip
           AND svr_port  = v_svr_port
           AND tenant_id = v_tenant_id;

        IF v_is_ghost > 0 THEN
            SET v_status = 'GHOST_PURGED';
        ELSE
            SET v_status = 'LIVE';
        END IF;

        SET v_new_end = NULL;
        SET v_rows_scanned_unit = 0;
        SELECT COUNT(*),
               MAX(request_time + elapsed_time)
          INTO v_rows_scanned_unit, v_new_end
          FROM oceanbase.GV$OB_SQL_AUDIT
         WHERE svr_ip    = v_svr_ip
           AND svr_port  = v_svr_port
           AND tenant_id = v_tenant_id
           AND is_inner_sql = 0
           AND request_time + elapsed_time > v_last_end;

        SET v_inserted_unit = 0;

        IF v_new_end IS NOT NULL THEN
            SET @p_svr_ip    = v_svr_ip;
            SET @p_svr_port  = v_svr_port;
            SET @p_tenant_id = v_tenant_id;
            SET @p_last_end  = v_last_end;
            SET @p_new_end   = v_new_end;

            EXECUTE stmt_ins USING @p_svr_ip, @p_svr_port, @p_tenant_id,
                                   @p_last_end, @p_new_end;
            SET v_inserted_unit   = ROW_COUNT();
            SET v_inserted_total  = v_inserted_total + v_inserted_unit;
            SET v_units_with_data = v_units_with_data + 1;

            UPDATE admintools.ddl_dcl_audit_checkpoint
               SET last_end_time = v_new_end,
                   updated_at    = NOW(6)
             WHERE svr_ip    = v_svr_ip
               AND svr_port  = v_svr_port
               AND tenant_id = v_tenant_id;
        ELSE
            UPDATE admintools.ddl_dcl_audit_checkpoint
               SET updated_at = NOW(6)
             WHERE svr_ip    = v_svr_ip
               AND svr_port  = v_svr_port
               AND tenant_id = v_tenant_id;
        END IF;

        SET v_rows_scanned_total = v_rows_scanned_total + v_rows_scanned_unit;

        INSERT INTO admintools.ddl_dcl_audit_last_run_stats
            (svr_ip, svr_port, tenant_id, status,
             last_end_before, last_end_after,
             rows_scanned, rows_inserted, duration_us)
        VALUES
            (v_svr_ip, v_svr_port, v_tenant_id, v_status,
             v_last_end, v_new_end,
             v_rows_scanned_unit, v_inserted_unit,
             time_to_usec(NOW(6)) - v_unit_started_usec);
    END LOOP ckpt_loop;
    CLOSE cur_checkpoint;
    SET v_done = 0;

    DEALLOCATE PREPARE stmt_ins;


    -- ═══ Шаг 6: cleanup ghost-юнитов ═════════════════════════════════════
    DELETE c FROM admintools.ddl_dcl_audit_checkpoint c
      JOIN admintools.ddl_dcl_audit_ghost_buffer g
        ON g.svr_ip    = c.svr_ip
       AND g.svr_port  = c.svr_port
       AND g.tenant_id = c.tenant_id;

    DELETE FROM admintools.ddl_dcl_audit_ghost_buffer WHERE 1=1;


    -- ═══ возврат метрик ══════════════════════════════════════════════════
    SET p_inserted        = v_inserted_total;
    SET p_rows_scanned    = v_rows_scanned_total;
    SET p_units_total     = v_units_total;
    SET p_units_new       = v_units_new;
    SET p_units_with_data = v_units_with_data;
    SET p_units_ghost     = v_units_ghost;
    SET p_duration_ms     = (time_to_usec(NOW(6)) - v_started_at_usec) DIV 1000;
END $$


-- =============================================================================
--  admintools.sp_collect_ddl_dcl_audit_v2_run
--
--  Обёртка без параметров для удобного ручного вызова.
--  Внутри вызывает sp_collect_ddl_dcl_audit_v2 и автоматически выводит
--  два result set'а:
--    1. агрегаты прогона (одна строка)
--    2. per-unit детализация из ddl_dcl_audit_last_run_stats
--
--  Использование:
--    CALL admintools.sp_collect_ddl_dcl_audit_v2_run();
-- =============================================================================

DROP PROCEDURE IF EXISTS admintools.sp_collect_ddl_dcl_audit_v2_run $$

CREATE PROCEDURE admintools.sp_collect_ddl_dcl_audit_v2_run()
    SQL SECURITY INVOKER
    COMMENT 'Обёртка для ручного вызова sp_collect_ddl_dcl_audit_v2 с выводом метрик'
BEGIN
    DECLARE v_inserted        BIGINT;
    DECLARE v_rows_scanned    BIGINT;
    DECLARE v_units_total     BIGINT;
    DECLARE v_units_new       BIGINT;
    DECLARE v_units_with_data BIGINT;
    DECLARE v_units_ghost     BIGINT;
    DECLARE v_duration_ms     BIGINT;

    -- сам прогон
    CALL admintools.sp_collect_ddl_dcl_audit_v2(
        v_inserted, v_rows_scanned, v_units_total, v_units_new,
        v_units_with_data, v_units_ghost, v_duration_ms);

    -- result set 1: агрегаты
    SELECT v_inserted        AS inserted,
           v_rows_scanned    AS rows_scanned,
           v_units_total     AS units_total,
           v_units_new       AS units_new,
           v_units_with_data AS units_with_data,
           v_units_ghost     AS units_ghost,
           v_duration_ms     AS duration_ms;

    -- result set 2: per-unit детализация
    SELECT seq,
           svr_ip, svr_port, tenant_id, status,
           usec_to_time(last_end_before) AS last_end_before_ts,
           usec_to_time(last_end_after)  AS last_end_after_ts,
           rows_scanned,
           rows_inserted,
           ROUND(duration_us / 1000.0, 2) AS duration_ms
      FROM admintools.ddl_dcl_audit_last_run_stats
     ORDER BY seq;
END $$


-- =============================================================================
--  admintools.sp_collect_ddl_dcl_audit_v2_preview
--
--  Версия для отладки. НЕ выполняет INSERT и НЕ обновляет checkpoint.
--  Возвращает 2 result set:
--    1. список юнитов которые будут обработаны (с пометкой ghost/live/new)
--    2. сгенерированный текст INSERT (схематично, с плейсхолдерами)
-- =============================================================================
DROP PROCEDURE IF EXISTS admintools.sp_collect_ddl_dcl_audit_v2_preview $$

CREATE PROCEDURE admintools.sp_collect_ddl_dcl_audit_v2_preview()
    SQL SECURITY INVOKER
    COMMENT 'Preview для sp_collect_ddl_dcl_audit_v2 без побочных эффектов'
BEGIN
    DECLARE v_done           TINYINT DEFAULT 0;
    DECLARE v_dyn_targets    LONGTEXT DEFAULT '';
    DECLARE v_tgt_tenant_id  BIGINT;
    DECLARE v_tgt_db_name    VARCHAR(128);
    DECLARE v_tgt_obj_name   VARCHAR(128);
    DECLARE v_db_esc         VARCHAR(256);
    DECLARE v_obj_esc        VARCHAR(256);

    DECLARE cur_targets CURSOR FOR
        SELECT tenant_id, db_name, object_name
          FROM admintools.ddl_dcl_audit_targets
         WHERE is_active = 1
         ORDER BY id;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    OPEN cur_targets;
    target_loop: LOOP
        FETCH cur_targets INTO v_tgt_tenant_id, v_tgt_db_name, v_tgt_obj_name;
        IF v_done = 1 THEN LEAVE target_loop; END IF;
        SET v_obj_esc = REPLACE(v_tgt_obj_name, '''', '''''');
        IF v_tgt_db_name IS NOT NULL AND v_tgt_db_name <> '' THEN
            SET v_db_esc = REPLACE(v_tgt_db_name, '''', '''''');
            SET v_dyn_targets = CONCAT(v_dyn_targets,
                '     OR (query_sql LIKE ''%', v_db_esc, '.', v_obj_esc, '%''',
                ' OR (db_name = ''', v_db_esc, ''' AND query_sql LIKE ''%', v_obj_esc, '%''))'
            );
        ELSE
            SET v_dyn_targets = CONCAT(v_dyn_targets,
                '     OR query_sql LIKE ''%', v_obj_esc, '%'''
            );
        END IF;
    END LOOP target_loop;
    CLOSE cur_targets;

    -- 1) список юнитов которые будут обработаны
    SELECT 'EXISTING' AS source_in_run,
           c.svr_ip, c.svr_port, c.tenant_id,
           c.last_end_time,
           usec_to_time(c.last_end_time) AS last_end_ts,
           c.updated_at,
           CASE WHEN u.tenant_id IS NULL THEN 'GHOST' ELSE 'LIVE' END AS status
      FROM admintools.ddl_dcl_audit_checkpoint c
      LEFT JOIN oceanbase.DBA_OB_UNITS u
        ON u.svr_ip = c.svr_ip AND u.svr_port = c.svr_port
       AND u.tenant_id = c.tenant_id AND u.status = 'ACTIVE'
    UNION ALL
    SELECT 'NEW (will be added)' AS source_in_run,
           u.svr_ip, u.svr_port, u.tenant_id,
           0 AS last_end_time,
           usec_to_time(0) AS last_end_ts,
           NULL AS updated_at,
           'LIVE' AS status
      FROM oceanbase.DBA_OB_UNITS u
      LEFT JOIN admintools.ddl_dcl_audit_checkpoint c
        ON c.svr_ip = u.svr_ip AND c.svr_port = u.svr_port
       AND c.tenant_id = u.tenant_id
     WHERE u.status = 'ACTIVE' AND c.tenant_id IS NULL
    ORDER BY svr_ip, svr_port, tenant_id;

    -- 2) сгенерированный INSERT (схематично)
    SET @ins_sql = CONCAT(
        'INSERT IGNORE INTO admintools.ddl_dcl_audit_log (...)\n',
        'SELECT ... FROM oceanbase.GV$OB_SQL_AUDIT\n',
        ' WHERE svr_ip = ? AND svr_port = ? AND tenant_id = ?\n',
        '   AND is_inner_sql = 0\n',
        '   AND request_time + elapsed_time >  ? (= v_last_end)\n',
        '   AND request_time + elapsed_time <= ? (= v_new_end)\n',
        '   AND <DDL/DCL filters>',
        v_dyn_targets
    );
    SELECT @ins_sql AS generated_sql_skeleton;
END $$

DELIMITER ;


-- =============================================================================
--  Примеры использования
-- =============================================================================
--
-- 1) Удобный ручной запуск (без параметров, два result set'а на выходе):
--    CALL admintools.sp_collect_ddl_dcl_audit_v2_run();
--
-- 2) Программный запуск (для Go-приложения, с OUT-параметрами):
--    CALL admintools.sp_collect_ddl_dcl_audit_v2(
--        @inserted, @rows_scanned, @units_total, @units_new,
--        @units_with_data, @units_ghost, @duration_ms);
--    SELECT @inserted, @rows_scanned, @units_total, @units_new,
--           @units_with_data, @units_ghost, @duration_ms;
--    SELECT * FROM admintools.ddl_dcl_audit_last_run_stats;
--
-- 3) Preview без побочных эффектов:
--    CALL admintools.sp_collect_ddl_dcl_audit_v2_preview();
--
-- 4) Текущее состояние всех курсоров:
--    SELECT svr_ip, svr_port, tenant_id,
--           last_end_time,
--           usec_to_time(last_end_time) AS last_end_ts,
--           updated_at,
--           TIMESTAMPDIFF(SECOND, updated_at, NOW(6)) AS age_seconds
--      FROM admintools.ddl_dcl_audit_checkpoint
--     ORDER BY svr_ip, svr_port, tenant_id;
--
-- 5) Сравнение checkpoint с DBA_OB_UNITS (увидеть ghosts и пропущенные новые):
--    SELECT 'in_checkpoint_but_not_in_units' AS issue,
--           c.svr_ip, c.svr_port, c.tenant_id
--      FROM admintools.ddl_dcl_audit_checkpoint c
--      LEFT JOIN oceanbase.DBA_OB_UNITS u
--        ON u.svr_ip=c.svr_ip AND u.svr_port=c.svr_port
--       AND u.tenant_id=c.tenant_id AND u.status='ACTIVE'
--     WHERE u.tenant_id IS NULL
--    UNION ALL
--    SELECT 'in_units_but_not_in_checkpoint' AS issue,
--           u.svr_ip, u.svr_port, u.tenant_id
--      FROM oceanbase.DBA_OB_UNITS u
--      LEFT JOIN admintools.ddl_dcl_audit_checkpoint c
--        ON c.svr_ip=u.svr_ip AND c.svr_port=u.svr_port
--       AND c.tenant_id=u.tenant_id
--     WHERE u.status='ACTIVE' AND c.tenant_id IS NULL;
--
-- 6) Сброс всех курсоров (повторно обработать буфер):
--    UPDATE admintools.ddl_dcl_audit_checkpoint SET last_end_time = 0;
--
-- 7) Полная чистка состояния v2 (начать с нуля):
--    DELETE FROM admintools.ddl_dcl_audit_checkpoint     WHERE 1=1;
--    DELETE FROM admintools.ddl_dcl_audit_ghost_buffer   WHERE 1=1;
--    DELETE FROM admintools.ddl_dcl_audit_last_run_stats WHERE 1=1;
--
-- 8) Регулярный запуск через cron (раз в минуту, человеко-читаемый вывод):
--    obclient -h 192.168.55.205 -P 2881 -u ocp@sys -pqaz123 admintools \
--      -e "CALL admintools.sp_collect_ddl_dcl_audit_v2_run();"
--
-- 9) Что попало в лог за последние 10 минут:
--    SELECT collected_at, request_ts, tenant_name, user_name, stmt_type,
--           LEFT(query_sql, 100) AS query_preview
--      FROM admintools.ddl_dcl_audit_log
--     WHERE collected_at > NOW() - INTERVAL 10 MINUTE
--     ORDER BY id DESC;
-- =============================================================================
