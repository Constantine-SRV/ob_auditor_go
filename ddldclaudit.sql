-- =============================================================================
--  admintools.sp_collect_ddl_dcl_audit
--
--  Аналог Go-кода DdlDclAuditDao.Collect() в виде хранимой процедуры.
--  Полностью самодостаточна: все данные читаются из БД, всё пишется в БД.
--
--  Что делает:
--    1. читает last_request_time из admintools.audit_collector_state (id=1)
--    2. находит MAX(request_time) среди новых строк GV$OB_SQL_AUDIT
--    3. строит динамический WHERE с учётом ddl_dcl_audit_targets
--    4. INSERT IGNORE в admintools.ddl_dcl_audit_log
--    5. обновляет last_request_time и updated_at в audit_collector_state
--
--  Установка:
--    obclient -h <host> -P 2881 -u ocp@sys -p admintools < sp_collect_ddl_dcl_audit.sql
--
--  Запуск:
--    CALL admintools.sp_collect_ddl_dcl_audit(@inserted, @new_rt);
--    SELECT @inserted, @new_rt;
--
--  Привилегии (для пользователя ocp):
--    GRANT CREATE ROUTINE, EXECUTE ON admintools.* TO 'ocp'@'%';
--    -- SELECT на oceanbase.* уже выдан (для GV$OB_SQL_AUDIT)
--
--  Примечание про SQL SECURITY:
--    Используется INVOKER — процедура работает с правами вызвавшего пользователя.
--    Это значит, что у вызывающего должны быть права на SELECT GV$OB_SQL_AUDIT
--    и INSERT/UPDATE в admintools.*. У ocp всё это уже есть.
-- =============================================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS admintools.sp_collect_ddl_dcl_audit $$

CREATE PROCEDURE admintools.sp_collect_ddl_dcl_audit(
    OUT p_inserted          BIGINT,
    OUT p_new_request_time  BIGINT
)
    SQL SECURITY INVOKER
    COMMENT 'Перенос новых DDL/DCL событий из GV$OB_SQL_AUDIT в ddl_dcl_audit_log'
BEGIN
    -- ─── локальные переменные ────────────────────────────────────────────────
    DECLARE v_last_rt        BIGINT  DEFAULT 0;
    DECLARE v_new_rt         BIGINT  DEFAULT NULL;
    DECLARE v_inserted       BIGINT  DEFAULT 0;

    -- курсор по динамическим targets
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

    -- ─── 1) последнее обработанное request_time ──────────────────────────────
    SELECT last_request_time
      INTO v_last_rt
      FROM admintools.audit_collector_state
     WHERE id = 1;

    -- если строки нет — стартуем с нуля
    IF v_last_rt IS NULL THEN
        SET v_last_rt = 0;
    END IF;

    -- ─── 2) максимум среди новых строк ───────────────────────────────────────
    SELECT MAX(request_time)
      INTO v_new_rt
      FROM oceanbase.GV$OB_SQL_AUDIT
     WHERE is_inner_sql = 0
       AND request_time > v_last_rt;

    -- ─── 3) если ничего нового — heartbeat и выход ───────────────────────────
    IF v_new_rt IS NULL THEN
        UPDATE admintools.audit_collector_state
           SET updated_at = NOW(6)
         WHERE id = 1;

        SET p_inserted         = 0;
        SET p_new_request_time = v_last_rt;
    ELSE
        -- ─── 4) собираем динамический WHERE из targets ───────────────────────
        OPEN cur_targets;
        target_loop: LOOP
            FETCH cur_targets INTO v_tenant_id, v_db_name, v_obj_name;
            IF v_done = 1 THEN
                LEAVE target_loop;
            END IF;

            -- экранируем одинарные кавычки удвоением (SQL-стандарт)
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

        -- ─── 5) собираем и выполняем INSERT IGNORE через PREPARE ─────────────
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
            -- ── глобальные исключения собственных служебных запросов ──
            '   AND query_sql NOT LIKE ''%INSERT IGNORE INTO admintools.ddl_dcl_audit_log%''',
            '   AND query_sql NOT LIKE ''%UPDATE sessions SET logoff_time%''',
            '   AND query_sql NOT LIKE ''%UPDATE sessions p JOIN sessions s%''',
            '   AND query_sql NOT LIKE ''%sp_collect_ddl_dcl_audit%''',
            '   AND (',
            -- ── основной список DDL/DCL stmt_type ──
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
            -- ── user management через LIKE (отдельного stmt_type нет) ──
            '     OR (',
            '         query_sql LIKE ''%CREATE USER%''',
            '         OR query_sql LIKE ''%ALTER USER%''',
            '         OR query_sql LIKE ''%lock_user(%''',
            '         OR query_sql LIKE ''%unlock_user(%''',
            '     )',
            -- ── DELETE/UPDATE таблиц аудита ──
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
            -- ── динамические targets ──
            v_dyn_targets,
            '   )'
        );

        PREPARE  stmt FROM @sql;
        EXECUTE  stmt USING @last_rt, @new_rt;
        SET      v_inserted = ROW_COUNT();
        DEALLOCATE PREPARE stmt;

        -- ─── 6) обновляем state ──────────────────────────────────────────────
        UPDATE admintools.audit_collector_state
           SET last_request_time = v_new_rt,
               updated_at        = NOW(6)
         WHERE id = 1;

        SET p_inserted         = v_inserted;
        SET p_new_request_time = v_new_rt;
    END IF;
END $$


-- =============================================================================
--  admintools.sp_collect_ddl_dcl_audit_preview
--
--  Версия для отладки: НЕ выполняет INSERT, только показывает что бы выполнилось.
--  Возвращает result set с last_rt, new_rt и текстом сгенерированного SQL.
-- =============================================================================
DROP PROCEDURE IF EXISTS admintools.sp_collect_ddl_dcl_audit_preview $$

CREATE PROCEDURE admintools.sp_collect_ddl_dcl_audit_preview()
    SQL SECURITY INVOKER
    COMMENT 'Показывает SQL, который выполнила бы sp_collect_ddl_dcl_audit, БЕЗ INSERT'
BEGIN
    DECLARE v_last_rt        BIGINT  DEFAULT 0;
    DECLARE v_new_rt         BIGINT  DEFAULT NULL;

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

    SELECT last_request_time INTO v_last_rt
      FROM admintools.audit_collector_state WHERE id = 1;
    IF v_last_rt IS NULL THEN SET v_last_rt = 0; END IF;

    SELECT MAX(request_time) INTO v_new_rt
      FROM oceanbase.GV$OB_SQL_AUDIT
     WHERE is_inner_sql = 0 AND request_time > v_last_rt;

    OPEN cur_targets;
    target_loop: LOOP
        FETCH cur_targets INTO v_tenant_id, v_db_name, v_obj_name;
        IF v_done = 1 THEN LEAVE target_loop; END IF;
        SET v_obj_esc = REPLACE(v_obj_name, '''', '''''');
        IF v_db_name IS NOT NULL AND v_db_name <> '' THEN
            SET v_db_esc = REPLACE(v_db_name, '''', '''''');
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
        '   AND request_time >  ', v_last_rt,
        '   AND request_time <= ', IFNULL(v_new_rt, 0),
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

    SELECT v_last_rt          AS last_request_time,
           v_new_rt            AS new_request_time,
           @sql                AS generated_sql;
END $$


DELIMITER ;

-- =============================================================================
--  Примеры использования
-- =============================================================================
--
-- 1) Боевой запуск:
--    CALL admintools.sp_collect_ddl_dcl_audit(@n, @rt);
--    SELECT @n AS inserted, @rt AS new_request_time;
--
-- 2) Посмотреть, что бы выполнилось (без INSERT):
--    CALL admintools.sp_collect_ddl_dcl_audit_preview();
--
-- 3) Текущее состояние коллектора:
--    SELECT * FROM admintools.audit_collector_state WHERE id = 1;
--
-- 4) Что попало в лог за последние 10 минут:
--    SELECT collected_at, user_name, stmt_type,
--           LEFT(query_sql, 100) AS query_preview
--    FROM admintools.ddl_dcl_audit_log
--    WHERE collected_at > NOW() - INTERVAL 10 MINUTE
--    ORDER BY id DESC;
--
-- 5) Сброс курсора (например, чтобы перечитать всё с нуля):
--    UPDATE admintools.audit_collector_state SET last_request_time = 0 WHERE id = 1;
--
-- 6) Регулярный запуск (в OceanBase через DBMS_SCHEDULER, либо извне через cron):
--    -- например, раз в минуту из скрипта:
--    obclient -h 192.168.55.205 -P 2881 -u ocp@sys -pqaz123 admintools \
--      -e "CALL admintools.sp_collect_ddl_dcl_audit(@n,@rt); SELECT @n,@rt;"
-- =============================================================================