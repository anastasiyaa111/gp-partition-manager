CREATE OR REPLACE FUNCTION partition_utils.proc_dependency_manager(
    p_operation_mode text,
    p_schema_name text,
    p_table_name text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE
AS $$
DECLARE
    v_table_oid OID;
    v_dependent_record RECORD;
    v_grant_record RECORD;
    v_direct_dependency_record RECORD;
    v_object_identity TEXT;
    v_object_owner TEXT;
    v_object_ddl TEXT;
    v_object_type TEXT;
    v_view_oid OID;
    v_full_table_name TEXT;
    proc text := 'proc_dependency_manager';
BEGIN
    -- =========================================================================
    -- РЕЖИМ SAVE
    -- =========================================================================
    IF upper(p_operation_mode) = 'SAVE' THEN
        RAISE NOTICE '[MANAGE_DEP] [SAVE] Начало сохранения зависимостей для %.%',
            p_schema_name, p_table_name;

        v_full_table_name := quote_ident(p_schema_name) || '.' || quote_ident(p_table_name);

        SELECT c.oid INTO v_table_oid
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = p_schema_name AND c.relname = p_table_name;

        IF v_table_oid IS NULL THEN
            RAISE WARNING '[MANAGE_DEP] Таблица %.% не найдена. SAVE пропущена.',
                p_schema_name, p_table_name;
            RETURN;
        END IF;

        -- Создаем временные таблицы для хранения метаданных
        CREATE TABLE IF NOT EXISTS partition_utils.temp_saved_ddl (
            priority INT,
            object_identity TEXT,
            object_owner TEXT,
            object_ddl TEXT
        );

        CREATE TABLE IF NOT EXISTS partition_utils.temp_saved_grants (
            grant_command TEXT
        );

        TRUNCATE partition_utils.temp_saved_ddl;
        TRUNCATE partition_utils.temp_saved_grants;
        RAISE NOTICE '[MANAGE_DEP] Таблицы DDL и грантов очищены.';

        -- 1. СОХРАНЯЕМ ПРАВА САМОЙ ТАБЛИЦЫ
        RAISE NOTICE '[MANAGE_DEP] Сохранение прав исходной таблицы...';
        FOR v_grant_record IN
            SELECT
                CASE WHEN a.grantee = 0 THEN 'PUBLIC'
                     ELSE quote_ident(pg_get_userbyid(a.grantee))
                END as grantee_name,
                a.privilege_type
            FROM pg_class c,
                 aclexplode(c.relacl) a
            WHERE c.oid = v_table_oid
        LOOP
            INSERT INTO partition_utils.temp_saved_grants
            VALUES (format('GRANT %s ON %s TO %s;',
                v_grant_record.privilege_type,
                v_full_table_name,
                v_grant_record.grantee_name));

            RAISE NOTICE '[MANAGE_DEP] [TABLE] Сохранено право: %', format('GRANT %s ON %s TO %s;',
                v_grant_record.privilege_type, v_full_table_name, v_grant_record.grantee_name);
        END LOOP;

        -- 2. РЕКУРСИВНЫЙ ПОИСК ЗАВИСИМОСТЕЙ (VIEWS)
        RAISE NOTICE '[MANAGE_DEP] Рекурсивный поиск зависимостей (VIEWS)...';
        FOR v_dependent_record IN
            WITH RECURSIVE dependency_tree AS (
                SELECT
                    CASE WHEN d.classid = 'pg_rewrite'::regclass::oid
                        THEN (SELECT ev_class FROM pg_rewrite WHERE oid = d.objid)
                        ELSE d.objid
                    END AS view_oid,
                    1 AS depth,
                    ARRAY[
                        CASE WHEN d.classid = 'pg_rewrite'::regclass::oid
                            THEN (SELECT ev_class FROM pg_rewrite WHERE oid = d.objid)
                            ELSE d.objid
                        END
                    ]::oid[] AS path
                FROM pg_depend d
                WHERE d.refobjid = v_table_oid
                  AND d.deptype = 'n'
                  AND d.classid = 'pg_rewrite'::regclass::oid

                UNION ALL

                SELECT
                    CASE WHEN d.classid = 'pg_rewrite'::regclass::oid
                        THEN (SELECT ev_class FROM pg_rewrite WHERE oid = d.objid)
                        ELSE d.objid
                    END AS view_oid,
                    dt.depth + 1 AS depth,
                    path || CASE WHEN d.classid = 'pg_rewrite'::regclass::oid
                        THEN (SELECT ev_class FROM pg_rewrite WHERE oid = d.objid)
                        ELSE d.objid
                    END AS path
                FROM pg_depend d
                JOIN dependency_tree dt ON d.refobjid = dt.view_oid
                WHERE d.deptype = 'n'
                  AND d.classid = 'pg_rewrite'::regclass::oid
                  AND NOT (CASE WHEN d.classid = 'pg_rewrite'::regclass::oid
                        THEN (SELECT ev_class FROM pg_rewrite WHERE oid = d.objid)
                        ELSE d.objid
                    END = ANY(dt.path))
            )

            -- 3. СОХРАНЯЕМ DDL НАЙДЕННЫХ VIEWS
            SELECT view_oid, MIN(depth) AS depth
            FROM dependency_tree
            GROUP BY view_oid
            ORDER BY depth ASC
        LOOP
            v_view_oid := v_dependent_record.view_oid;

            SELECT
                quote_ident(n.nspname) || '.' || quote_ident(c.relname),
                pg_get_userbyid(c.relowner),
                'CREATE OR REPLACE VIEW ' || quote_ident(n.nspname) || '.' ||
                quote_ident(c.relname) || ' AS ' || pg_get_viewdef(c.oid),
                'VIEW'
            INTO v_object_identity, v_object_owner, v_object_ddl, v_object_type
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.oid = v_view_oid;

            IF v_object_identity IS NULL THEN
                RAISE WARNING '[MANAGE_DEP] Не удалось получить info о view OID: %', v_view_oid;
                CONTINUE;
            END IF;

            INSERT INTO partition_utils.temp_saved_ddl
            VALUES (v_dependent_record.depth, v_object_identity, v_object_owner, v_object_ddl);

            RAISE NOTICE '[MANAGE_DEP] Сохранен DDL (глубина %): %',
                v_dependent_record.depth, v_object_identity;

            -- 4. СОХРАНЯЕМ ПРАВА ЭТОЙ ВЬЮХИ
            FOR v_grant_record IN
                SELECT
                    CASE WHEN a.grantee = 0 THEN 'PUBLIC'
                         ELSE quote_ident(pg_get_userbyid(a.grantee))
                    END as grantee_name,
                    a.privilege_type
                FROM pg_class c,
                     aclexplode(c.relacl) a
                WHERE c.oid = v_view_oid
            LOOP
                INSERT INTO partition_utils.temp_saved_grants
                VALUES (format('GRANT %s ON %s TO %s;',
                    v_grant_record.privilege_type,
                    v_object_identity,
                    v_grant_record.grantee_name));

                RAISE NOTICE '[MANAGE_DEP] [VIEW] Сохранено право: %', format('GRANT %s ON %s TO %s;',
                    v_grant_record.privilege_type, v_object_identity, v_grant_record.grantee_name);
            END LOOP;
        END LOOP;

        -- 5. УДАЛЕНИЕ ЗАВИСИМЫХ ОБЪЕКТОВ
        RAISE NOTICE '[MANAGE_DEP] Удаление зависимых объектов...';
        FOR v_direct_dependency_record IN
            SELECT
                CASE WHEN d.classid = 'pg_rewrite'::regclass::oid
                     THEN (SELECT ev_class FROM pg_rewrite WHERE oid = d.objid)
                     ELSE d.objid
                END as view_oid
            FROM pg_depend d
            WHERE d.refobjid = v_table_oid
              AND d.deptype = 'n'
              AND d.classid = 'pg_rewrite'::regclass::oid
        LOOP
            SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname)
            INTO v_object_identity
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.oid = v_direct_dependency_record.view_oid;

            IF v_object_identity IS NOT NULL THEN
                EXECUTE 'DROP VIEW IF EXISTS ' || v_object_identity || ' CASCADE;';
                RAISE NOTICE '[MANAGE_DEP] Удален объект: %', v_object_identity;
            END IF;
        END LOOP;

        RAISE NOTICE '[MANAGE_DEP] [SAVE] Завершено.';

    -- =========================================================================
    -- РЕЖИМ RESTORE
    -- =========================================================================
    ELSIF upper(p_operation_mode) = 'RESTORE' THEN
        RAISE NOTICE '[MANAGE_DEP] [RESTORE] Начало восстановления зависимостей...';

        -- 1. ВОССТАНОВЛЕНИЕ ОБЪЕКТОВ (VIEWS)
        RAISE NOTICE '[MANAGE_DEP] Восстановление объектов (VIEWS)...';
        FOR v_dependent_record IN
            SELECT * FROM partition_utils.temp_saved_ddl
            ORDER BY priority ASC
        LOOP
            BEGIN
                EXECUTE v_dependent_record.object_ddl;
                RAISE NOTICE '[MANAGE_DEP] Создан объект: % c владельцем: %',
                    v_dependent_record.object_identity, v_dependent_record.object_owner;
                
                EXECUTE format('ALTER VIEW %s OWNER TO %I;',
                    v_dependent_record.object_identity, v_dependent_record.object_owner);
            EXCEPTION WHEN OTHERS THEN
                RAISE WARNING '[MANAGE_DEP] Ошибка восстановления view %: %',
                    v_dependent_record.object_identity, SQLERRM;
            END;
        END LOOP;

        -- 2. ВОССТАНОВЛЕНИЕ ПРАВ ДОСТУПА
        RAISE NOTICE '[MANAGE_DEP] Восстановление прав доступа (TABLE + VIEWS)...';
        FOR v_grant_record IN SELECT * FROM partition_utils.temp_saved_grants LOOP
            BEGIN
                EXECUTE v_grant_record.grant_command;
                RAISE NOTICE '[MANAGE_DEP] Восстановлено право: %', v_grant_record.grant_command;
            EXCEPTION WHEN OTHERS THEN
                RAISE WARNING '[MANAGE_DEP] Ошибка восстановления прав: %', SQLERRM;
            END;
        END LOOP;

        -- 3. ОЧИСТКА
        DROP TABLE IF EXISTS partition_utils.temp_saved_ddl CASCADE;
        RAISE NOTICE '[MANAGE_DEP] Таблица temp_saved_ddl удалена';
        
        DROP TABLE IF EXISTS partition_utils.temp_saved_grants CASCADE;
        RAISE NOTICE '[MANAGE_DEP] Таблица temp_saved_grants удалена';

        RAISE NOTICE '[MANAGE_DEP] [RESTORE] Завершено.';
    ELSE
        RAISE EXCEPTION '[MANAGE_DEP] Неизвестный режим: %. Допустимые: ''SAVE'', ''RESTORE''', p_operation_mode;
    END IF;

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '[MANAGE_DEP] Ошибка: %', SQLERRM;
END;
$$ EXECUTE ON ANY;
