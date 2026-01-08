CREATE OR REPLACE FUNCTION partition_utils.proc_executor(
    p_schema_name text,
    p_table_name text,
    p_partition_column text,
    p_partition_type text,
    p_start_date date,
    p_end_date date,
    p_interval interval,
    p_list_definitions _text,
    p_subpartition_column text,
    p_subpartition_type text,
    p_subpartition_start_date date,
    p_subpartition_end_date date,
    p_subpartition_interval interval,
    p_subpartition_list_definitions _text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE
AS $$
DECLARE
    v_stat_start_dt timestamp := clock_timestamp();

    v_full_table_name TEXT := quote_ident(p_schema_name) || '.' || quote_ident(p_table_name);
    v_temp_table_name TEXT := quote_ident(p_table_name || '_part_tmp');
    v_table_owner TEXT;

    v_table_exists BOOLEAN;
    v_is_partitioned BOOLEAN;
    v_existing_part_type TEXT;

    v_has_default BOOLEAN := FALSE;
    v_default_part_name TEXT;
    v_default_phys_name TEXT; -- Физическое имя таблицы-партиции для SELECT
    v_temp_backup_table TEXT; -- Имя таблицы для бэкапа данных дефолта

    v_curr_date DATE;
    v_next_date DATE;
    v_part_name TEXT;
    v_list_val TEXT;

    v_sql TEXT;
    v_partition_clause TEXT;
    v_subpartition_clause TEXT := '';
    v_partition_spec TEXT := '';
    v_storage_options TEXT;
    v_with_clause TEXT := '';
    v_check_partition_exists BOOLEAN;

    v_existing_partitions RECORD;
    v_first_partition BOOLEAN := TRUE;
    v_range_start TEXT;
    v_range_end TEXT;
    
    proc text := 'proc_executor';

BEGIN
    RAISE NOTICE '[EXECUTER] Старт для %.%', p_schema_name, p_table_name;

    SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = p_schema_name AND tablename = p_table_name) INTO v_table_exists;

    IF v_table_exists THEN
        SELECT EXISTS (SELECT 1 FROM pg_partitions WHERE schemaname = p_schema_name AND tablename = p_table_name) INTO v_is_partitioned;
    ELSE
        v_is_partitioned := FALSE;
    END IF;

    IF v_is_partitioned THEN
        SELECT lower(partitiontype) INTO v_existing_part_type
        FROM pg_partitions
        WHERE schemaname = p_schema_name AND tablename = p_table_name
        LIMIT 1;

        IF v_existing_part_type IS DISTINCT FROM lower(p_partition_type) THEN
            RAISE NOTICE '[EXECUTER] КОНФЛИКТ ТИПОВ: Текущий=%, Запрашиваемый=%. Переход в режим FULL REWRITE.', v_existing_part_type, p_partition_type;
            v_is_partitioned := FALSE;
        END IF;
    END IF;

    -- =========================================================================
    -- ВЕТКА 1: ОБСЛУЖИВАНИЕ (ALTER TABLE)
    -- =========================================================================
    IF v_table_exists AND v_is_partitioned THEN
        RAISE NOTICE '[EXECUTER] Таблица существует и партиционирована корректно. Режим UPDATE (ALTER TABLE).';

        -- Ищем логическое и физическое имя DEFAULT партиции
        SELECT partitionname, partitiontablename INTO v_default_part_name, v_default_phys_name
        FROM pg_partitions
        WHERE schemaname = p_schema_name AND tablename = p_table_name AND partitionisdefault = 't'
        LIMIT 1;

        IF v_default_part_name IS NOT NULL THEN
            v_has_default := TRUE;
        END IF;

        -- А) RANGE (SPLIT работает нормально)
        IF upper(p_partition_type) = 'RANGE' THEN
            v_curr_date := p_start_date;
            WHILE v_curr_date < p_end_date LOOP
                v_next_date := v_curr_date + p_interval;
                v_part_name := 'p_' || to_char(v_curr_date, 'YYYYMMDD');

                SELECT EXISTS (
                    SELECT 1 FROM pg_partitions
                    WHERE schemaname = p_schema_name
                      AND tablename = p_table_name
                      AND (partitionname = v_part_name OR partitionrangestart LIKE '%' || v_curr_date::text || '%')
                ) INTO v_check_partition_exists;

                IF NOT v_check_partition_exists THEN
                    IF v_has_default THEN
                        RAISE NOTICE '[EXECUTER] SPLIT DEFAULT (RANGE) для периода % - %', v_curr_date, v_next_date;
                        v_sql := format('ALTER TABLE %s SPLIT DEFAULT PARTITION START(%L::date) END(%L::date) INTO (PARTITION %I, PARTITION %I)',
                                        v_full_table_name, v_curr_date, v_next_date, v_part_name, v_default_part_name);
                        EXECUTE v_sql;
                    ELSE
                        RAISE NOTICE '[EXECUTER] ADD PARTITION (RANGE) для периода % - %', v_curr_date, v_next_date;
                        v_sql := format('ALTER TABLE %s ADD PARTITION %I START(%L::date) END(%L::date)',
                                        v_full_table_name, v_part_name, v_curr_date, v_next_date);
                        EXECUTE v_sql;
                    END IF;
                ELSE
                    RAISE NOTICE '[EXECUTER] Партиция % уже существует, пропускаем.', v_part_name;
                END IF;
                v_curr_date := v_next_date;
            END LOOP;

        -- Б) LIST (SPLIT не работает в GP 6 -> Используем DROP & RESTORE)
        ELSIF upper(p_partition_type) = 'LIST' THEN
            FOREACH v_list_val IN ARRAY p_list_definitions LOOP
                v_part_name := 'p_' || lower(regexp_replace(v_list_val, '[^a-zA-Z0-9_]', '', 'g'));

                SELECT EXISTS (
                    SELECT 1 FROM pg_partitions
                    WHERE schemaname = p_schema_name
                      AND tablename = p_table_name
                      AND partitionlistvalues LIKE '%' || v_list_val || '%'
                ) INTO v_check_partition_exists;

                IF NOT v_check_partition_exists THEN
                    IF v_has_default THEN
                        RAISE NOTICE '[EXECUTER] Эмуляция SPLIT для LIST (GP 6 limitation) для значения %', v_list_val;

                        -- 1. Эвакуация данных из дефолта
                        v_temp_backup_table := quote_ident(p_table_name || '_def_bak_' || floor(random()*10000)::text);
                        RAISE NOTICE '[EXECUTER] Бэкап данных DEFAULT партиции в %', v_temp_backup_table;

                        -- Создаем temp таблицу и льем туда данные из физической таблицы дефолта
                        v_sql := format('CREATE TEMP TABLE %I ON COMMIT DROP AS SELECT * FROM %I.%I',
                                        v_temp_backup_table, p_schema_name, v_default_phys_name);
                        EXECUTE v_sql;

                        -- 2. Удаление DEFAULT партиции
                        RAISE NOTICE '[EXECUTER] Удаление DEFAULT партиции...';
                        v_sql := format('ALTER TABLE %s DROP DEFAULT PARTITION', v_full_table_name);
                        EXECUTE v_sql;

                        -- 3. Добавление НОВОЙ партиции
                        RAISE NOTICE '[EXECUTER] Добавление новой партиции % для значения %', v_part_name, v_list_val;
                        v_sql := format('ALTER TABLE %s ADD PARTITION %I VALUES(%L)',
                                        v_full_table_name, v_part_name, v_list_val);
                        EXECUTE v_sql;

                        -- 4. Восстановление DEFAULT партиции (пустой)
                        RAISE NOTICE '[EXECUTER] Восстановление DEFAULT партиции %', v_default_part_name;
                        v_sql := format('ALTER TABLE %s ADD DEFAULT PARTITION %I', v_full_table_name, v_default_part_name);
                        EXECUTE v_sql;

                        -- 5. Возврат данных (Ре-дистрибуция)
                        RAISE NOTICE '[EXECUTER] Возврат данных из бэкапа...';
                        v_sql := format('INSERT INTO %s SELECT * FROM %I', v_full_table_name, v_temp_backup_table);
                        EXECUTE v_sql;

                        -- 6. Чистка
                        EXECUTE format('DROP TABLE IF EXISTS %I', v_temp_backup_table);

                        -- Обновляем имя физической партиции дефолта
                        SELECT partitiontablename INTO v_default_phys_name
                        FROM pg_partitions
                        WHERE schemaname = p_schema_name AND tablename = p_table_name AND partitionisdefault = 't'
                        LIMIT 1;
                    ELSE
                        RAISE NOTICE '[EXECUTER] ADD PARTITION (LIST) для значения %', v_list_val;
                        v_sql := format('ALTER TABLE %s ADD PARTITION %I VALUES(%L)',
                                        v_full_table_name, v_part_name, v_list_val);
                        EXECUTE v_sql;
                    END IF;
                ELSE
                    RAISE NOTICE '[EXECUTER] Значение % уже есть в партициях, пропускаем.', v_list_val;
                END IF;
            END LOOP;
        END IF;

        RETURN;
    END IF;

    -- =========================================================================
    -- ВЕТКА 2: МИГРАЦИЯ / СОЗДАНИЕ (FULL REWRITE)
    -- =========================================================================
    RAISE NOTICE '[EXECUTER] Режим FULL REWRITE (Создание/Миграция/Смена типа).';

    IF v_table_exists THEN
        -- Запоминаем владельца (Owner)
        SELECT pg_get_userbyid(c.relowner) INTO v_table_owner
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = p_schema_name AND c.relname = p_table_name;
        
        RAISE NOTICE '[EXECUTER] Текущий владелец таблицы: %', v_table_owner;

        SELECT array_to_string(reloptions, ',') INTO v_storage_options
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = p_schema_name AND c.relname = p_table_name;
        
        IF v_storage_options IS NOT NULL AND length(v_storage_options) > 0 THEN
            v_with_clause := 'WITH (' || v_storage_options || ')';
        END IF;
    END IF;

    -- 3. Subpartitions
    IF p_subpartition_column IS NOT NULL THEN
        -- Блок LIST
        IF upper(p_subpartition_type) = 'LIST' THEN
            v_subpartition_clause := format('SUBPARTITION BY LIST(%I) SUBPARTITION TEMPLATE (', p_subpartition_column);
            FOR i IN 1..array_length(p_subpartition_list_definitions, 1) LOOP
                v_list_val := p_subpartition_list_definitions[i];
                v_subpartition_clause := v_subpartition_clause || format('SUBPARTITION sp_%s VALUES(%L)',
                                          regexp_replace(v_list_val, '[^a-zA-Z0-9_]', '', 'g'), v_list_val);
                IF i < array_length(p_subpartition_list_definitions, 1) THEN v_subpartition_clause := v_subpartition_clause || ', '; END IF;
            END LOOP;
            v_subpartition_clause := v_subpartition_clause || ', DEFAULT SUBPARTITION other_sub )';

        -- Блок RANGE
        ELSIF upper(p_subpartition_type) = 'RANGE' THEN
            v_subpartition_clause := format('SUBPARTITION BY RANGE(%I) SUBPARTITION TEMPLATE ( START(%L::date) END(%L::date) EVERY(%L::interval), DEFAULT SUBPARTITION other_sub )',
                                            p_subpartition_column, p_subpartition_start_date, p_subpartition_end_date, p_subpartition_interval);
        END IF;
    END IF;

    -- 4. Partition Clause
    IF upper(p_partition_type) = 'RANGE' THEN
        v_partition_clause := format('PARTITION BY RANGE(%I)', p_partition_column);
    ELSIF upper(p_partition_type) = 'LIST' THEN
        v_partition_clause := format('PARTITION BY LIST(%I)', p_partition_column);
    END IF;

    -- 5. Partitions Generation
    v_partition_spec := '(';
    v_first_partition := TRUE;

    -- А) Сохраняем существующие партиции
    IF v_table_exists AND v_is_partitioned THEN
        FOR v_existing_partitions IN
            SELECT partitionname, partitionrangestart, partitionrangeend, partitionlistvalues
            FROM pg_partitions
            WHERE schemaname = p_schema_name AND tablename = p_table_name
              AND partitionlevel = 0 AND NOT partitionisdefault
            ORDER BY partitionname
        LOOP
            IF NOT v_first_partition THEN v_partition_spec := v_partition_spec || ', '; END IF;

            IF upper(p_partition_type) = 'RANGE' AND v_existing_partitions.partitionrangestart IS NOT NULL THEN
                v_range_start := split_part(v_existing_partitions.partitionrangestart, '::', 1);
                v_range_start := trim(both '''' from v_range_start);
                v_range_end := split_part(v_existing_partitions.partitionrangeend, '::', 1);
                v_range_end := trim(both '''' from v_range_end);
                v_partition_spec := v_partition_spec || format('PARTITION %I START (%L) INCLUSIVE END (%L) EXCLUSIVE',
                                    v_existing_partitions.partitionname, v_range_start, v_range_end);

            ELSIF upper(p_partition_type) = 'LIST' AND v_existing_partitions.partitionlistvalues IS NOT NULL THEN
                v_list_val := v_existing_partitions.partitionlistvalues;
                v_list_val := regexp_replace(v_list_val, '::[a-zA-Z0-9_ ]+', '', 'g');
                v_list_val := trim(both '{}''' from v_list_val);
                v_partition_spec := v_partition_spec || format('PARTITION %I VALUES (%L)', v_existing_partitions.partitionname, v_list_val);
            END IF;

            v_first_partition := FALSE;
        END LOOP;
    END IF;

    -- Б) Новые партиции
    IF upper(p_partition_type) = 'RANGE' THEN
        v_curr_date := p_start_date;
        WHILE v_curr_date < p_end_date LOOP
            v_next_date := v_curr_date + p_interval;
            v_part_name := 'p_' || to_char(v_curr_date, 'YYYYMMDD');
            IF NOT v_first_partition THEN v_partition_spec := v_partition_spec || ', '; END IF;
            v_partition_spec := v_partition_spec || format('PARTITION %I START (%L::date) END (%L::date)',
                                                            v_part_name, v_curr_date, v_next_date);
            v_first_partition := FALSE;
            v_curr_date := v_next_date;
        END LOOP;
    ELSIF upper(p_partition_type) = 'LIST' THEN
        FOREACH v_list_val IN ARRAY p_list_definitions LOOP
            v_part_name := 'p_' || lower(regexp_replace(v_list_val, '[^a-zA-Z0-9_]', '', 'g'));
            IF NOT v_first_partition THEN v_partition_spec := v_partition_spec || ', '; END IF;
            v_partition_spec := v_partition_spec || format('PARTITION %I VALUES (%L)', v_part_name, v_list_val);
            v_first_partition := FALSE;
        END LOOP;
    END IF;

    IF NOT v_first_partition THEN v_partition_spec := v_partition_spec || ', '; END IF;
    v_partition_spec := v_partition_spec || 'DEFAULT PARTITION other';
    v_partition_spec := v_partition_spec || ')';

    -- 6. Execute
    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', p_schema_name, v_temp_table_name);
    
    v_sql := format(
        'CREATE TABLE %I.%I (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES) %s %s %s %s',
        p_schema_name, v_temp_table_name, v_full_table_name,
        v_with_clause,
        v_partition_clause, v_subpartition_clause, v_partition_spec
    );
    
    RAISE NOTICE '[EXECUTER] SQL Create: %', v_sql;
    EXECUTE v_sql;

    IF v_table_exists THEN
        RAISE NOTICE '[EXECUTER] Переливка данных...';
        EXECUTE format('INSERT INTO %I.%I SELECT * FROM %s', p_schema_name, v_temp_table_name, v_full_table_name);
        
        RAISE NOTICE '[EXECUTER] Удаление старой таблицы...';
        EXECUTE format('DROP TABLE %s CASCADE', v_full_table_name);
    END IF;

    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', p_schema_name, v_temp_table_name, p_table_name);
    
    -- Восстановление владельца
    IF v_table_owner IS NOT NULL THEN
        RAISE NOTICE '[EXECUTER] Восстановление владельца: %', v_table_owner;
        EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', p_schema_name, p_table_name, v_table_owner);
    END IF;

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '[EXECUTER] Ошибка: %', SQLERRM;
END;
$$ EXECUTE ON ANY;
