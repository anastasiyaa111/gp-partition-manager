CREATE OR REPLACE FUNCTION partition_utils.proc_analyzer(
    p_schema_name text,
    p_table_name text,
    p_partition_column text,
    p_partition_type text,
    p_start_date date,
    p_end_date date,
    p_interval interval,
    p_list_definitions _text
)
RETURNS bool
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE
AS $$
DECLARE
    v_stat_start_dt timestamp := clock_timestamp();
    v_is_partitioned BOOLEAN;
    v_existing_part_type TEXT;

    -- Переменные для проверки интервала (RANGE)
    v_check_start_date date;
    v_check_end_date date;
    v_min_partition_date date;
    v_max_partition_date date;

    -- Переменные для LIST
    v_partition_values text[];
    v_clean_values text[];
    v_all_values_exist boolean := true;
    v_current_value text;
    v_existing_value text;
    v_raw_value text;
    
    proc text := 'proc_analyzer';

BEGIN
    RAISE NOTICE '[PREVIEW] Начало проверки таблицы %.%', p_schema_name, p_table_name;

    -- 1. Проверка существования таблицы
    IF NOT EXISTS (
        SELECT 1 FROM pg_tables
        WHERE tablename = p_table_name
        AND schemaname = p_schema_name
    ) THEN
        RAISE NOTICE '[PREVIEW] Таблица "%.%" не существует', p_schema_name, p_table_name;
        RETURN FALSE;
    END IF;

    -- 2. Проверка существования колонки
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = p_table_name AND table_schema = p_schema_name
        AND column_name = p_partition_column
    ) THEN
        RAISE NOTICE '[PREVIEW] Колонка "%" в таблице "%.%" не существует', p_partition_column, p_schema_name, p_table_name;
        RETURN FALSE;
    END IF;

    -- 3. Проверка статуса партиционирования
    SELECT EXISTS (
        SELECT 1 FROM pg_partitions
        WHERE schemaname = p_schema_name
        AND tablename = p_table_name
    ) INTO v_is_partitioned;

    IF NOT v_is_partitioned THEN
        RAISE NOTICE '[PREVIEW] Таблица не партиционирована. Возвращаем true.';
        RETURN TRUE;
    END IF;

    -- 4. Проверка соответствия типа (RANGE vs LIST)
    SELECT lower(partitiontype) INTO v_existing_part_type
    FROM pg_partitions
    WHERE schemaname = p_schema_name
    AND tablename = p_table_name
    LIMIT 1;

    IF v_existing_part_type IS DISTINCT FROM lower(p_partition_type) THEN
        RAISE NOTICE '[PREVIEW] ОШИБКА ТИПОВ: В базе %, в запросе %. Смена типа запрещена. Пропускаем.',
            upper(v_existing_part_type),
            upper(p_partition_type);
        RETURN FALSE;
    END IF;

    -- =========================================================================================
    -- ЛОГИКА RANGE
    -- =========================================================================================
    IF upper(p_partition_type) = 'RANGE' THEN

        -- ПРОВЕРКА СООТВЕТСТВИЯ ИНТЕРВАЛА
        -- Берем одну любую НЕ дефолтную партицию, чтобы проверить её длину
        SELECT
            (regexp_replace(partitionrangestart, '^.*''(\d{4}-\d{2}-\d{2}).*$', '\1'))::date,
            (regexp_replace(partitionrangeend, '^.*''(\d{4}-\d{2}-\d{2}).*$', '\1'))::date
        INTO v_check_start_date, v_check_end_date
        FROM pg_partitions
        WHERE schemaname = p_schema_name
          AND tablename = p_table_name
          AND partitiontype = 'range'
          AND partitionlevel = 0
          AND NOT partitionisdefault
        LIMIT 1;

        -- Если нашли хоть одну нормальную партицию, проверяем её длину
        IF v_check_start_date IS NOT NULL AND v_check_end_date IS NOT NULL THEN
            -- Если к старту старой партиции прибавить НОВЫЙ интервал,
            -- мы должны получить конец старой партиции. Иначе интервалы разные.
            IF (v_check_start_date + p_interval) <> v_check_end_date THEN
                RAISE NOTICE '[PREVIEW] ОШИБКА ИНТЕРВАЛА: Текущая разбивка таблицы не соответствует запрошенному интервалу "%". Работа невозможна.', p_interval;
                RETURN FALSE; -- Прерываем работу
            END IF;
        END IF;

        -- Стандартная проверка границ
        SELECT
            MIN(CASE
                WHEN partitionrangestart IS NOT NULL AND partitionrangestart != '' 
                THEN (regexp_replace(partitionrangestart, '^.*(''\d{4}-\d{2}-\d{2}'').*$', '\1'))::date
                ELSE NULL 
            END),
            MAX(CASE
                WHEN partitionrangeend IS NOT NULL AND partitionrangeend != '' AND partitionrangeend < '9999-01-01' 
                THEN (regexp_replace(partitionrangeend, '^.*(''\d{4}-\d{2}-\d{2}'').*$', '\1'))::date
                ELSE NULL 
            END)
        INTO v_min_partition_date, v_max_partition_date
        FROM pg_partitions
        WHERE schemaname = p_schema_name
          AND tablename = p_table_name 
          AND partitiontype = 'range'
          AND partitionrangestart IS NOT NULL 
          AND partitionrangeend IS NOT NULL;

        RAISE NOTICE '[PREVIEW] RANGE: min=%, max=%, запрос: start=%, end=%',
            v_min_partition_date, v_max_partition_date, p_start_date, p_end_date;

        IF (v_min_partition_date IS NOT NULL AND p_start_date <= v_min_partition_date - p_interval::interval)
           OR
           (v_max_partition_date IS NOT NULL AND p_end_date >= v_max_partition_date + p_interval::interval)
        THEN
            RAISE NOTICE '[PREVIEW] Диапазон выходит за границы. Возвращаем true.';
            RETURN TRUE;
        END IF;

    -- =========================================================================================
    -- ЛОГИКА LIST
    -- =========================================================================================
    ELSIF upper(p_partition_type) = 'LIST' THEN
        SELECT ARRAY_AGG(DISTINCT partitionlistvalues)
        INTO v_partition_values
        FROM pg_partitions
        WHERE schemaname = p_schema_name
          AND tablename = p_table_name 
          AND partitiontype = 'list'
          AND partitionlistvalues IS NOT NULL;

        RAISE NOTICE '[PREVIEW] LIST: существующие=%, переданные=%',
            v_partition_values, p_list_definitions;

        v_clean_values := '{}'::text[];

        IF v_partition_values IS NOT NULL THEN
            FOREACH v_raw_value IN ARRAY v_partition_values LOOP
                v_existing_value := regexp_replace(v_raw_value, '::[a-zA-Z0-9_ ]+', '', 'g');
                v_existing_value := trim(both '{}''' from v_existing_value);
                v_clean_values := array_append(v_clean_values, v_existing_value);
            END LOOP;
        END IF;

        v_all_values_exist := true;

        FOREACH v_current_value IN ARRAY p_list_definitions LOOP
            IF NOT (v_current_value = ANY(v_clean_values)) THEN
                RAISE NOTICE '[PREVIEW] Значение "%" не найдено', v_current_value;
                v_all_values_exist := false;
                EXIT;
            ELSE
                RAISE NOTICE '[PREVIEW] Значение "%" уже существует', v_current_value;
            END IF;
        END LOOP;

        IF v_all_values_exist THEN
            RAISE NOTICE '[PREVIEW] Все значения существуют. Возвращаем false.';
            RETURN FALSE;
        ELSE
            RAISE NOTICE '[PREVIEW] Не все значения существуют. Возвращаем true.';
            RETURN TRUE;
        END IF;
    END IF;

    RAISE NOTICE '[PREVIEW] Возвращаем false';
    RETURN FALSE;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION '[PREVIEW] Ошибка: %', SQLERRM;
END;
$$ EXECUTE ON ANY;
