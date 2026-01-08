CREATE OR REPLACE FUNCTION partition_utils.srv_partition_mng(p_json_data text)
RETURNS void
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_json_array jsonb;
    v_json_element jsonb;

    -- Переменные для парсинга
    v_schema_name text;
    v_table_name text;
    v_partition_column text;
    v_partition_type text;

    v_start_date date;
    v_end_date date;
    v_interval interval;
    v_list_definitions text[];

    v_sub_column text;
    v_sub_type text;
    v_sub_start date;
    v_sub_end date;
    v_sub_interval interval;
    v_sub_list text[];

    v_op_plan jsonb;
    v_sub_part jsonb;
    v_sub_op_plan jsonb;

    proc text := 'srv_partition_mng';
    v_raw_input text;

BEGIN
    RAISE NOTICE '[PARSER] ===== НАЧАЛО ОБРАБОТКИ =====';

    IF p_json_data IS NULL OR p_json_data = '' THEN
        RETURN;
    END IF;

    -- 1. Подготовка входных данных: Превращаем список объектов в валидный JSON-массив
    -- Если входные данные уже массив "[...]", оставляем как есть.
    -- Если это "OBJ, OBJ", оборачиваем в "[OBJ, OBJ]".
    v_raw_input := trim(p_json_data);

    IF left(v_raw_input, 1) <> '[' THEN
        v_raw_input := '[' || v_raw_input || ']';
    END IF;

    -- Попытка распарсить как JSON массив
    BEGIN
        v_json_array := v_raw_input::jsonb;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '[PARSER] Ошибка валидации JSON. Убедитесь, что объекты разделены запятыми. Ошибка: %', SQLERRM;
    END;

    -- 2. ЦИКЛ ПО ВСЕМ ОБЪЕКТАМ В МАССИВЕ
    FOR v_json_element IN SELECT * FROM jsonb_array_elements(v_json_array)
    LOOP
        RAISE NOTICE '[PARSER] ---> Обработка следующего объекта конфигурации...';

        -- Сброс переменных перед каждой итерацией
        v_sub_column := NULL; v_sub_type := NULL; v_sub_start := NULL;
        v_sub_end := NULL; v_sub_interval := NULL; v_sub_list := NULL;

        -- 2.1. Парсинг основного уровня
        v_schema_name := v_json_element->>'schema_name';
        v_table_name := v_json_element->>'table_name';
        v_partition_column := v_json_element->>'partition_column';
        v_partition_type := upper(v_json_element->>'partition_type');
        v_op_plan := v_json_element->'operation_plan';

        RAISE NOTICE '[PARSER] Схема=%, Таблица=%, Тип=%', v_schema_name, v_table_name, v_partition_type;

        -- Парсинг параметров основного уровня
        IF v_partition_type = 'RANGE' THEN
            v_start_date := (v_op_plan->>'start_date')::date;
            v_end_date := (v_op_plan->>'end_date')::date;
            v_interval := (v_op_plan->>'interval')::interval;
            v_list_definitions := NULL;
        ELSIF v_partition_type = 'LIST' THEN
            SELECT array_agg(x) INTO v_list_definitions
            FROM jsonb_array_elements_text(v_op_plan->'definitions') t(x);
            v_start_date := NULL; v_end_date := NULL; v_interval := NULL;
        END IF;

        -- 2.2. Парсинг Субпартиций (если есть)
        IF v_json_element ? 'subpartition' AND (v_json_element->'subpartition') IS NOT NULL THEN
            v_sub_part := v_json_element->'subpartition';
            v_sub_column := v_sub_part->>'partition_column';
            v_sub_type := upper(v_sub_part->>'partition_type');
            v_sub_op_plan := v_sub_part->'operation_plan';

            RAISE NOTICE '[PARSER] Найдена субпартиция: Колонка=%, Тип=%', v_sub_column, v_sub_type;

            IF v_sub_type = 'RANGE' THEN
                v_sub_start := (v_sub_op_plan->>'start_date')::date;
                v_sub_end := (v_sub_op_plan->>'end_date')::date;
                v_sub_interval := (v_sub_op_plan->>'interval')::interval;
            ELSIF v_sub_type = 'LIST' THEN
                SELECT array_agg(x) INTO v_sub_list
                FROM jsonb_array_elements_text(v_sub_op_plan->'definitions') t(x);
            END IF;
        END IF;

        -- 3. Вызов Оркестратора для текущего объекта
        PERFORM partition_utils.proc_orchestrator(
            v_schema_name, v_table_name,
            v_partition_column, v_partition_type,
            v_start_date, v_end_date, v_interval, v_list_definitions,
            v_sub_column, v_sub_type,
            v_sub_start, v_sub_end, v_sub_interval, v_sub_list
        );

        RAISE NOTICE '[PARSER] <--- Объект обработан успешно.';
    END LOOP;

    RAISE NOTICE '[PARSER] ===== ВСЕ ЗАДАЧИ ВЫПОЛНЕНЫ =====';

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '[PARSER] Ошибка: %', SQLERRM;
END;
$$ EXECUTE ON ANY;
