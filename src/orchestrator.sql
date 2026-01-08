CREATE OR REPLACE FUNCTION partition_utils.proc_orchestrator(
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
VOLATILE
AS $$
DECLARE
    v_preview boolean;
    v_sub_preview boolean;
    v_table_owner text;
    proc text := 'proc_orchestrator';
BEGIN
    RAISE NOTICE '[ORCHESTRATOR] Начало работы...';
    RAISE NOTICE '[ORCHESTRATOR] Основная партиция: %.% (%)', p_schema_name, p_table_name, p_partition_type;

    IF p_subpartition_column IS NOT NULL THEN
        RAISE NOTICE '[ORCHESTRATOR] Субпартиция: % (%)', p_subpartition_column, p_subpartition_type;
    ELSE
        RAISE NOTICE '[ORCHESTRATOR] Субпартиции нет';
    END IF;

    -- 1. Анализ необходимости партицирования
    RAISE NOTICE '[ORCHESTRATOR] >> Вызов proc_analyzer...';
    v_preview := partition_utils.proc_analyzer(
        p_schema_name, p_table_name, p_partition_column, p_partition_type,
        p_start_date, p_end_date, p_interval, p_list_definitions
    );

    IF p_subpartition_column IS NOT NULL THEN
        v_sub_preview := partition_utils.proc_analyzer(
            p_schema_name, p_table_name, p_subpartition_column, p_subpartition_type,
            p_subpartition_start_date, p_subpartition_end_date, p_subpartition_interval, p_subpartition_list_definitions
        );
    END IF;

    RAISE NOTICE '[ORCHESTRATOR] Результат proc_analyzer: %', v_preview;
    IF p_subpartition_column IS NOT NULL THEN
        RAISE NOTICE '[ORCHESTRATOR] Результат proc_analyzer для субпартиции: %', v_sub_preview;
    END IF;

    -- Проверка на валидность ввода
    IF v_sub_preview AND NOT v_preview THEN
        RAISE NOTICE '[ORCHESTRATOR] [ERROR] Неверный ввод. Партиция уже существует, а субпартиции нет';
        RETURN;
    END IF;

    -- 2. Если партицирование нужно - запускаем процесс
    IF v_preview THEN
        -- Получаем владельца (для информации)
        SELECT pg_get_userbyid(c.relowner) INTO v_table_owner
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = p_schema_name AND c.relname = p_table_name;
        
        RAISE NOTICE '[EXECUTER] Текущий владелец таблицы: %', v_table_owner;

        -- А. Сохраняем зависимости
        RAISE NOTICE '[ORCHESTRATOR] >> Сохраняем зависимости (SAVE)...';
        PERFORM partition_utils.proc_dependency_manager(
            'SAVE', p_schema_name, p_table_name
        );

        -- Б. Выполняем перепартицирование
        RAISE NOTICE '[ORCHESTRATOR] >> Вызов proc_executor...';
        PERFORM partition_utils.proc_executor(
            p_schema_name, p_table_name,
            p_partition_column, p_partition_type,
            p_start_date, p_end_date, p_interval, p_list_definitions,
            p_subpartition_column, p_subpartition_type,
            p_subpartition_start_date, p_subpartition_end_date,
            p_subpartition_interval, p_subpartition_list_definitions
        );

        RAISE NOTICE '[ORCHESTRATOR] % партиционирование для таблицы %.% успешно завершено',
            p_partition_type, p_schema_name, p_table_name;

        -- В. Восстанавливаем зависимости
        RAISE NOTICE '[ORCHESTRATOR] >> Восстанавливаем зависимости (RESTORE)...';
        PERFORM partition_utils.proc_dependency_manager(
            'RESTORE', p_schema_name, p_table_name
        );

    ELSE
        RAISE NOTICE '[ORCHESTRATOR] Партиционирование не требуется (preview вернул false)';
    END IF;

    RAISE NOTICE '[ORCHESTRATOR] Завершено';
END;
$$ EXECUTE ON ANY;
