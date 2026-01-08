-- ============================================================================
-- AUTOMATED TEST SUITE: PARTITION MANAGER
-- ============================================================================

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'test_viewer') THEN
    CREATE ROLE test_viewer;
  END IF;
END
$$;

\echo '=== STARTING TESTS ==='

-- ============================================================================
-- TEST 1: MIGRATION
-- ============================================================================
\echo '>>> TEST 1: Migration...'

DROP TABLE IF EXISTS public.test_migration CASCADE;
CREATE TABLE public.test_migration (id int, sale_date date, val text) DISTRIBUTED BY (id);
INSERT INTO public.test_migration VALUES (1, '2024-01-15', 'Jan'), (2, '2024-02-20', 'Feb'), (3, '2024-05-01', 'Future');

-- JSON В ОДНУ СТРОКУ!
SELECT partition_utils.srv_partition_mng('[{"schema_name":"public","table_name":"test_migration","partition_column":"sale_date","partition_type":"range","operation_plan":{"start_date":"2024-01-01","end_date":"2024-03-01","interval":"1 month"}}]');

-- CHECK
DO $$
DECLARE
    v_cnt int;
    v_is_part boolean;
BEGIN
    SELECT count(*) INTO v_cnt FROM public.test_migration;
    IF v_cnt <> 3 THEN RAISE EXCEPTION 'FAIL 1.1: Data lost!'; END IF;
    SELECT EXISTS(SELECT 1 FROM pg_partitions WHERE tablename = 'test_migration') INTO v_is_part;
    IF NOT v_is_part THEN RAISE EXCEPTION 'FAIL 1.2: Not partitioned!'; END IF;
    RAISE NOTICE '[PASS] Migration test passed.';
END $$;

-- ============================================================================
-- TEST 2: EXPANSION
-- ============================================================================
\echo '>>> TEST 2: Expansion...'

-- JSON В ОДНУ СТРОКУ!
SELECT partition_utils.srv_partition_mng('[{"schema_name":"public","table_name":"test_migration","partition_column":"sale_date","partition_type":"range","operation_plan":{"start_date":"2024-03-01","end_date":"2024-04-01","interval":"1 month"}}]');

-- CHECK
DO $$
DECLARE
    v_has_april boolean;
BEGIN
    SELECT EXISTS (SELECT 1 FROM pg_partitions WHERE tablename = 'test_migration' AND partitionrangestart LIKE '%2024-03-01%') INTO v_has_april;
    IF NOT v_has_april THEN RAISE EXCEPTION 'FAIL 2.1: April partition missing!'; END IF;
    RAISE NOTICE '[PASS] Expansion test passed.';
END $$;

-- ============================================================================
-- TEST 3: LIST
-- ============================================================================
\echo '>>> TEST 3: List...'

DROP TABLE IF EXISTS public.test_list CASCADE;
CREATE TABLE public.test_list (id int, region text) DISTRIBUTED BY (id) PARTITION BY LIST (region) (PARTITION p_moscow VALUES ('Moscow'), DEFAULT PARTITION other);
INSERT INTO public.test_list VALUES (1, 'Moscow'), (2, 'Spb');

-- JSON В ОДНУ СТРОКУ!
SELECT partition_utils.srv_partition_mng('[{"schema_name":"public","table_name":"test_list","partition_column":"region","partition_type":"list","operation_plan":{"definitions":["Spb"]}}]');

-- CHECK
DO $$
DECLARE
    v_cnt int;
    v_has_spb boolean;
BEGIN
    SELECT count(*) INTO v_cnt FROM public.test_list;
    IF v_cnt <> 2 THEN RAISE EXCEPTION 'FAIL 3.1: Data lost!'; END IF;
    SELECT EXISTS (SELECT 1 FROM pg_partitions WHERE tablename = 'test_list' AND partitionlistvalues LIKE '%Spb%') INTO v_has_spb;
    IF NOT v_has_spb THEN RAISE EXCEPTION 'FAIL 3.2: Spb partition missing!'; END IF;
    RAISE NOTICE '[PASS] List test passed.';
END $$;

-- ============================================================================
-- TEST 4: DEPS
-- ============================================================================
\echo '>>> TEST 4: Dependencies...'

DROP TABLE IF EXISTS public.test_deps CASCADE;
CREATE TABLE public.test_deps (id int, dt date) DISTRIBUTED BY (id);
CREATE VIEW public.v_test_deps AS SELECT * FROM public.test_deps;
GRANT SELECT ON public.v_test_deps TO test_viewer;

-- JSON В ОДНУ СТРОКУ!
SELECT partition_utils.srv_partition_mng('[{"schema_name":"public","table_name":"test_deps","partition_column":"dt","partition_type":"range","operation_plan":{"start_date":"2024-01-01","end_date":"2024-02-01","interval":"1 month"}}]');

-- CHECK
DO $$
DECLARE
    v_view boolean;
    v_grant boolean;
BEGIN
    SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = 'v_test_deps') INTO v_view;
    IF NOT v_view THEN RAISE EXCEPTION 'FAIL 4.1: View killed!'; END IF;
    SELECT EXISTS(SELECT 1 FROM information_schema.table_privileges WHERE table_name = 'v_test_deps' AND grantee = 'test_viewer') INTO v_grant;
    IF NOT v_grant THEN RAISE EXCEPTION 'FAIL 4.2: Grant lost!'; END IF;
    RAISE NOTICE '[PASS] Dependency test passed.';
END $$;

-- CLEANUP
DROP TABLE IF EXISTS public.test_migration CASCADE;
DROP TABLE IF EXISTS public.test_list CASCADE;
DROP TABLE IF EXISTS public.test_deps CASCADE;
DROP ROLE IF EXISTS test_viewer;

\echo '=== ALL TESTS PASSED SUCCESSFULLY ==='
