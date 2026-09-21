--
-- Test all the different plan shapes that the planner can generate for
-- DISTINCT queries
--
create table distinct_test (a int, b int, c int) distributed by (a);
insert into distinct_test select g / 1000, g / 2000, g from generate_series(1, 10000) g;
analyze distinct_test;

--
-- With the default cost settings, you get hashed plans
--

-- If the DISTINCT is a superset of the table's distribution keys, the
-- duplicates can be eliminated independently in the segments.
explain select distinct a, b from distinct_test;
select distinct a, b from distinct_test;

-- Otherwise, redistribution is needed
explain select distinct b from distinct_test;
select distinct b from distinct_test;

-- The two-stage aggregation can be disabled with GUC
set gp_enable_preunique = off;
explain select distinct b from distinct_test;
reset gp_enable_preunique;

-- If the input is highly unique already the pre-Unique step is not worthwhile.
-- (Only print count(*) of the result because it returns so many rows)
explain select distinct c from distinct_test;
select count(*) from (
        select distinct c from distinct_test
offset 0) as x;

--
-- Repeat the same tests with sorted Unique plans
--
set enable_hashagg=off;
set optimizer_enable_hashagg=off;

-- If the DISTINCT is a superset of the table's distribution keys, the
-- duplicates can be eliminated independently in the segments.
explain select distinct a, b from distinct_test;
select distinct a, b from distinct_test;

-- Otherwise, redistribution is needed
explain select distinct b from distinct_test;
select distinct b from distinct_test;

-- If the input is highly unique already the pre-Unique step is not worthwhile.
-- (Only print count(*) of the result because it returns so many rows)
explain select distinct c from distinct_test;
select count(*) from (
        select distinct c from distinct_test
offset 0) as x;

--
-- Also test paths where the explicit Sort is not needed
--
create index on distinct_test (a, b);
create index on distinct_test (b);
create index on distinct_test (c);
set random_page_cost=1;


-- If the DISTINCT is a superset of the table's distribution keys, the
-- duplicates can be eliminated independently in the segments.
explain select distinct a, b from distinct_test;
select distinct a, b from distinct_test;

-- Otherwise, redistribution is needed
explain select distinct b from distinct_test;
select distinct b from distinct_test;

-- If the input is highly unique already the pre-Unique step is not worthwhile.
-- (Only print count(*) of the result because it returns so many rows)
explain select distinct c from distinct_test;
select count(*) from (
        select distinct c from distinct_test
offset 0) as x;

CREATE TABLE t_issue_internal_738(
    pram_tp_nm        TEXT,
    proc_nm           TEXT,
    isp_gr_key_id     TEXT,
    isp_atc_nm        TEXT,
    lang_id           TEXT,
    proc_id           TEXT
);

INSERT INTO t_issue_internal_738 (pram_tp_nm, proc_nm, isp_gr_key_id, isp_atc_nm, lang_id, proc_id) VALUES
-- Same para group, different languages
('TypeA', 'ProcessA', 'GROUP001', 'Activity Name A', 'ko-KR', 'trLevel1_1'),
('TypeA', 'ProcessA', 'GROUP001', 'Activity Name A', 'en-US', 'trLevel1_1'),
('TypeA', 'ProcessA', 'GROUP001', 'Activity Name A Extended', 'ko-KR', 'trLevel1_1'),  -- Same group, different activity name

-- Different para groups
('TypeB', 'ProcessB', 'GROUP002', 'Production Process B', 'ko-KR', 'trLevel2_2'),
('TypeC', 'ProcessC', 'GROUP003', 'Quality Control C', 'ko-KR', 'trLevel2_3'),
('TypeD', 'ProcessD', 'GROUP004', 'Assembly Process D', 'ko-KR', 'A5000'),
('TypeE', 'ProcessE', 'GROUP005', 'Testing Process E', 'ko-KR', 'A7000'),
('TypeF', 'ProcessF', 'GROUP006', 'Packaging Process F', 'ko-KR', 'A8000'),
('TypeG', 'ProcessG', 'GROUP007', 'Planning Process G', 'ko-KR', 'P1000'),
('TypeH', 'ProcessH', 'GROUP008', 'Monitoring Process H', 'ko-KR', 'P2000'),

-- Test proc_id not in the list (should be filtered out)
('TypeX', 'ProcessX', 'GROUP009', 'External Process X', 'ko-KR', 'OTHER_ID'),
('TypeY', 'ProcessY', 'GROUP010', 'External Process Y', 'en-US', 'OTHER_ID2'),

-- Test lang_id not 'ko-KR' (should be filtered out)
('TypeZ', 'ProcessZ', 'GROUP011', 'International Process Z', 'en-US', 'trLevel1_1'),
('TypeZ', 'ProcessZ', 'GROUP011', 'International Process Z', 'ja-JP', 'trLevel2_2'),

-- Same para value, different para_name (test max() function)
('TypeSAME', 'ProcessSAME', 'GROUP012', 'Activity Alpha', 'ko-KR', 'P5000'),
('TypeSAME', 'ProcessSAME', 'GROUP012', 'Activity Beta', 'ko-KR', 'P5000'),
('TypeSAME', 'ProcessSAME', 'GROUP012', 'Activity Gamma', 'ko-KR', 'P5000'),

-- Test other values in proc_id list
('TypeI', 'ProcessI', 'GROUP013', 'Inspection Process I', 'ko-KR', 'A9000'),
('TypeJ', 'ProcessJ', 'GROUP014', 'Fabrication Process J', 'ko-KR', 'F4000'),
('TypeK', 'ProcessK', 'GROUP015', 'Processing K', 'ko-KR', 'P6000'),
('TypeL', 'ProcessL', 'GROUP016', 'Logistics Process L', 'ko-KR', 'P9000'),
('TypeM', 'ProcessM', 'GROUP017', 'Maintenance Process M', 'ko-KR', 'trLevel2_4'),
('TypeN', 'ProcessN', 'GROUP018', 'Network Process N', 'ko-KR', 'trLevel2_5'),
('TypeO', 'ProcessO', 'GROUP019', 'Operation Process O', 'ko-KR', 'trLevel2_6'),

-- Additional test cases
('Analysis', 'DataAnalysis', 'GROUP020', 'Statistical Analysis', 'ko-KR', 'A5000'),
('Analysis', 'DataAnalysis', 'GROUP020', 'Statistical Analysis Pro', 'ko-KR', 'A5000'),
('Factory', 'Manufacturing', 'GROUP021', 'Assembly Line', 'ko-KR', 'A7000'),
('Quality', 'QualityCheck', 'GROUP022', 'Final Inspection', 'ko-KR', 'P1000'),
('Quality', 'QualityCheck', 'GROUP022', 'Preliminary Check', 'ko-KR', 'P1000'),
('System', 'SystemProcess', 'GROUP023', 'System Monitoring', 'ko-KR', 'P2000'),
('System', 'SystemProcess', 'GROUP023', 'System Diagnostics', 'ko-KR', 'P2000'),
('Report', 'Reporting', 'GROUP024', 'Daily Report', 'ko-KR', 'F4000'),
('Report', 'Reporting', 'GROUP024', 'Weekly Summary', 'ko-KR', 'F4000');

explain(verbose, costs off)
SELECT para, max(para_name) para_name FROM (
SELECT DISTINCT pram_tp_nm || '' || PROC_NM || '' || ISP_GR_KEY_ID AS para, '[' || proc_nm || '] ' || isp_atc_nm AS para_name
FROM t_issue_internal_738
WHERE 1=1
and lang_id = 'ko-KR'
and proc_id IN('trLevel1_1', 'trLevel2_2', 'trLevel2_3', 'trLevel2_4', 'trLevel2_5', 'trLevel2_6', 'A5000', 'A7000', 'A8000', 'A9000', 'F4000', 'P1000', 'P2000', 'P5000', 'P6000', 'P9000')
) z GROUP BY para;

SELECT para, max(para_name) para_name FROM (
SELECT DISTINCT pram_tp_nm || '' || PROC_NM || '' || ISP_GR_KEY_ID AS para, '[' || proc_nm || '] ' || isp_atc_nm AS para_name
FROM t_issue_internal_738
WHERE 1=1
and lang_id = 'ko-KR'
and proc_id IN('trLevel1_1', 'trLevel2_2', 'trLevel2_3', 'trLevel2_4', 'trLevel2_5', 'trLevel2_6', 'A5000', 'A7000', 'A8000', 'A9000', 'F4000', 'P1000', 'P2000', 'P5000', 'P6000', 'P9000')
) z GROUP BY para;

set gp_eager_two_phase_agg=on;
explain(verbose, costs off)
SELECT para, max(para_name) para_name FROM (
SELECT DISTINCT pram_tp_nm || '' || PROC_NM || '' || ISP_GR_KEY_ID AS para, '[' || proc_nm || '] ' || isp_atc_nm AS para_name
FROM t_issue_internal_738
WHERE 1=1
and lang_id = 'ko-KR'
and proc_id IN('trLevel1_1', 'trLevel2_2', 'trLevel2_3', 'trLevel2_4', 'trLevel2_5', 'trLevel2_6', 'A5000', 'A7000', 'A8000', 'A9000', 'F4000', 'P1000', 'P2000', 'P5000', 'P6000', 'P9000')
) z GROUP BY para;

SELECT para, max(para_name) para_name FROM (
SELECT DISTINCT pram_tp_nm || '' || PROC_NM || '' || ISP_GR_KEY_ID AS para, '[' || proc_nm || '] ' || isp_atc_nm AS para_name
FROM t_issue_internal_738
WHERE 1=1
and lang_id = 'ko-KR'
and proc_id IN('trLevel1_1', 'trLevel2_2', 'trLevel2_3', 'trLevel2_4', 'trLevel2_5', 'trLevel2_6', 'A5000', 'A7000', 'A8000', 'A9000', 'F4000', 'P1000', 'P2000', 'P5000', 'P6000', 'P9000')
) z GROUP BY para;

reset gp_eager_two_phase_agg;
set enable_hashagg = on;
set enable_groupagg = off;
explain(verbose, costs off)
SELECT para, max(para_name) para_name FROM (
SELECT DISTINCT pram_tp_nm || '' || PROC_NM || '' || ISP_GR_KEY_ID AS para, '[' || proc_nm || '] ' || isp_atc_nm AS para_name
FROM t_issue_internal_738
WHERE 1=1
and lang_id = 'ko-KR'
and proc_id IN('trLevel1_1', 'trLevel2_2', 'trLevel2_3', 'trLevel2_4', 'trLevel2_5', 'trLevel2_6', 'A5000', 'A7000', 'A8000', 'A9000', 'F4000', 'P1000', 'P2000', 'P5000', 'P6000', 'P9000')
) z GROUP BY para;
SELECT para, max(para_name) para_name FROM (
SELECT DISTINCT pram_tp_nm || '' || PROC_NM || '' || ISP_GR_KEY_ID AS para, '[' || proc_nm || '] ' || isp_atc_nm AS para_name
FROM t_issue_internal_738
WHERE 1=1
and lang_id = 'ko-KR'
and proc_id IN('trLevel1_1', 'trLevel2_2', 'trLevel2_3', 'trLevel2_4', 'trLevel2_5', 'trLevel2_6', 'A5000', 'A7000', 'A8000', 'A9000', 'F4000', 'P1000', 'P2000', 'P5000', 'P6000', 'P9000')
) z GROUP BY para;

reset enable_hashagg;
reset enable_groupagg;
drop table t_issue_internal_738;
