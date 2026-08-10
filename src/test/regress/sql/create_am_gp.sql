-- Cloudberry specific access method tests, in addition to what's
-- covered by upstream create_am.sql tests

\set HIDE_TABLEAM off

create access method ao_row_testam type table handler ao_row_tableam_handler;
create access method ao_col_testam type table handler ao_column_tableam_handler;
create access method heap_testam type table handler heap_tableam_handler;

select amname, amhandler, amtype from pg_am where amname like '%_testam';

create table create_am_gp_ao1 (a int, b int) using ao_row_testam distributed by (a);
\d+ create_am_gp_ao1

create table create_am_gp_ao2 (a int, b int) using ao_row_testam with (compresstype=zlib) distributed by (a);
\d+ create_am_gp_ao2

-- Should fail
create table create_am_gp_ao3 (a int, b int) using ao_row_testam with (compresstype=rle_type) distributed by (a);
create table create_am_gp_ao3 (a int, b int) using heap_testam with (compresstype=rle_type) distributed by (a);

create table create_am_gp_ao3 (a int, b int) using ao_col_testam with (compresstype=rle_type) distributed by (a);
\d+ create_am_gp_ao3

-- Should fail because encoding clause is not supported by the tableam
create table create_am_gp_ao4(a int, b int encoding (compresstype=zlib)) using ao_row_testam distributed by (a);

set gp_default_storage_options='compresstype=rle_type';

create table create_am_gp_heap(a int, b int) using heap_testam distributed by (a);
-- should not have compresstype parameter
\d+ create_am_gp_heap

-- Should fail because the default compresstype configured above is
-- not supported by this tableam
create table create_am_gp_ao5(a int, b int) using ao_row_testam distributed by (a);
create table create_am_gp_ao6(a int, b int) using ao_row_testam with (compresstype=zlib) distributed by (a);
\d+ create_am_gp_ao6

create table create_am_gp_ao7(a int, b int encoding (compresstype=zlib)) using ao_col_testam distributed by (a);
\d+ create_am_gp_ao7

-- Exercise AOCS / AO row internal sanity asserts on tables whose AM is a
-- user-defined AM created with the canonical handler (ao_col_testam /
-- ao_row_testam above).  Such tables dispatch into the AO/AOCS code path
-- via rd_amhandler, but pg_class.relam is the derived AM's OID, not
-- AO_{ROW,COLUMN}_TABLE_AM_OID.  Before the rel.h fix, the
-- RelationStorageIs{AoRows,AoCols} macros were OID-strict and the
-- internal asserts (aocssegfiles.c:{329,519,...}, aocs_compaction.c, ...)
-- aborted the backend on VACUUM / VACUUM FREEZE / etc.  pg_upgrade's
-- post-restore VACUUM FREEZE on every DB hit this and broke `make check`.
insert into create_am_gp_ao3 select i, i * 2 from generate_series(1, 100) i;
insert into create_am_gp_ao7 select i, i * 2 from generate_series(1, 100) i;
insert into create_am_gp_ao1 select i, i * 2 from generate_series(1, 100) i;
insert into create_am_gp_ao2 select i, i * 2 from generate_series(1, 100) i;
vacuum            create_am_gp_ao3;
vacuum freeze     create_am_gp_ao3;
vacuum analyze    create_am_gp_ao7;
vacuum            create_am_gp_ao1;
vacuum freeze     create_am_gp_ao1;
vacuum analyze    create_am_gp_ao2;
select 'ao_col_testam pass' as result, count(*) from create_am_gp_ao3;
select 'ao_col_testam pass' as result, count(*) from create_am_gp_ao7;
select 'ao_row_testam pass' as result, count(*) from create_am_gp_ao1;
select 'ao_row_testam pass' as result, count(*) from create_am_gp_ao2;

-- Cleanup
drop table if exists create_am_gp_ao1;
drop table if exists create_am_gp_ao2;
drop table if exists create_am_gp_ao3;
drop table if exists create_am_gp_ao4;
drop table if exists create_am_gp_ao5;
drop table if exists create_am_gp_ao6;
drop table if exists create_am_gp_ao7;
drop table if exists create_am_gp_heap;
