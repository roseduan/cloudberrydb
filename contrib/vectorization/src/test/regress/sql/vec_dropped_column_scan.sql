-- Regression test for lightning issue #12: a relation that has a dropped
-- column must still be scannable with vectorization on.
--
--     create table t1(a int, b int) using pax;
--     alter table t1 drop column b;
--     select * from t1;
--     ERROR:  cannot convert PG type 0 to arrow data type (transfuncs.c:250)
--
-- A dropped attribute keeps its pg_attribute row, with atttypid reset to 0.
-- PGTypeToArrow() has no case for type 0, so building the scan's Arrow schema
-- from the whole tuple descriptor raised the error above.  Dropped attributes
-- are now left out of the vectorized projection: the scan stays vectorized and
-- returns the surviving columns.
--
-- The plan is inspected through a wrapper that returns only a boolean,
-- because a raw EXPLAIN embeds the segment count of whichever cluster runs
-- the test.
--
-- Coverage:
--   contrib/vectorization/src/backend/utils/arrow/transfuncs.c  (PGTypeToArrow)
--   contrib/vectorization/src/backend/optimizer/planner_vec.c   (is_type_vectorable)

create schema vec_drop12;
set search_path = vec_drop12, public;
set vector.enable_vectorization = on;

create function is_vectorized(q text) returns boolean as $$
declare
  line text;
begin
  for line in execute 'explain (costs off) ' || q loop
    if line like '%Vec %' then
      return true;
    end if;
  end loop;
  return false;
end;
$$ language plpgsql;

-- The exact shape from the report: empty relation, trailing column dropped.
create table t_pax (a int, b int) using pax distributed by (a);
alter table t_pax drop column b;
select is_vectorized('select * from t_pax');
select * from t_pax;

-- With rows, and with the dropped column in the middle.
create table t2_pax (a int, b int, c text) using pax distributed by (a);
insert into t2_pax select i, i * 2, 'x' || i from generate_series(1, 5) i;
alter table t2_pax drop column b;
analyze t2_pax;
select is_vectorized('select a, c from t2_pax');
select a, c from t2_pax order by a;

-- The same two shapes on ao_column.
create table t_aoco (a int, b int) using ao_column distributed by (a);
alter table t_aoco drop column b;
select is_vectorized('select * from t_aoco');
select * from t_aoco;

create table t2_aoco (a int, b int, c text) using ao_column distributed by (a);
insert into t2_aoco select i, i * 2, 'x' || i from generate_series(1, 5) i;
alter table t2_aoco drop column b;
analyze t2_aoco;
select is_vectorized('select a, c from t2_aoco');
select a, c from t2_aoco order by a;

-- Dropping a column must not change what the surviving columns hold.
select count(*) as mismatches
from ((select a, c from t2_pax) except all (select a, c from t2_aoco)) d;

-- A column dropped after the data was written is read back the same way.
create table t3_aoco (a int, b text, c text) using ao_column distributed by (a);
insert into t3_aoco select i, 'drop' || i, 'keep' || i from generate_series(1, 5) i;
alter table t3_aoco drop column b;
analyze t3_aoco;
select is_vectorized('select a, c from t3_aoco');
select a, c from t3_aoco order by a;

set client_min_messages = warning;
drop schema vec_drop12 cascade;
