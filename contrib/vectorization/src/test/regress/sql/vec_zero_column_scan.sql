-- Regression test for lightning issue #13: scanning a relation that has no
-- columns at all, with vectorization on.
--
--     create table t0() using pax;
--     select * from t0;
--     WARNING:  problem in alloc set MessageContext: detected write past chunk end
--
-- A zero-column relation reaches the planner with an attribute whose type is
-- 0, which has no Arrow equivalent.  The planner now sees that and declines to
-- vectorize the plan -- with vector.print_fallback_log on it logs
--     Fallback to non-vectorization: type 0 not support
-- -- so the scan runs in the row executor and the corruption warning is gone.
--
-- The ao_column half of the same report is issue #37, which rejects a
-- zero-column column-oriented AO relation at creation time; only the pax and
-- heap shapes are exercised here.
--
-- The plan is inspected through a wrapper that returns only a boolean,
-- because a raw EXPLAIN embeds the segment count of whichever cluster runs
-- the test.
--
-- Coverage:
--   contrib/vectorization/src/backend/optimizer/planner_vec.c  (is_type_vectorable)

create schema vec_zero13;
set search_path = vec_zero13, public;
set vector.enable_vectorization = on;
set client_min_messages = warning;

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

-- Control: heap has always handled this.
create table z_heap ();
select count(*) from z_heap;
insert into z_heap default values;
select count(*) from z_heap;

-- The shape from the report.  It must answer, and it must answer from the row
-- executor -- there is no column for Arrow to carry.
create table z_pax () using pax;
select is_vectorized('select * from z_pax');
select count(*) from z_pax;
select * from z_pax;

insert into z_pax default values;
insert into z_pax default values;
select count(*) from z_pax;
select * from z_pax;

-- A relation whose every column was dropped ends up in the same place.
create table z_dropped (a int) using pax distributed by (a);
insert into z_dropped select generate_series(1, 3);
alter table z_dropped drop column a;
analyze z_dropped;
select is_vectorized('select * from z_dropped');
select count(*) from z_dropped;

-- Neighbouring one-column relations must stay vectorized: the fallback is for
-- the zero-column case only.
create table nz_pax (a int) using pax distributed by (a);
insert into nz_pax select generate_series(1, 3);
analyze nz_pax;
select is_vectorized('select * from nz_pax');
select a from nz_pax order by a;

drop schema vec_zero13 cascade;
