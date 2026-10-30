-- Regression test for lightning issue #134: substr() with a constant subject
-- and column offsets has to fall back to the row executor instead of failing.
--
-- This reaches the same place as issue #125 -- substr(text, int, int) is
-- another of the four OIDs translated to Arrow's utf8_slice_codeunits, whose
-- start and stop are fixed in GArrowSliceOptions when the plan is built -- but
-- through a different entry point: here it is the subject that is constant and
-- the two offsets that come from columns, which is how the original report
-- found it (substr('UzLI', s2.c0, s4.c1) over a join).
--
-- Before the fix the planner accepted the shape and
-- utf8_slice_codeunits_expression() raised
--     ERROR:  unrecognized node type: 156 (execMain.c:1873)
-- with the plan already running.
--
-- Coverage:
--   contrib/vectorization/pg_vec.dat
--     (procoid 936/937/877/883 -> check_slice_args)
--   contrib/vectorization/src/backend/utils/arrow/fmgr.c
--     (check_slice_args)

create schema vec_substr134;
set search_path = vec_substr134, public;
set vector.enable_vectorization = on;

create table s2 (c0 int) using ao_column distributed by (c0);
create table s4 (c1 int) using ao_column distributed by (c1);
insert into s2 values (1), (2), (3);
insert into s4 values (2), (4);
analyze s2;
analyze s4;

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

-- The reported shape, over a join: constant subject, both offsets are columns.
select is_vectorized('select substr(''UzLIabcd'', s2.c0, s4.c1) from s2, s4');
select s2.c0, s4.c1, substr('UzLIabcd', s2.c0, s4.c1)
from s2, s4 order by 1, 2;

-- Only the length from a column.
select is_vectorized('select substr(''UzLIabcd'', 2, s4.c1) from s4');
select c1, substr('UzLIabcd', 2, c1) from s4 order by 1;

-- Only the start from a column.
select is_vectorized('select substr(''UzLIabcd'', s2.c0) from s2');
select c0, substr('UzLIabcd', c0) from s2 order by 1;

-- Control: a constant subject with constant offsets is folded by the planner
-- long before the vectorizer sees it, and the plan still vectorizes.
select is_vectorized('select substr(''UzLIabcd'', 2, 3) from s2');
select c0, substr('UzLIabcd', 2, 3) from s2 order by 1;

-- Control: the fallback answers match the row executor exactly.
set vector.enable_vectorization = off;
create table ref as
  select s2.c0, s4.c1, substr('UzLIabcd', s2.c0, s4.c1) r from s2, s4
  distributed by (c0);
set vector.enable_vectorization = on;
select count(*) as mismatches
from ((select s2.c0, s4.c1, substr('UzLIabcd', s2.c0, s4.c1) r from s2, s4)
      except all
      (select c0, c1, r from ref)) d;

drop schema vec_substr134 cascade;
