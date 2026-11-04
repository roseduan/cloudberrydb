-- Regression test for lightning issue #124: REPLACE() on a column that is not
-- a bare Var, and EXTRACT() of a unit Arrow has no mapping for, both have to
-- fall back to the row executor instead of failing.
--
-- The report's query contained a TO_CHAR() that already dragged the whole plan
-- back to row mode, so it returned the right answer and merely lost
-- vectorization; the two hard errors underneath only show up when each is run
-- on its own.
--
--   replace(text, text, text) is translated to Arrow's replace_substring.  The
--   pattern and the replacement travel in GArrowReplaceSubstringOptions, so
--   both have to be constants, and replace_expression() takes the subject
--   straight out of a Var.  A varchar or char(n) column reaches a text
--   function under a RelabelType, so it is not a bare Var:
--       ERROR:  unrecognized node type: 176 (execMain.c:2045)
--
--   EXTRACT(<unit> FROM <value>) is translated by extract_expression(), which
--   maps eleven units to Arrow temporal component functions.  dow, doy, week,
--   quarter, epoch and the rest reach the switch's default:
--       ERROR:  Failed to call extract type (31)
--
-- Coverage:
--   contrib/vectorization/pg_vec.dat
--     (procoid 2087 -> check_replace_args; 6199/6200/6202/6204 -> check_extract_args)
--   contrib/vectorization/src/backend/utils/arrow/fmgr.c
--     (check_replace_args, check_extract_args)

create schema vec_replace124;
set search_path = vec_replace124, public;
set vector.enable_vectorization = on;

create table t (id int, a text, v varchar(20), c char(10), d date, ts timestamp)
  using ao_column distributed by (id);
insert into t values
  (1, 'abcdefgh', 'abcdefgh', 'bcdefg', '2026-01-02', '2026-01-02 03:04:05'),
  (2, 'zyxwvu',   'zyxwvu',   'yxwv',   '2026-06-30', '2026-06-30 10:11:12'),
  (3, NULL,       NULL,       NULL,     NULL,         NULL);
analyze t;

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

-- REPLACE on a varchar column: the subject arrives under a RelabelType.
select is_vectorized('select replace(v, ''b'', ''X'') from t');
select id, replace(v, 'b', 'X') from t order by id;

-- REPLACE on a char(n) column.
select is_vectorized('select replace(c, ''b'', ''X'') from t');
select id, replace(c, 'b', 'X') from t order by id;

-- REPLACE with a computed subject.
select is_vectorized('select replace(a || ''!'', ''b'', ''X'') from t');
select id, replace(a || '!', 'b', 'X') from t order by id;

-- Control: a bare text column still vectorizes.
select is_vectorized('select replace(a, ''b'', ''X'') from t');
select id, replace(a, 'b', 'X') from t order by id;

-- EXTRACT of units Arrow has no component function for.
select is_vectorized('select extract(dow from ts) from t');
select id, extract(dow from ts) from t order by id;
select is_vectorized('select extract(doy from ts) from t');
select id, extract(doy from ts) from t order by id;
select is_vectorized('select extract(quarter from ts) from t');
select id, extract(quarter from ts) from t order by id;
select is_vectorized('select extract(epoch from ts) from t');
select id, extract(epoch from ts) from t order by id;
select is_vectorized('select extract(dow from d) from t');
select id, extract(dow from d) from t order by id;

-- Control: the mapped units still vectorize.
select is_vectorized('select extract(year from ts) from t');
select id, extract(year from ts), extract(month from ts), extract(day from ts)
from t order by id;

-- Control: the fallback answers match the row executor exactly.
set vector.enable_vectorization = off;
create table ref as
  select id, replace(v, 'b', 'X') r, extract(dow from ts) w from t
  distributed by (id);
set vector.enable_vectorization = on;
select count(*) as mismatches
from ((select id, replace(v, 'b', 'X') r, extract(dow from ts) w from t)
      except all
      (select id, r, w from ref)) q;

set client_min_messages = warning;
drop schema vec_replace124 cascade;
