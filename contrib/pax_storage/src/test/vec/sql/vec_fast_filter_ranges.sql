--
-- PAX fast filter: quals the vectorized scan hands to the PAX storage layer.
-- Every query runs with vectorization on and then off; the two halves of
-- each section must print the same counts.
--
create schema vec_ffr;
set search_path = vec_ffr;

create table ffr_int (id int, c int) using pax distributed by (id);
insert into ffr_int select i, i % 10 from generate_series(1, 10000) i;

-- With predicate pushdown off the scan builds no fast filter, so it must
-- keep evaluating the qual itself instead of dropping it.
set gp_enable_predicate_pushdown = off;
set vector.enable_vectorization = on;
select count(*) from ffr_int where c = 5;
select count(*) from ffr_int where c > 7;
set vector.enable_vectorization = off;
select count(*) from ffr_int where c = 5;
select count(*) from ffr_int where c > 7;
reset gp_enable_predicate_pushdown;
reset vector.enable_vectorization;

--
-- Columns added after the data was written: the old files do not have them,
-- and every old row reads as the column's default (or NULL).  The filters
-- must judge that value, whether it passes or not.  Small groups, so the
-- sampling and the interleaved paths both run.
--
set pax.max_tuples_per_group = 1000;
create table ffr_add (id int, a int)
  using pax with (storage_format = porc) distributed by (id);
insert into ffr_add select i, i % 7 from generate_series(1, 6000) i;
create table ffr_addvec (like ffr_add)
  using pax with (storage_format = porc_vec) distributed by (id);
insert into ffr_addvec select * from ffr_add;
reset pax.max_tuples_per_group;
alter table ffr_add add column k int default 5, add column m int,
  add column s text default 'abc';
alter table ffr_addvec add column k int default 5, add column m int,
  add column s text default 'abc';
insert into ffr_add values (9001, 1, 7, 1, 'xyz'), (9002, 2, null, null, null);
insert into ffr_addvec values (9001, 1, 7, 1, 'xyz'), (9002, 2, null, null, null);
create table ffr_addq (n int, qual text) distributed randomly;
insert into ffr_addq values
  (1, $q$k = 5$q$), (2, $q$k = 7$q$), (3, $q$k > 6$q$), (4, $q$k <> 5$q$),
  (5, $q$k in (5, 7)$q$), (6, $q$k in (1, 2)$q$), (7, $q$m = 1$q$), (8, $q$m <> 2$q$),
  (9, $q$s like '%b%'$q$), (10, $q$s not like '%b%'$q$), (11, $q$s <> ''$q$),
  (12, $q$k = 7 and a < 3$q$), (13, $q$k = 5 and s like '%y%'$q$);
set vector.enable_vectorization = on;
select format('select %s as n, %L as tbl, count(*) from %I where %s', n, t, t, qual)
from ffr_addq, (values ('ffr_add'), ('ffr_addvec')) f(t)
order by n, t \gexec
set vector.enable_vectorization = off;
select format('select %s as n, %L as tbl, count(*) from %I where %s', n, t, t, qual)
from ffr_addq, (values ('ffr_add'), ('ffr_addvec')) f(t)
order by n, t \gexec
reset vector.enable_vectorization;

--
-- Numeric columns, and AND / OR trees on one column (range sets).  Small
-- groups, so the sampling, interleaved and selective paths all run.
--
set pax.max_tuples_per_group = 1000;
create table ffr_num (id int, a numeric(7,2), b numeric(7,2), c int,
                      d numeric(18,6), e numeric(4,0), f numeric(4,4), g int8)
  using pax with (storage_format = porc) distributed by (id);
insert into ffr_num
select i,
       case when i % 97 = 0 then null
            when i % 101 = 0 then 'NaN'::numeric
            else ((i * 37) % 40000 - 20000) / 100.0 end,
       ((i * 53) % 60000 - 10000) / 100.0,
       i % 10,
       case when i % 89 = 0 then null
            else (i * 7919 % 2000000 - 1000000) * 123456.789123 / 1000 end,
       (i % 199) - 99,
       (i % 9999) / 10000.0,
       case i % 5 when 0 then 9223372036854775807
                  when 1 then -9223372036854775808 else i end
from generate_series(1, 30000) i;
create table ffr_numvec (like ffr_num)
  using pax with (storage_format = porc_vec) distributed by (id);
insert into ffr_numvec select * from ffr_num;
reset pax.max_tuples_per_group;

create table ffr_quals (n int, qual text) distributed randomly;
insert into ffr_quals values
  -- q13: an OR of ranges on one column, ANDed with another such OR
  (1, $q$((a >= 100.00 and a <= 150.00) or (a >= 50.00 and a <= 100.00) or (a >= 150.00 and a <= 200.00))
         and ((b >= '100'::numeric(7,2) and b <= '200'::numeric(7,2)) or (b >= 150 and b <= 300) or (b >= 50 and b <= 250))$q$),
  -- single comparisons, both operand orders
  (2, $q$a = 12.34$q$), (3, $q$a <> 12.34$q$), (4, $q$a < -0.5$q$), (5, $q$a <= 0$q$),
  (6, $q$a > 99.99$q$), (7, $q$a >= 199.99$q$), (8, $q$12.34 < a$q$), (9, $q$0 >= a$q$),
  -- NaN sorts above every number
  (10, $q$a > 150$q$), (11, $q$a <> 0$q$), (12, $q$a >= 199.99 or a < -199.99$q$),
  -- the whole qual is one OR
  (16, $q$(a between -10 and -5) or (a between 5 and 10) or a = 0$q$),
  -- empty and nested ranges
  (17, $q$(a < 10 and a > 20) or a = 50$q$),
  (18, $q$((a > 0 and a < 10) or a > 100) and a <> 150$q$),
  -- numeric beside integer filters
  (19, $q$c in (1, 3, 5) and a between 0 and 100$q$),
  (20, $q$(c = 1 or c = 3) and (a < -100 or a > 100)$q$),
  (21, $q$(c < 2 or c > 8) and c <> 9$q$),
  -- wide scale, scale 0, scale == precision
  (22, $q$(d between -1000.5 and 1000.5) or d > 100000000.123456$q$),
  (23, $q$d < -50000.000001 or d = 0$q$),
  (24, $q$(e between -10 and 10) or e = 99$q$), (25, $q$e > -5 and e <= 5$q$),
  (26, $q$(f >= 0.5 and f < 0.75) or f = 0.9998$q$),
  -- int8 at the ends of its range
  (27, $q$g = 9223372036854775807 or g = -9223372036854775808$q$),
  (28, $q$g < -9223372036854775807 or g > 9223372036854775806$q$),
  (29, $q$(g > 100 and g < 200) or g >= 29990$q$);

set vector.enable_vectorization = on;
select format('select %s as n, %L as fmt, count(*) from %I where %s',
              n, t, t, qual)
from ffr_quals, (values ('ffr_num'), ('ffr_numvec')) f(t)
order by n, t \gexec
set vector.enable_vectorization = off;
select format('select %s as n, %L as fmt, count(*) from %I where %s',
              n, t, t, qual)
from ffr_quals, (values ('ffr_num'), ('ffr_numvec')) f(t)
order by n, t \gexec
reset vector.enable_vectorization;

-- Values, not only counts, for the q13 qual.
set vector.enable_vectorization = on;
select count(*), sum(a), sum(b), sum(d) from ffr_num
where ((a >= 100.00 and a <= 150.00) or (a >= 50.00 and a <= 100.00) or (a >= 150.00 and a <= 200.00))
  and ((b >= 100 and b <= 200) or (b >= 150 and b <= 300) or (b >= 50 and b <= 250));
select count(*), sum(a), sum(b), sum(d) from ffr_numvec
where ((a >= 100.00 and a <= 150.00) or (a >= 50.00 and a <= 100.00) or (a >= 150.00 and a <= 200.00))
  and ((b >= 100 and b <= 200) or (b >= 150 and b <= 300) or (b >= 50 and b <= 250));
set vector.enable_vectorization = off;
select count(*), sum(a), sum(b), sum(d) from ffr_num
where ((a >= 100.00 and a <= 150.00) or (a >= 50.00 and a <= 100.00) or (a >= 150.00 and a <= 200.00))
  and ((b >= 100 and b <= 200) or (b >= 150 and b <= 300) or (b >= 50 and b <= 250));
select count(*), sum(a), sum(b), sum(d) from ffr_numvec
where ((a >= 100.00 and a <= 150.00) or (a >= 50.00 and a <= 100.00) or (a >= 150.00 and a <= 200.00))
  and ((b >= 100 and b <= 200) or (b >= 150 and b <= 300) or (b >= 50 and b <= 250));
reset vector.enable_vectorization;

--
-- Columns added after the data was written: the old files do not have them,
-- and every old row reads as the column's default (or NULL).  The filters
-- must judge that value, whether it passes or not.
--
alter table ffr_num add column h numeric(7,2) default 5.00, add column k int default 5,
  add column m numeric(7,2);
alter table ffr_numvec add column h numeric(7,2) default 5.00, add column k int default 5,
  add column m numeric(7,2);
insert into ffr_num (id, a, h, k, m) values (40001, 1, 7.5, 7, 1), (40002, 2, null, null, null);
insert into ffr_numvec (id, a, h, k, m) values (40001, 1, 7.5, 7, 1), (40002, 2, null, null, null);
create table ffr_numaddq (n int, qual text) distributed randomly;
insert into ffr_numaddq values
  (1, $q$h between 4 and 6 or h > 7$q$), (2, $q$h = 7.5 or h < 0$q$), (3, $q$h = 5$q$),
  (4, $q$k = 5$q$), (5, $q$k = 7$q$), (6, $q$k = 5 or k = 7$q$), (7, $q$k > 6 or k < 0$q$),
  (8, $q$m = 1 or m > 5$q$), (9, $q$m <> 2$q$),
  (10, $q$k = 7 and a between 0 and 100$q$), (11, $q$h > 7 and k = 7$q$);
set vector.enable_vectorization = on;
select format('select %s as n, %L as fmt, count(*) from %I where %s', n, t, t, qual)
from ffr_numaddq, (values ('ffr_num'), ('ffr_numvec')) f(t)
order by n, t \gexec
set vector.enable_vectorization = off;
select format('select %s as n, %L as fmt, count(*) from %I where %s', n, t, t, qual)
from ffr_numaddq, (values ('ffr_num'), ('ffr_numvec')) f(t)
order by n, t \gexec
reset vector.enable_vectorization;

--
-- Deleted rows.  The fast filter must not mark the rows it filters out in
-- the visimap the backend caches: a later scan in the same session would
-- lose them too.  ffr_del1 deletes rows all over its files; ffr_del2 only
-- the first rows, so its visimaps end before most filtered rows.
--
create table ffr_del1 (id int, c int) using pax with (storage_format = porc)
  distributed by (id);
create table ffr_del1v (like ffr_del1) using pax with (storage_format = porc_vec)
  distributed by (id);
create table ffr_del2 (like ffr_del1) using pax with (storage_format = porc)
  distributed by (id);
insert into ffr_del1 select i, i % 10 from generate_series(1, 10000) i;
insert into ffr_del1v select * from ffr_del1;
insert into ffr_del2 select * from ffr_del1;
delete from ffr_del1 where id % 2 = 0;
delete from ffr_del1v where id % 2 = 0;
delete from ffr_del2 where id <= 30;
set vector.enable_vectorization = on;
select 'del1', sum(n) from (select c, count(*) n from ffr_del1 where c = 5 group by c) t;
select 'del1', sum(n) from (select c, count(*) n from ffr_del1 group by c) t;
select 'del1v', sum(n) from (select c, count(*) n from ffr_del1v where c = 5 group by c) t;
select 'del1v', sum(n) from (select c, count(*) n from ffr_del1v group by c) t;
select 'del2', sum(n) from (select c, count(*) n from ffr_del2 where c = 5 group by c) t;
select 'del2', sum(n) from (select c, count(*) n from ffr_del2 group by c) t;
set vector.enable_vectorization = off;
select 'del1', sum(n) from (select c, count(*) n from ffr_del1 where c = 5 group by c) t;
select 'del1', sum(n) from (select c, count(*) n from ffr_del1 group by c) t;
select 'del1v', sum(n) from (select c, count(*) n from ffr_del1v where c = 5 group by c) t;
select 'del1v', sum(n) from (select c, count(*) n from ffr_del1v group by c) t;
select 'del2', sum(n) from (select c, count(*) n from ffr_del2 where c = 5 group by c) t;
select 'del2', sum(n) from (select c, count(*) n from ffr_del2 group by c) t;
reset vector.enable_vectorization;

--
-- PORC_VEC bool columns keep one bit per row; the rows the filter keeps
-- (or the deletes leave) must come out with their own values.
--
create table ffr_vbool (id int, c int, f bool) using pax
  with (storage_format = porc_vec) distributed by (id);
insert into ffr_vbool select i, i % 10, case when i % 7 = 0 then null else i % 3 = 0 end
from generate_series(1, 10000) i;
set vector.enable_vectorization = on;
select f, count(*) from ffr_vbool where c = 5 group by f order by f;
set vector.enable_vectorization = off;
select f, count(*) from ffr_vbool where c = 5 group by f order by f;
delete from ffr_vbool where id % 4 = 0;
set vector.enable_vectorization = on;
select f, count(*) from ffr_vbool group by f order by f;
select f, count(*) from ffr_vbool where c = 5 group by f order by f;
set vector.enable_vectorization = off;
select f, count(*) from ffr_vbool group by f order by f;
select f, count(*) from ffr_vbool where c = 5 group by f order by f;
reset vector.enable_vectorization;

reset search_path;
drop schema vec_ffr cascade;
