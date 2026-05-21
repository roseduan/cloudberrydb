set optimizer=on;
create table s1(a int, b int, c int, d int);
create table s2(a int, b int, c int, d int);

explain select *
from
(select * from s1 where a < 0) q1,
(select * from s1 where a > 0) q2;

explain select *
from
(select sum(b) from s1 where a < 0) q1,
(select sum(b) from s1 where a > 0) q2;

explain select *
from
(select sum(b) from s1 where int4lt(a, 0)) q1,
(select sum(b) from s1 where int4eq(a, 0)) q2;

explain select *
from
(select sum(s1.b)
 from s1,s2
 where s1.d = s2.d and s1.a < 0) q1,
(select sum(s1.b)
 from s1,s2
 where s1.d = s2.d and s1.a > 0) q2;

explain select *
from
(select sum(s1.b)
 from s1,s2
 where s1.d = s2.d and s1.a < 0) q1(x),
(select sum(s1.b)
 from s1,s2
 where s1.d = s2.d and s1.a > 0) q2(x)
where q2.x > 1;

explain select *
from
(select sum(s1.b)
 from s1,s2
 where s1.d = s2.d and s1.a < 0) q1(x),
(select avg(s1.b)
 from s1,s2
 where s1.d = s2.d and s1.a > 0) q2(x)
where q2.x > 1;

-- falied, orca can not give a plan
explain select *
from
(select count(*)
 from s1,s2
 where s1.d = s2.d) q1(x),
(select count(*)
 from s1,s2
 where s1.d = s2.d) q2(x);

-- failed, ref CTranslatorScalarToDXL::TranslateAggrefToDXL aggfilter does not support
explain select *
from
(select sum(b) filter (where c < 0) from s1 where int4lt(a, 0)) q1,
(select sum(b) filter (where c < 0) from s1 where int4eq(a, 0)) q2;

-- failed, join order does not same
explain select *
from
(select sum(s1.b)
 from s1,s2
 where s1.d = s2.d and s1.a < 0) q1,
(select sum(s1.b)
 from s2,s1
 where s1.d = s2.d and s1.a > 0) q2;

--bug fix
select count(*) from (select 1) a, (select 1) b;

explain select *
from
(select sum(s1.b)
 from s1,s2
 where s1.d = s2.d and s1.a < 0) q1(x),
(select 1) a,
(select avg(s1.b)
 from s1,s2
 where s1.d = s2.d and s1.a > 0) q2(x),
(select 1) b
where q2.x > 1;

-- All args of the WHERE BoolExpr differ between the two grouped subqueries.
-- Previously this left an empty BoolExpr in the shared-CTE jointree quals,
-- tripping CTranslatorScalarToDXL.cpp:796 (0 < ListLength(bool_expr->args))
-- and falling back to the Postgres planner. The reformer should now drop
-- the empty BoolExpr and ORCA should produce a plan.
explain (costs off) select *
from
(select sum(b) from s1 where a > 0 and b > 0) q1,
(select sum(b) from s1 where a < 0 and b < 0) q2;

-- TPC-DS query 28 pattern: same table, multiple disjoint range filters,
-- comma cross join, all WHERE BoolExpr args differ across siblings.
explain (costs off) select *
from
(select avg(b) lp from s1 where a between 0 and 5
   and (b between 124 and 134 or c between 1160 and 2160 or d between 79 and 99)) q1,
(select avg(b) lp from s1 where a between 6 and 10
   and (b between 121 and 131 or c between 7528 and 8528 or d between 76 and 96)) q2;

-- Both subqueries have no WHERE clause. Previously crashed the segment in
-- GroupedMatchSubquery via IsA(NULL, BoolExpr); ORCA should now produce a
-- plan that shares the scan of s1.
explain (costs off) select *
from
(select avg(b) from s1) q1,
(select avg(b) from s1) q2;

drop table s1, s2;
reset optimizer;
