-- A reader gang must inherit the writer's subtransaction state.
--
-- readSharedLocalSnapshot_forCursor() (src/backend/utils/time/sharedsnapshot.c)
-- hand-copies the writer's shared local snapshot.  When it forgets
-- subxcnt / subxip[] / suboverflowed, the reader cannot see another session's
-- running SUBTRANSACTION: XidInMVCCSnapshot_Local() says "not running", CLOG
-- says "not committed", so HeapTupleSatisfiesMVCC() concludes "the deleter must
-- have aborted" and stamps a permanent, un-WAL-logged, never-revalidated
-- HEAP_XMAX_INVALID.  Once the deleter commits, the deleted tuple is
-- resurrected and the unique index holds two live entries for one key.
--
-- That snapshot path is taken whenever DtxContextInfo.cursorContext is set,
-- which is just portal->is_extended_query, and THREE different things set it:
--
--   1. DECLARE CURSOR                      portalcmds.c, PerformCursorOpen()
--   2. an SPI cursor, i.e. every
--      PL/pgSQL "FOR rec IN SELECT ..."    spi.c, SPI_cursor_open_internal()
--   3. any extended-protocol Bind, i.e.
--      a parameterized statement from
--      JDBC / psycopg2 / ODBC / libpq      postgres.c, exec_bind_message()
--
-- All three are exercised below.  Entry point 3 needs the extended query
-- protocol, which this driver does not speak, so it runs out of the
-- xmax_extquery_test helper program.
--
-- Every scenario needs the same three ingredients:
--   a. the reader gang is warmed up while NO subtransaction exists anywhere, so
--      the reader QE's process-static snapshot keeps subxcnt = 0 (the cursor
--      path never writes that field again);
--   b. the DELETE runs in a subtransaction -- a subxid reaches a snapshot only
--      through subxip[];
--   c. another transaction commits afterwards, so the subxid falls strictly
--      inside (snap_xmin, snap_xmax) and the range check at the top of
--      XidInMVCCSnapshot() does not mask the bug.

-- ----------------------------------------------------------------------------
-- 1. explicit DECLARE CURSOR
-- ----------------------------------------------------------------------------

create table xmax_cursor (id int, pad text) distributed by (id);
create unique index xmax_cursor_uniq on xmax_cursor (id);
create table xmax_cursor_bump (id int) distributed by (id);
insert into xmax_cursor select i, 'orig-' || i from generate_series(1, 20) i;
-- settle the xmin hint bits so the reader goes straight to the xmax path
vacuum (freeze, analyze) xmax_cursor;

-- warm the reader gang up while no subtransaction exists
1: begin;
1: declare c0 cursor for select id from xmax_cursor order by id;
1: fetch 3 from c0;
1: close c0;
1: commit;

-- delete inside a subtransaction, re-insert the key at the top level, hold open
2: begin;
2: savepoint s1;
2: delete from xmax_cursor where id = 1;
2: release savepoint s1;
2: insert into xmax_cursor values (1, 'new-1');

-- commit a write on every segment
3: insert into xmax_cursor_bump select generate_series(1, 300);

-- the trigger
1: begin;
1: declare c1 cursor for select id, pad from xmax_cursor order by id;
1: fetch 3 from c1;
1: close c1;
1: commit;

2: commit;

-- exactly one live row with id = 1, and it must be 'new-1'
1: select id, pad from xmax_cursor where id = 1 order by pad;
1: select id, count(*) as n from xmax_cursor group by id having count(*) > 1;
-- and rebuilding the existing unique index must still succeed
1: reindex index xmax_cursor_uniq;

1q:
2q:
3q:
drop table xmax_cursor;
drop table xmax_cursor_bump;

-- ----------------------------------------------------------------------------
-- 2. SPI cursor: a PL/pgSQL FOR loop, no DECLARE CURSOR anywhere
-- ----------------------------------------------------------------------------

create table xmax_plpgsql (id int, pad text) distributed by (id);
create unique index xmax_plpgsql_uniq on xmax_plpgsql (id);
create table xmax_plpgsql_bump (id int) distributed by (id);
insert into xmax_plpgsql select i, 'orig-' || i from generate_series(1, 20) i;
vacuum (freeze, analyze) xmax_plpgsql;

4: do $$ declare r record; n int := 0; begin for r in select id from xmax_plpgsql order by id loop n := n + 1; end loop; raise notice 'warmup scanned % rows', n; end $$;

5: begin;
5: savepoint s1;
5: delete from xmax_plpgsql where id = 1;
5: release savepoint s1;
5: insert into xmax_plpgsql values (1, 'new-1');

6: insert into xmax_plpgsql_bump select generate_series(1, 300);

4: do $$ declare r record; n int := 0; begin for r in select id, pad from xmax_plpgsql order by id loop n := n + 1; end loop; raise notice 'trigger scanned % rows', n; end $$;

5: commit;

4: select id, pad from xmax_plpgsql where id = 1 order by pad;
4: select id, count(*) as n from xmax_plpgsql group by id having count(*) > 1;
4: reindex index xmax_plpgsql_uniq;

4q:
5q:
6q:
drop table xmax_plpgsql;
drop table xmax_plpgsql_bump;

-- ----------------------------------------------------------------------------
-- 3. extended query protocol (Parse/Bind/Execute), no cursor at all
-- ----------------------------------------------------------------------------

! ./xmax_extquery_test dbname=isolation2test;

7: select id, pad from xmax_extquery where id = 1 order by pad;
7: select id, count(*) as n from xmax_extquery group by id having count(*) > 1;
7: reindex index xmax_extquery_uniq;

7q:
drop table xmax_extquery;
drop table xmax_extquery_bump;
