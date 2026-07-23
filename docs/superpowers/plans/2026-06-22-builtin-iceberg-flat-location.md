# Builtin Iceberg Flat Location Implementation Plan

> **2026-07-20 更新（实现时的方案偏离，以此为准）：** 用户拍板对 builtin 表**硬禁止**
> `location` 选项 —— 建表带 `location` 直接 `ERROR: location option is not allowed for
> builtin iceberg tables`，**不再保留**本文原先"显式 LOCATION 分支照旧"的设计（下文
> 第 7 行的旧描述已作废）。因此 `pg_iceberg_generate_builtin_location()` 签名简化为
> 单参 `(table_info)`，函数体不再拼 db/ns/table，也不再有"空 suffix"校验，改为：非空
> `location` → 报错；否则返回去掉结尾 `/` 的 volume prefix。已同步新增 TDD 冒烟用例
> `iceberg_buildin_flat_location`（断言"拍平 + 共享" + "带 location 报错"）并加入 builtin
> smoke `REGRESS`。代码/测试/指南文档改动已落地本地，**待用户复核后再 commit/push**。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make builtin-catalog iceberg tables write to the shared volume base path `<scheme>://<bucket>/<base_path>` instead of `<base>/<db>/<schema>/<table>`, dropping the `db/namespace/table` path segments.

**Architecture:** The default storage location for a builtin iceberg table is computed in one place — `pg_iceberg_generate_builtin_location()` (`contrib/datalake_fdw/src/am_iceberg/pg_iceberg_catalog.c`). Today it appends `db/namespace/table` to the volume prefix. We make the default return the volume prefix itself (trailing slash trimmed). All tables on a volume then share `<base>/metadata/` and `<base>/data/`; Iceberg keeps them isolated at the file level (every file name carries a UUID) and the builtin catalog tracks each table's `metadata_location` in the PG catalog, so no per-table path segment is needed for correctness. The explicit per-table `LOCATION` option keeps its current behavior.

**Tech Stack:** C (PostgreSQL/Cloudberry extension, PGXS), pg_regress smoke tests, MinIO (S3) object storage, dev container `hd-iceberg-test`.

## Global Constraints

- C indentation: **tabs** (tab width 4); 78-col line limit; `/* */` comments only. (repo `.editorconfig` / CLAUDE.md)
- Build and test run **inside the container** `hd-iceberg-test`; repo is mounted at `/workspace/database`.
- `datalake_fdw` is in `shared_preload_libraries`, so reloading new C code requires a **cluster restart** (`gpstop -ar`), not just a new session.
- Do **not** modify `buildVolumeBasePath()` (`contrib/datalake_fdw/src/iceberg_volume_fdw/iceberg_volume_option.c`) — it is shared with other callers and must keep emitting a trailing `/`.
- Behavior change applies to **newly created** builtin tables only. Pre-existing tables keep their stored `metadata_location` and the `location` ltoption verbatim — no migration, mixed layouts coexist.
- Assumption / non-goal: builtin volumes are expected to set a non-empty `base_path`. A builtin volume with an empty `base_path` (degenerate, esp. HDFS root) is out of scope; the volume prefix already behaves oddly there today.
- Branch: continue on `iceberg-delete-344`. Commit style: `feat(iceberg): ...`.

---

## File Structure

- `contrib/datalake_fdw/src/am_iceberg/pg_iceberg_catalog.c` — **modify**. `pg_iceberg_generate_builtin_location()` (lines 83-130) gets the new default branch and a simplified signature; its single caller (line 372) is updated.
- `contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin/sql/iceberg_buildin_flat_location.sql` — **create**. New smoke test asserting the flat + shared layout.
- `contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin/expected/iceberg_buildin_flat_location.out` — **create**. Expected output (flat layout).
- `contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin/Makefile` — **modify**. Add the new test to `REGRESS`.
- `contrib/datalake_fdw/docs/datalake_fdw_iceberg_guide_zh.md` — **modify**. Update the documented path layout (§4.3 volume model / wherever the `<base>/db/schema/table` layout is described).

---

### Task 1: Failing smoke test for flat + shared layout

**Files:**
- Create: `contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin/sql/iceberg_buildin_flat_location.sql`
- Create: `contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin/expected/iceberg_buildin_flat_location.out`
- Modify: `contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin/Makefile`

**Interfaces:**
- Consumes: catalog/volume DDL idiom from existing `iceberg_buildin.sql` (server type `s3`, endpoint `http://minio:9000`, bucket `warehouse`, volume `base_path '/default_volume/'`); the deletion-queue table `pg_ext_aux.pg_iceberg_deletion_queue` with columns `path` (= enqueued `metadata_location`) and `table_qname` (= `"nsp.relname"`), populated synchronously on the QD during `DROP`.
- Produces: a pg_regress test named `iceberg_buildin_flat_location` that asserts (a) each table's metadata path sits directly under `<base>/metadata/` with no db/schema/table segments, and (b) two tables on the same volume share one root.

- [ ] **Step 1: Write the test SQL**

Create `contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin/sql/iceberg_buildin_flat_location.sql`:

```sql
-- Verify builtin iceberg tables share the volume base path with no
-- db/namespace/table segments: location = <scheme>://<bucket>/<base_path>.
CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- catalog
CREATE SERVER flat_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER flat_catalog_server;
CREATE FOREIGN CATALOG flat_catalog SERVER flat_catalog_server;
set iceberg_default_catalog='flat_catalog';

-- volume
CREATE SERVER flat_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER flat_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME flat_volume SERVER flat_volume_server
OPTIONS (base_path '/flat_volume/');
set iceberg_default_volume='flat_volume';

-- two tables on the same volume
CREATE ICEBERG TABLE flat_t1 (id bigint);
CREATE ICEBERG TABLE flat_t2 (id bigint);
INSERT INTO flat_t1 VALUES (1);
INSERT INTO flat_t2 VALUES (1);

-- DROP enqueues each table's metadata_location into the deletion queue
-- (synchronous on the QD, visible in the same transaction).
BEGIN;
DROP TABLE flat_t1;
DROP TABLE flat_t2;

-- (a) each metadata path is directly under <base>/metadata/, no extra segments
SELECT table_qname,
       path ~ '^[a-z0-9]+://warehouse/flat_volume/metadata/[0-9]{5}-[0-9a-f-]+\.metadata\.json$'
         AS flat_layout,
       path !~ '/(public|flat_t[12])/' AS no_db_schema_table_segment
  FROM pg_ext_aux.pg_iceberg_deletion_queue
 WHERE table_qname IN ('public.flat_t1', 'public.flat_t2')
 ORDER BY table_qname;

-- (b) both tables resolve to the SAME root (strip "/metadata/<file>")
SELECT count(DISTINCT regexp_replace(path, '/metadata/[^/]+$', '')) AS distinct_roots
  FROM pg_ext_aux.pg_iceberg_deletion_queue
 WHERE table_qname IN ('public.flat_t1', 'public.flat_t2');
COMMIT;

-- Cleanup the enqueued rows (queue table has a pinned system OID).
SET allow_system_table_mods = on;
DELETE FROM pg_ext_aux.pg_iceberg_deletion_queue WHERE table_qname LIKE 'public.flat_t%';
SET allow_system_table_mods = off;

-- Cleanup catalog/volume.
DROP VOLUME flat_volume;
DROP USER MAPPING FOR current_user SERVER flat_volume_server;
DROP SERVER flat_volume_server;
DROP CATALOG flat_catalog;
DROP USER MAPPING FOR current_user SERVER flat_catalog_server;
DROP SERVER flat_catalog_server;
```

- [ ] **Step 2: Write the expected output (the desired, post-change result)**

Create `contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin/expected/iceberg_buildin_flat_location.out` capturing the DDL/INSERT echo plus the two assertion results. The assertion blocks must be:

```
 table_qname    | flat_layout | no_db_schema_table_segment 
----------------+-------------+----------------------------
 public.flat_t1 | t           | t
 public.flat_t2 | t           | t
(2 rows)

 distinct_roots 
----------------
              1
(1 row)
```

> Note: the exact echoed banner lines for the CREATE/INSERT statements should be filled in from the first real run (Step 3) — copy the non-assertion lines verbatim from `results/iceberg_buildin_flat_location.out` and keep the two assertion blocks exactly as above. Before the code change these two blocks will differ (booleans `f`, `distinct_roots = 2`), which is the intended failing diff.

- [ ] **Step 3: Add the test to REGRESS and run it to verify it FAILS**

Edit the `REGRESS` list in `.../builtin/Makefile` to append `iceberg_buildin_flat_location` (add at the end of the list, continuing the existing backslash-continued lines):

```make
          iceberg_truncate_cleanup iceberg_vacuum_cleanup \
          iceberg_buildin_flat_location
```

Run (in container, against the running cluster):

```bash
docker exec hd-iceberg-test bash -lc '
  cd /workspace/database/contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin &&
  make installcheck REGRESS=iceberg_buildin_flat_location 2>&1 | tail -30'
```

Expected: **FAIL**. `regression.diffs` shows `flat_layout = f` / `no_db_schema_table_segment = f` and `distinct_roots = 2`, because the current code emits `.../flat_volume/<db>/public/flat_t1/metadata/...`.

- [ ] **Step 4: Commit the failing test**

```bash
cd /Users/hashdata/hashdata-lightning-umbrella/database
git add contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin/sql/iceberg_buildin_flat_location.sql \
        contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin/expected/iceberg_buildin_flat_location.out \
        contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin/Makefile
git commit -m "test(iceberg): flat builtin location smoke test (failing) (#344)"
```

---

### Task 2: Flatten the default builtin location

**Files:**
- Modify: `contrib/datalake_fdw/src/am_iceberg/pg_iceberg_catalog.c` (function `pg_iceberg_generate_builtin_location`, lines 83-130; call site line 372)

**Interfaces:**
- Consumes: `pg_iceberg_get_builtin_volume_prefix(const char *volume_server_name, const char *volume_name)` → returns the volume base path **with a trailing `/`** (unchanged).
- Produces: `static char *pg_iceberg_generate_builtin_location(IcebergTableInfo *table_info)` — signature reduced from 3 params to 1 (the `nameSpace`/`tableName` params are no longer used by this function; the caller still computes them for `pg_iceberg_create_table`). Returns the table-root location: the explicit `LOCATION` suffix appended to the prefix when set, otherwise the volume prefix with its single trailing `/` trimmed.

- [ ] **Step 1: Replace the function body and signature**

Replace the whole function `pg_iceberg_generate_builtin_location` (currently `pg_iceberg_catalog.c:83-130`) with:

```c
/*
 * Generate the storage location (table root URI) for a builtin-catalog
 * iceberg table.
 *
 * Unless the table sets an explicit LOCATION option, every builtin table on a
 * volume shares the volume base path "<scheme>://<bucket>/<base_path>"; Iceberg
 * creates the per-table metadata/ and data/ subtrees underneath it.  The
 * builtin catalog records each table's metadata_location in the PG catalog and
 * every file Iceberg writes carries a UUID, so a shared directory keeps tables
 * isolated at the file level without db/namespace/table path segments.  Note
 * this means table-scoped cleanup must enumerate the metadata tree (it does);
 * no prefix-based listing may be used against this directory.
 */
static char *
pg_iceberg_generate_builtin_location(IcebergTableInfo *table_info)
{
	char	   *prefix;
	const char *location_suffix;
	size_t		prefix_len;

	Assert(table_info != NULL);

	prefix = pg_iceberg_get_builtin_volume_prefix(table_info->volume_server_name,
												  table_info->volume_name);

	/*
	 * Explicit per-table LOCATION option: keep the historical behavior of
	 * appending the user-provided suffix to the volume prefix.
	 */
	if (table_info->opts != NULL &&
		table_info->opts->location != NULL &&
		table_info->opts->location[0] != '\0')
	{
		location_suffix = table_info->opts->location;

		while (*location_suffix == '/')
			location_suffix++;

		if (*location_suffix == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("empty iceberg table location suffix")));

		return psprintf("%s%s", prefix, location_suffix);
	}

	/*
	 * Default: share the volume base path.  pg_iceberg_get_builtin_volume_prefix
	 * always returns a trailing '/'; strip it so Iceberg derives clean
	 * "<base>/metadata" and "<base>/data" paths rather than "<base>//metadata"
	 * (object stores treat "//" as a distinct, empty path segment).
	 */
	prefix_len = strlen(prefix);
	if (prefix_len > 0 && prefix[prefix_len - 1] == '/')
		prefix[prefix_len - 1] = '\0';

	return prefix;
}
```

- [ ] **Step 2: Update the single call site**

At `pg_iceberg_catalog.c:372`, change:

```c
			if (pg_iceberg_is_builtin_catalog(table_info->catalog_server_name))
				location = pg_iceberg_generate_builtin_location(table_info, nameSpace, tableName);
```

to:

```c
			if (pg_iceberg_is_builtin_catalog(table_info->catalog_server_name))
				location = pg_iceberg_generate_builtin_location(table_info);
```

(`nameSpace` and `tableName` are still used just below at the `pg_iceberg_create_table(...)` call, so leave those locals in place.)

- [ ] **Step 3: Rebuild the extension, restart the cluster**

```bash
docker exec hd-iceberg-test bash -lc '
  cd /workspace/database/contrib/datalake_fdw &&
  make -s install 2>&1 | tail -15 &&
  source /workspace/database/../greenplum_path.sh 2>/dev/null;
  gpstop -ar 2>&1 | tail -5'
```

Expected: build prints no errors; `gpstop -ar` reports the cluster restarted. (If `greenplum_path.sh` / `gpstop` live elsewhere in the container, use the env the existing smoke runs use; the goal is: reinstalled `.so` + restarted cluster so the new code is loaded.)

- [ ] **Step 4: Run the new smoke test to verify it PASSES**

```bash
docker exec hd-iceberg-test bash -lc '
  cd /workspace/database/contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin &&
  make installcheck REGRESS=iceberg_buildin_flat_location 2>&1 | tail -20'
```

Expected: **PASS** (`1 of 1 tests passed`). If the only diff is non-assertion banner lines, fix the expected file's banner lines (Task 1 Step 2 note) and re-run.

- [ ] **Step 5: Run the full builtin suite to catch regressions**

```bash
docker exec hd-iceberg-test bash -lc '
  cd /workspace/database/contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin &&
  make installcheck 2>&1 | tail -40'
```

Expected: all tests pass. Existing expected outputs do **not** print storage paths (verified: they echo DDL and query results, not `metadata_location`), so the layout change should not alter them. If any test diffs, inspect `regression.diffs` — a genuine path-printing assertion would need its expected file updated to the flat layout; investigate before blindly accepting.

- [ ] **Step 6: Commit**

```bash
cd /Users/hashdata/hashdata-lightning-umbrella/database
git add contrib/datalake_fdw/src/am_iceberg/pg_iceberg_catalog.c \
        contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg/builtin/expected/iceberg_buildin_flat_location.out
git commit -m "feat(iceberg): builtin tables share volume base path (drop db/ns/table) (#344)

Default builtin-catalog table location is now the volume base path
<scheme>://<bucket>/<base_path>; all tables on a volume share it.
Explicit per-table LOCATION option is unchanged. Pre-existing tables keep
their stored location/metadata_location, so layouts coexist with no
migration."
```

---

### Task 3: Update the iceberg guide doc

**Files:**
- Modify: `contrib/datalake_fdw/docs/datalake_fdw_iceberg_guide_zh.md`

**Interfaces:**
- Consumes: nothing. Documentation-only.
- Produces: doc text reflecting that builtin table storage layout is `<scheme>://<bucket>/<base_path>/{metadata,data}/...` shared per volume, and that isolation is at the volume (`base_path`) granularity.

- [ ] **Step 1: Find the path-layout description**

```bash
grep -n "db/table\|/metadata/\|base_path\|namespace\|schema/table\|warehouse" \
  contrib/datalake_fdw/docs/datalake_fdw_iceberg_guide_zh.md | head -20
```

- [ ] **Step 2: Edit the layout description**

Update the located section so it states the builtin layout is the shared volume base path. Replace any `<base>/<db>/<schema>/<table>/metadata/...` description with:

```
内置 catalog 的表存储路径为卷的 base path 本身：
<scheme>://<bucket>/<base_path>/metadata/...（以及 /data/...）。
同一个 volume 下的所有内置表共享该目录，靠每个文件名自带的 UUID 与
PG 目录中记录的 per-table metadata_location 实现文件级隔离；隔离粒度
落在 volume（base_path）上。显式设置表的 LOCATION 选项时，按
<base_path>/<location> 拼接（行为不变）。
```

(Adjust wording to match the surrounding doc style; keep it in the existing section rather than adding a new top-level heading.)

- [ ] **Step 3: Commit**

```bash
cd /Users/hashdata/hashdata-lightning-umbrella/database
git add contrib/datalake_fdw/docs/datalake_fdw_iceberg_guide_zh.md
git commit -m "doc(iceberg): document shared volume base-path layout for builtin tables (#344)"
```

---

## Notes for the implementer

- **Why no agent (Java) change:** `DlIcebergBuildInCatalog.defaultWarehouseLocation()` throws "nosupport" — the agent never derives the path; the FDW passes the computed `location`, and the agent uses it verbatim. Table identity on the agent is `(dbName, tableName)`, independent of the storage path.
- **Why cleanup stays safe:** the deletion consumer (`pg_iceberg_av_consumer.c:309-318`) deletes strictly by enqueued `path` (single file or metadata-tree parse), and the agent's `deleteFiles` (`IcebergRestController.java:1233/1323`) walks the table's own metadata tree — neither lists a prefix. A shared directory therefore does not cause cross-table deletion.
- **Residual constraint to honor:** because all tables share one directory, no prefix-listing maintenance (e.g. Iceberg `remove_orphan_files`, or the future compaction reserved by the `datalake.iceberg_autovacuum_*` GUCs in issue #344) may be run against these tables — it would treat sibling tables' live files as orphans. Keep cleanup metadata-tree-scoped.
