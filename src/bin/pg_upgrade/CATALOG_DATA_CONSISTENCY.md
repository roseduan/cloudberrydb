# pg_upgrade — Catalog ↔ User-Data Consistency in Apache Cloudberry

This note documents how `pg_upgrade` keeps the system catalog and the
on-disk user-data files mutually consistent across an upgrade in
Apache Cloudberry's MPP setup (QD + N segments), and what each commit
in this PAX/pg_upgrade work stream contributes to that contract.

## 1. The end-to-end consistency contract

### 1.1 What `pg_dump --binary-upgrade` preserves from OLD QD

Every cross-cluster-stable identifier is pinned in the dump preamble.

- `pg_class.oid` (heap + toast + toast index + AO/AOCS aux trio
  `pg_aoseg_<oid>` / `pg_aoblkdir_<oid>` (+index) / `pg_aovisimap_<oid>`
  (+index) + bitmap aux `pg_bm_<oid>`/`_index`).  Emitted by
  `binary_upgrade_set_pg_class_oids_impl()` in
  `src/bin/pg_dump/pg_dump.c:5449-5664`.
- PAX aux OID assignments via `pax_emit_aux_oid_preassignment()`
  (`pg_dump.c:5637`).
- `pg_type.oid` (typrelid, arrayoid, multirange) via the
  `binary_upgrade_set_type_oids_*` helpers.
- `pg_namespace.oid`, `pg_authid.oid`, `pg_tablespace.oid`,
  `pg_enum.oid` — all routed through
  `AddPreassignedOidFromBinaryUpgrade()` in
  `src/backend/utils/adt/pg_upgrade_support.c:42-181`.
- `datfrozenxid` / `datminmxid` is preserved by an explicit
  `UPDATE pg_catalog.pg_database SET ...` in the dump
  (`pg_dump.c:3654-3659`).
- `DISTRIBUTED BY (...)` is regenerated textually by
  `pg_get_table_distributedby()` (`pg_dump.c:21336-21359`), executed
  only on QD.

What is **not** preserved by the dump:

- Per-table `relfilenode`.  CREATE TABLE on the new cluster assigns a
  fresh relfilenode; only the OID matches OLD.
- Per-table `relfrozenxid` / `relminmxid`.  Reset by
  `set_frozenxids()` and later `update_db_xids()` /
  `freeze_master_data()` from `pg_upgrade.c:316-317`.
- `gp_distribution_policy` rows on segments.

### 1.2 NEW QD catalog construction

`create_new_objects()` in `pg_upgrade.c:763-867` runs
`pg_restore --binary-upgrade` per DB.  Each pg_class row is born from
CREATE/ALTER DDL in the dump:

- **OID**: from the `binary_upgrade_set_next_*` preassignments —
  identical to OLD.
- **relfilenode**: freshly allocated by `heap_create_with_catalog()`.
  Will be replaced later by `transfer_all_new_tablespaces()`.
- **xmin**: the xact that runs `pg_restore`.

After file transfer, `pg_upgrade.c:316-318` runs `update_db_xids()`
then `freeze_master_data()` to re-freeze QD relfrozenxids and the
gp_fastsequence rows.

### 1.3 User data-file transfer

`transfer_all_new_tablespaces()` in `src/bin/pg_upgrade/relfilenode.c`
dispatches per `RelType`:

- **Heap**: `transfer_relfile()` for "", "_fsm", "_vm" plus numbered
  1 GB extents (`relfilenode.c:200-321`).  Visibility-map may be
  format-rewritten if `cat_ver` crosses
  `VISIBILITY_MAP_FROZEN_BIT_CAT_VER`; otherwise byte-identical.
- **AO/AOCS**: `transfer_ao()` transfers segment 0, then enumerates
  `.<segno>` files via `ao_foreach_extent_file()`.  Byte-identical.
- **PAX**: `transfer_pax_relfilenode_dir()` in
  `src/bin/pg_upgrade/greenplum/check_pax.c:282-398`.
  Source `base/<dboid>/<old_relfilenode>_pax/` → target
  `base/<new_dboid>/<new_relfilenode>_pax/`.  Walks one level deep,
  link / copy / clone each regular file (manifest JSON, micro-partition
  data, visimap files).  Missing source dir is allowed.
- External tables and `pg_aoseg.*` aux tables are skipped at
  map-construction time (`info.c:148`).

### 1.4 QE segment catalog provenance

`test_gpdb.sh:470-477` performs the central segment-bootstrap step:

```
mv NEW_QE_initdb → .old
cp -rp NEW_QD → NEW_QE              # byte-for-byte image of QD's pgdata
cp .old/postgresql.conf … back      # restore segment-specific configs only
```

Every segment's `pg_class`, `pg_attribute`, `pg_depend`,
`pg_inherits`, `pg_index` heap+index files are byte-identical clones
of QD's.  OIDs, relfilenodes, xmins all match QD.

Then `upgrade_segment` runs `pg_upgrade --mode=segment`.  In segment
mode, `create_new_objects()` is **skipped**
(`pg_upgrade.c:241-246`, gated by `is_greenplum_dispatcher_mode()`)
and the flow goes straight to `transfer_all_new_tablespaces()` +
`restore_aosegment_tables()` + `restore_pax_aux_tables()`.

### 1.5 relfilenode mapping correctness

`gen_db_file_maps()` in `info.c:40-179` sorts old/new RelInfo arrays
by OID and matches positionally.  The OID match is guaranteed by the
preassignment chain in §1.1.  Each match builds:

- `map->old_relfilenode = old_rel->relfilenode` (from OLD pg_class)
- `map->new_relfilenode = new_rel->relfilenode` (from NEW pg_class)
- `map->old_db_oid` / `map->new_db_oid` from `get_db_infos()`
  (`info.c:369-429`)

On segments, NEW relfilenodes are inherited from the QD rsync image,
not freshly generated, so `transfer_relfile_segment()` writes the
segment's OLD data file to `base/<dboid>/<QD's_relfilenode>`.  The
rename happens implicitly via the (OLD-QE-side, QD-NEW-side) map
pair.

### 1.6 Byte-level data-file consistency

`transfer_relfile_segment()` does pure link / clone / copy with no
transform, except for the VM-frozen-bit rewrite when `cat_ver`
crosses `VISIBILITY_MAP_FROZEN_BIT_CAT_VER`.  LINK mode produces
hardlinks; the OLD cluster is then unbootable
(`disable_old_cluster()` at `pg_upgrade.c:290-291`).

### 1.7 Cloudberry-specific reconstruction

- **gp_distribution_policy**: restored by textual `DISTRIBUTED BY`
  clause in dump, executed only on QD.  Segments rely on the
  `cp -rp` image (segments don't have this catalog populated as a
  matter of GP semantics).
- **AO aux contents (`pg_aoseg.*`)**: per-segment, must be rebuilt.
  `get_rel_infos()` captures OLD QE's
  `pg_aoseg/pg_aocsseg/pg_aovisimap/pg_aoblkdir` rows into
  `RelInfo->aosegments/...` (`info.c:717-869`).
  `restore_aosegment_tables()` in
  `greenplum/aotable.c:242-281` TRUNCATEs each aux table on the
  NEW cluster and re-INSERTs the captured rows verbatim.
- **PAX aux (`pg_ext_aux.pg_pax_blocks_<oid>` /
  `pg_manifest_<oid>` / `pg_pax_fastsequence`)**: same pattern.
  `pax_capture_rel_state()` in
  `greenplum/pg_upgrade_pax.c:102-253` grabs OLD QE's rows;
  `restore_pax_aux_table()` in `greenplum/check_pax.c:419-579`
  TRUNCATEs + INSERTs them on the NEW QE.
- **fastsequence**: per-segment, captured at
  `pg_upgrade_pax.c:227-236`, restored as DELETE+INSERT
  (`check_pax.c:544-560`).  AO `gp_fastsequence` is upgraded via
  **relfilenode transfer** (treated as a regular user table — see
  `info.c:531-533` whitelist), which is why `freeze_master_data()`
  is delayed until after transfer (`pg_upgrade.c:296-318`).

## 2. Potential inconsistency hot-spots

The mechanisms above leave six load-bearing invariants whose
violation causes silent or catastrophic divergence between catalog
and data:

1. **Segment-side `pg_class.relfilenode` diverges from the actual
   data file path until rename.**  The QD's relfilenode is rsynced
   to segments.  On the OLD segment the file is at
   `base/<dboid>/<OLD_QE_relfilenode>`; on the NEW segment the
   catalog says `<QD's_relfilenode>` but the transfer code is given
   `old_relfilenode = OLD_QE_relfilenode` (from segment's own
   `get_rel_infos`) and `new_relfilenode = QD_relfilenode` (from the
   rsynced catalog).  Works as long as OLD QD's `pg_class` is not
   hot-modified between QD freeze and the segment rsync — if it is,
   mappings drift silently.

2. **`indisvalid = false` filter is dropped on segments**
   (`info.c:578`).  Bitmap/BRIN indexes get invalidated on QD
   **after** segment rsync (`pg_upgrade.c:313`
   `new_gpdb_invalidate_bitmap_indexes`).  The fix relies on doing
   the invalidation only after `transfer_all_new_tablespaces`, so the
   rsynced segment catalog still sees the index as valid and
   `get_rel_infos` on both sides agree.  Any future code path that
   invalidates indexes earlier breaks segment upgrade.

3. **`relfrozenxid` reset relies on a two-pass scheme.**
   `prepare_new_cluster()` runs `VACUUM FREEZE` on QD only
   (`pg_upgrade.c:700`, gated by `is_greenplum_dispatcher_mode()`)
   — explicitly skipped on segments because AO/AOCS data files
   aren't transferred yet (lines 717-724, citing GP commit
   592c5c30ba).  Segments rely on `update_db_xids()`
   (`pg_upgrade.c:259-260`) to fix relfrozenxid post-transfer.  If
   AO aux contents and relfrozenxid get out of sync (e.g. tuple
   xmin > relfrozenxid), subsequent VACUUM errors out — this is
   what `freeze_master_data()` at line 317 targets, especially for
   `gp_fastsequence`.

4. **PAX `pg_pax_fastsequence` seed-row collision.**  CREATE TABLE
   on PAX inserts a fastsequence row with seq=0 via
   `CPaxInitializeFastSequenceEntry`.  The restore code uses
   DELETE+INSERT rather than ON CONFLICT (the index on objid is
   non-unique) to absorb this seed row.  Any future change that
   suppresses the seed row still works; any change that adds a
   unique constraint makes the DELETE+INSERT mandatory rather than
   defensive.

5. **`pg_ext_aux.pg_manifest_<oid>` orphans.**  `get_rel_infos`
   (`info.c:522-530`) filters out aux tables whose parent OID is
   missing or in a temp namespace — comment cites a real
   segment-mode failure where an orphan caused "No match found in
   new cluster".  Any future PAX aux naming change beyond
   `pg_pax_blocks_` / `pg_manifest_` bypasses this filter.

6. **Segments carry QD-populated `gp_distribution_policy` rows.**
   These come from the `cp -rp` image even though
   `create_new_objects()` is gated off on segments.  Segments don't
   consult their own copy, so this is a quiet inconsistency rather
   than a functional bug — but it is a class of "rsync side-effects"
   that any change to QD-side post-transfer write paths should be
   aware of.

## 3. Per-commit problem matrix

Commits in this stream (oldest → newest, on top of `origin/main`):

| # | SHA | Problem solved | Touches risk |
|---|---|---|---|
| 1 | `76fef74f971` | **PAX round-trip support** — make PAX tables survive `pg_dump --binary-upgrade` + `pg_upgrade --link` round-trip: relfilenode transfer for `<oid>_pax/` dir, aux-relation collection in `get_rel_infos`, and a 4-layer test pyramid. Includes the `pg_ext_aux` orphan filter in segment-mode `get_rel_infos` and the defensive Assert in `pax_capture_rel_state`. | §1.3 (PAX transfer) + #5 |
| 2 | `10ee229cde6` | **pg_dump: partitioned roots need `USING <am>`** — `pg_dump` was omitting the access-method clause for partitioned-table roots, so PAX/AO partition roots restored as heap on the new cluster and child partitions then failed to attach. | §1.1 (DDL fidelity) |
| 3 | `f632dcf8eb2` | **PAX compresslevel / compresstype validation** — the per-column reloption check did not run when the option was supplied at table level; rejects invalid combos earlier (e.g. `compresslevel > 0` without `compresstype`). | (unrelated to upgrade; PAX hardening) |
| 4 | `b5a3d82efcb` | **PAX `pg_class.relfrozenxid` invariant** — three independent code paths (`cluster.c::swap_relation_files`, PAX `RelationVacuum`, `RelationCopyForCluster`) leaked non-zero values into PAX rels' relfrozenxid/relminmxid.  Hooks `IsAccessMethodPAX` into the AO whitelist and adds a regression test exercising every CREATE/DML/VACUUM/CLUSTER/REINDEX/MV path. | §1.7 + #3 |
| 5 | `0ccb44040eb` | **PAX OOM error message** — replace the bare function-name fallback with the underlying OOM diagnostic from `MemoryContextStats`. | (unrelated to upgrade; PAX diagnostics) |
| 6 | `eb631525dab` | **pg_upgrade: remove obsolete skip check** in `version.c` that no longer applies. | (cleanup) |
| 7 | `5e1d089d749` | **`RelationStorageIs{AoRows,AoCols}` handler-based check** — these macros tested `rd_rel->relam` against canonical AO OIDs, which broke user-defined access methods that share the canonical handler (e.g. `ao_row_testam`).  pg_upgrade's post-restore `VACUUM FREEZE` aborted in `aocssegfiles.c:519` on such relations.  Switch to handler-based checks matching `RelationStorageIsAO`. | §1.7 (AO storage typing) |
| 8 | `c6787c6e465` | **REINDEX AO/CO BRIN inline** — bitmap and AO/CO BRIN indexes invalidated by upgrade are reindexed in-process at line 314 (`new_reindex_ao_brin_indexes`) instead of left to a follow-up shell script.  Re-establishes the timing invariant described in §2#2 (invalidation must follow transfer). | §1.7 + #2 |
| 9 | `bc7c135d05f` | **CREATE FOREIGN TABLE … DISTRIBUTED forwarding** — completely unrelated to PAX/pg_upgrade; included in this branch for review convenience. | (orthogonal) |
| 10 | `9a0b9937746` | **pg_dump explicit orphan filter** — symmetric with `info.c`'s filter (in commit 1): explicitly excludes `pg_temp_*`/`pg_toast_temp_*`/`pg_ext_aux` orphans at the SQL query level so they never enter `pg_dump`'s in-memory rel array.  Defensive against future `selectDumpableNamespace` changes. | #5 (orphan filter symmetry) |
| 11 | `a191aae4035` | **Revert "advance old cluster checkpoint counters"** — the take-max behavior of `adjust_old_cluster()` violated the per-postmaster invariant on segments: `chkpnt_nxtxid` was promoted to the QD's value while the segment's `pg_xact` file still covered only the segment's lower XID range, causing postmaster startup to abort in `TrimCLOG` with "could not access status of transaction <high XID>". | §1.4 (segment counter semantics) |
| 12 | `a8672a8f129` | **Master-side pg_class freeze fix** — two interacting issues: (a) plain `VACUUM FREEZE` skips visibility-map all-visible pages, leaving recently-inserted tuples unfrozen; (b) AO/AOCS/PAX `vacuum_rel` recursion into PRE_CLEANUP / COMPACT / POST_CLEANUP sub-phases writes new pg_class rows whose xmin == own snapshot.xmin == FreezeLimit, never freezable in the same call.  Fix: pass `DISABLE_PAGE_SKIPPING`, then re-run `VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) pg_catalog.pg_class` to catch the leftover unfrozen pg_class rows. | §1.2, §1.4, §1.7 + #3 |
| 13 | `9d05f154373` | **`update_db_xids` exclude AO/AOCS/PAX** — the first UPDATE in `update_db_xids` was a reverse storage-type proxy (`AND NOT relfrozenxid = 0`).  Replaced with an explicit access-method filter via `relam NOT IN (AO_ROW, AO_COLUMN, PAX)` so heap relations whose relfrozenxid is legitimately 0 are still UPDATEd, and the AO/PAX invariant is enforced positively. | §1.7 + #3 |

## 4. Recap — which invariants this PR strengthens

- **#2 (invalidation ordering)** — `c6787c6e465` keeps the
  invalidation strictly after `transfer_all_new_tablespaces`.
- **#3 (relfrozenxid two-pass)** — `b5a3d82efcb` + `a8672a8f129` +
  `9d05f154373` close every leak path that gave PAX/AO relations a
  non-zero `relfrozenxid`, and ensure all heap catalog tuples are
  actually frozen before cp -rp.
- **#5 (PAX aux orphans)** — `76fef74f971` (in `info.c`) and
  `9a0b9937746` (in `pg_dump.c`) symmetrically filter out orphans.
- **§1.4 (segment counter semantics)** — `a191aae4035` restores the
  per-postmaster invariant by reverting the take-max behavior.
- **§1.7 (AO storage typing)** — `5e1d089d749` makes the
  storage-class macros agree with their `Rel*` counterparts.

## 5. Key files referenced

- `src/bin/pg_upgrade/pg_upgrade.c:96-354` — `main()` flow.
- `src/bin/pg_upgrade/relfilenode.c:37-340` — transfer dispatcher.
- `src/bin/pg_upgrade/info.c:40-179` — `gen_db_file_maps`;
  `:441-896` — `get_rel_infos`.
- `src/bin/pg_upgrade/test_gpdb.sh:169-208, 463-491` — multi-node
  orchestration.
- `src/bin/pg_dump/pg_dump.c:5449-5664` —
  `binary_upgrade_set_pg_class_oids_impl`.
- `src/backend/utils/adt/pg_upgrade_support.c:42-299` — OID
  preassignment backend.
- `src/bin/pg_upgrade/greenplum/aotable.c:55-281` —
  `restore_aosegment_tables`.
- `src/bin/pg_upgrade/greenplum/pg_upgrade_pax.c:102-253` — PAX
  state capture.
- `src/bin/pg_upgrade/greenplum/check_pax.c:282-579` —
  `transfer_pax_relfilenode_dir`, `restore_pax_aux_table`.
- `src/bin/pg_upgrade/greenplum/controldata_gp.c:49` —
  `freeze_master_data`.
- `src/bin/pg_upgrade/greenplum/frozenxids_gp.c` — `update_db_xids`.
