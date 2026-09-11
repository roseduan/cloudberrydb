/*-------------------------------------------------------------------------
 *
 * pg_upgrade_pax.h
 *	  PAX-specific types and helpers for pg_upgrade.
 *
 * This header isolates everything PAX-aware from the core pg_upgrade
 * header.  pg_upgrade.h only sees an opaque `struct PaxRelState *pax`
 * pointer on RelInfo; full definitions live here.  Files that actually
 * touch PAX-specific fields (info.c when capturing source state,
 * check_pax.c when transferring / restoring) include this header
 * directly.  All other core pg_upgrade .c files compile unchanged.
 *
 *	  src/bin/pg_upgrade/greenplum/pg_upgrade_pax.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_UPGRADE_PAX_H
#define PG_UPGRADE_PAX_H

#include "libpq-fe.h"

/* Forward decl from pg_upgrade.h — included indirectly by callers. */
struct relinfo;
typedef struct relinfo RelInfo;

/*
 * PAX has two metadata backends, selected at PAX build time:
 *
 *   CATALOG  — per-relation aux heap pg_ext_aux.pg_pax_blocks_<reloid>
 *              with one row per micro-partition.
 *   MANIFEST — per-relation aux heap pg_ext_aux.pg_manifest_<reloid>
 *              with a single TEXT row pointing at a JSON file inside
 *              the <relfilenode>_pax/ directory.
 *
 * pg_upgrade detects which backend the source cluster uses once per
 * database (via pax_detect_cluster_backend), and stamps every PAX
 * leaf's PaxRelState with that kind.  Both backends share the
 * physical <relfilenode>_pax/ directory layout, so the file-transfer
 * code in check_pax.c doesn't care which is in use.
 */
typedef enum PaxBackendKind
{
	PAX_BACKEND_NONE = 0,	/* not a PAX leaf, or unknown */
	PAX_BACKEND_CATALOG,	/* pg_pax_blocks_<reloid> */
	PAX_BACKEND_MANIFEST,	/* pg_manifest_<reloid> */
} PaxBackendKind;

/*
 * Catalog-backend payload: one row of pg_ext_aux.pg_pax_blocks_<reloid>
 * for a PAX leaf table.  Each row describes one micro-partition: its
 * block_id, row count, on-disk size, statistics (paxauxstats varlena
 * captured as hex-encoded bytea — see design doc §13.2.1), optional
 * visimap file reference, and flags.
 */
typedef struct
{
	int32		ptblockname;
	int32		pttupcount;
	int32		ptblocksize;
	char	   *ptstatistics;	/* hex-encoded bytea round-trip */
	char	   *ptvisimapname;	/* NULL if no visimap */
	bool		ptexistexttoast;
	bool		ptisclustered;
} PaxBlockInfo;

/*
 * Manifest-backend payload: the path stored in the single row of
 * pg_ext_aux.pg_manifest_<reloid>.  Format is "<uuid>_<txid>.meta",
 * resolved against the relation's <relfilenode>_pax/ directory.  The
 * actual block metadata lives in that JSON file; pg_upgrade does not
 * need to parse it because the file rides on the directory hard-link
 * done by transfer_pax_relfilenode_dir().
 */
typedef struct
{
	char	   *path;	/* manifest filename, relative to _pax/ dir */
} PaxManifestInfo;

/*
 * Per-relation PAX state.  Allocated on-demand by pax_capture_rel_state
 * for every PAX leaf (relkind in ('r','m') AND relam='pax' AND has an
 * aux entry in pg_ext_aux); NULL on every other relation (heap, AO,
 * AOCS, partition root).
 *
 * CATALOG backend:
 *   blocks[0..nblocks-1] — one row per micro-partition, restored into
 *   pg_ext_aux.pg_pax_blocks_<reloid>.
 *
 * MANIFEST backend:
 *   manifest.path — the JSON filename inside the relation's
 *   <relfilenode>_pax/ directory; restored as a single row in
 *   pg_ext_aux.pg_manifest_<reloid>.  The JSON file itself rides on
 *   the directory hard-link done by transfer_pax_relfilenode_dir.
 *
 * Both backends share pg_ext_aux.pg_pax_fastsequence — captured in
 * fastseq / has_fastseq for both.
 *
 * On QD upgrades the capture is typically empty (PAX data lives on
 * segments); restore_pax_aux_tables() short-circuits.
 */
typedef struct PaxRelState
{
	PaxBackendKind	backend;
	PaxBlockInfo   *blocks;
	int				nblocks;
	PaxManifestInfo	manifest;
	int32			fastseq;	/* pg_pax_fastsequence.seq for this oid */
	bool			has_fastseq;
} PaxRelState;

/* ---- entry points called from pg_upgrade core ---- */

/*
 * One-time cluster-wide detect of the source's PAX backend kind.
 * pg_ext_aux.pg_pax_tables existence ↔ catalog mode (rule mirrors
 * pg_dump_pax.c); absence ↔ manifest mode; pg_am missing 'pax' ↔ NONE.
 *
 * Called once per database in get_rel_infos(); the returned kind is
 * passed to pax_capture_rel_state() for every relation in that DB.
 */
extern PaxBackendKind pax_detect_cluster_backend(PGconn *conn);

/*
 * Capture the per-relation PAX state for a single relation found in
 * `res` row `relnum`.  If the row's amname isn't 'pax', or relkind
 * isn't 'r' or 'm', or no aux table exists, this is a no-op and
 * rel->pax stays NULL.  Otherwise allocates a PaxRelState and fills it
 * by issuing additional queries on `conn`.
 *
 * Pass i_amname = -1 if the result set doesn't include an amname
 * column (e.g. old-cluster pre-7 queries) — in that case the function
 * skips entirely.
 */
extern void pax_capture_rel_state(RelInfo *rel,
								  PGconn *conn,
								  PaxBackendKind cluster_kind,
								  PGresult *res,
								  int relnum,
								  int i_amname,
								  char relkind);

/*
 * Initialise rel->pax to a known-empty state.  Called before every
 * relation's PAX capture so subsequent code can rely on the field
 * being a valid (possibly NULL) pointer.
 */
extern void pax_init_rel_state(RelInfo *rel);

/* Free a PaxRelState (and its embedded blocks/manifest path). */
extern void pax_free_rel_state(struct PaxRelState *st);

/*
 * True if `rel` is a PAX leaf — i.e. its capture succeeded.  Core code
 * uses this to set old_rel->reltype = PAX in gen_db_file_maps without
 * peeking inside the opaque struct.
 */
extern bool pax_rel_has_state(const RelInfo *rel);

#endif							/* PG_UPGRADE_PAX_H */
