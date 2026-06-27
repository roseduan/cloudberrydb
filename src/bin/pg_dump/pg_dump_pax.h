/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * pg_dump_pax.h
 *	  PAX-specific symbols and helpers used by pg_dump.
 *
 * PAX (column-optimized storage) is registered as a table access method by
 * the pax_storage contrib module via pax-cdbinit--1.0.sql, not by core
 * pg_am.dat.  Its OID is therefore not surfaced by genbki.pl into
 * pg_am_d.h, so frontend code that needs to recognise PAX tables keeps a
 * private copy of the OID here.  The value must stay in sync with
 * contrib/pax_storage/pax-cdbinit--1.0.sql.
 *
 * All PAX-specific logic that pg_dump needs lives in pg_dump_pax.c so the
 * core pg_dump.c only invokes a few hook functions and stays free of PAX
 * knowledge.
 * pax_access_handle.cc
 *
 * Portions Copyright (c) 2026-Present, HashData Technology Limited.
 *
 * IDENTIFICATION
 *	  src/bin/pg_dump/pg_dump_pax.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DUMP_PAX_H
#define PG_DUMP_PAX_H

#include "postgres_fe.h"
#include "pg_backup_archiver.h"

#define PAX_TABLE_AM_OID 7047

/*
 * PAX has two metadata backends, selected at PAX build time:
 *
 *   CATALOG  — per-relation aux heap pg_ext_aux.pg_pax_blocks_<reloid>
 *              holds one row per micro-partition (block_id, tuple count,
 *              size, statistics protobuf, visimap name, flags).
 *
 *   MANIFEST — per-relation aux heap pg_ext_aux.pg_manifest_<reloid> holds
 *              one TEXT row pointing at a JSON file inside the relation's
 *              <relfilenode>_pax/ directory; the JSON contains the full
 *              block list (yyjson serialization, .meta suffix).
 *
 * For pg_dump and pg_upgrade we don't take sides at compile time — both
 * tools detect the backend at runtime by probing pg_ext_aux for whichever
 * aux table exists.  The same binary handles both source clusters.
 *
 * NONE is returned for relations that aren't PAX leaves (partitioned
 * roots, etc.) — for those the caller should skip PAX-specific work.
 */
typedef enum PaxBackendKind
{
	PAX_BACKEND_NONE = 0,
	PAX_BACKEND_CATALOG,
	PAX_BACKEND_MANIFEST,
} PaxBackendKind;

/*
 * OIDs of the PAX per-relation auxiliary objects we have to preassign
 * during --binary-upgrade so that pg_upgrade can hard-link the on-disk
 * PAX micro-partition directory unchanged.
 *
 *   kind           — which backend this PAX table uses
 *   aux_oid        — pg_class.oid of pg_ext_aux.pg_pax_blocks_<parent_oid>
 *                    (catalog mode) or pg_ext_aux.pg_manifest_<parent_oid>
 *                    (manifest mode)
 *   aux_relname    — the actual relname (used for the
 *                    binary_upgrade_set_next_*_pg_class_oid name argument)
 *   aux_idx_oid    — pg_class.oid of the (single) btree index on aux_oid,
 *                    InvalidOid if the aux table has none (manifest mode's
 *                    single-row aux table has no index)
 *
 * Each aux table is a normal heap, so its own TOAST table, TOAST index,
 * and rowtype OIDs are picked up automatically when we recurse into
 * binary_upgrade_set_pg_class_oids_impl() from pg_dump.c.
 */
typedef struct PaxAuxOidInfo
{
	PaxBackendKind	kind;
	Oid				aux_oid;
	char			aux_relname[64];	/* "pg_pax_blocks_<oid>" or "pg_manifest_<oid>" */
	Oid				aux_idx_oid;		/* InvalidOid if aux table has no index */
} PaxAuxOidInfo;

/*
 * Detect the PAX backend used by the source cluster.  Result is cached
 * after the first call; subsequent calls don't re-probe.
 *
 * Detection rule (instructed by the PAX team):
 *
 *   If the global catalog table pg_ext_aux.pg_pax_tables EXISTS in the
 *   source cluster, the cluster was built with USE_PAX_CATALOG=ON and
 *   every PAX table on it uses the catalog backend (pg_pax_blocks_<oid>).
 *
 *   Otherwise the cluster is manifest-only (USE_PAX_CATALOG=OFF), and
 *   every PAX table uses the manifest backend (pg_manifest_<oid> aux
 *   table + .meta JSON file in <relfilenode>_pax/).
 *
 *   If the PAX AM isn't installed at all (no pg_am row for 'pax'),
 *   returns PAX_BACKEND_NONE.
 *
 * The backend choice is cluster-wide because it's a PAX compile-time
 * option — every PAX table on a single cluster picks the same backend.
 */
extern PaxBackendKind pax_cluster_backend_kind(Archive *fout);

/*
 * Look up the per-relation PAX aux table for the given PAX main relation
 * OID using the cluster-wide backend kind.  Returns true and fills *out
 * if the aux table exists (which it should for every non-partitioned
 * PAX leaf); false for partition roots, for non-PAX relations, or when
 * PAX itself isn't installed.
 *
 * Caller is expected to have already established that this relation has
 * pg_class.relam == PAX_TABLE_AM_OID — this function does not re-validate.
 */
extern bool pax_get_aux_oids(Archive *fout, Oid pax_main_oid,
							 PaxAuxOidInfo *out);

/*
 * Emit PAX-specific OID preassignment SQL into upgrade_buffer, if this
 * relation is a PAX leaf.  No-op for non-PAX relations and for PAX
 * partition roots (which have no aux table).  Called from inside
 * pg_dump.c's binary_upgrade_set_pg_class_oids_impl(), as the single
 * entry point that PAX needs in the pg_dump core.
 *
 * pg_class_relam is the relation's relam — we don't re-query it here.
 * If it's not PAX_TABLE_AM_OID, we return immediately.
 */
extern void pax_emit_aux_oid_preassignment(Archive *fout,
										   PQExpBuffer upgrade_buffer,
										   Oid pg_class_oid,
										   Oid pg_class_relam);

/*
 * These two helpers live in pg_dump.c and emit "binary_upgrade_set_next_*"
 * SQL for a single relation OID under a caller-supplied relname (which
 * lets us reuse the same plumbing for both AO aux tables and PAX aux
 * tables).  We declare them here because pg_dump_pax.c is the only
 * external caller — pg_dump.c uses them internally too.
 */
extern void binary_upgrade_set_pg_class_oids_impl(Archive *fout,
												  PQExpBuffer upgrade_buffer,
												  Oid pg_class_oid,
												  bool is_index,
												  char *relname_override);
extern void binary_upgrade_set_type_oids_by_rel_oid_impl(Archive *fout,
														 PQExpBuffer upgrade_buffer,
														 Oid pg_rel_oid,
														 char *typname_override);

#endif							/* PG_DUMP_PAX_H */
