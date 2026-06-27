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
 * pg_dump_pax.c
 *	  PAX-specific helpers used by pg_dump.
 *
 * This file isolates PAX (column-optimized storage) knowledge so the core
 * pg_dump.c stays free of references to PAX-only catalogs.  Today the only
 * resident is pax_get_aux_oids(), used by --binary-upgrade emission to
 * resolve the per-relation pg_ext_aux.pg_pax_blocks_<reloid> auxiliary
 * heap and its index.  Future PAX-specific dump helpers belong here as
 * well.
 *
 * Portions Copyright (c) 2026-Present, HashData Technology Limited.
 *
 * IDENTIFICATION
 *	  src/bin/pg_dump/pg_dump_pax.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "pg_backup_archiver.h"
#include "pg_backup_db.h"
#include "pg_dump_pax.h"

/*
 * Cached cluster-wide PAX backend kind, populated lazily on the first
 * call to pax_cluster_backend_kind().  pg_dump runs against one source
 * cluster per invocation, so a process-static cache is fine.
 */
static PaxBackendKind pax_cached_kind = PAX_BACKEND_NONE;
static bool			  pax_cached_kind_valid = false;

PaxBackendKind
pax_cluster_backend_kind(Archive *fout)
{
	PQExpBuffer query;
	PGresult   *res;
	const char *pax_installed;
	const char *has_pax_tables;

	if (pax_cached_kind_valid)
		return pax_cached_kind;

	query = createPQExpBuffer();

	/*
	 * Two existence checks in one round trip:
	 *
	 *   pax_installed   — is the PAX table access method registered?
	 *                     If not, this cluster has no PAX at all and we
	 *                     return PAX_BACKEND_NONE.
	 *
	 *   has_pax_tables  — does the global registry pg_ext_aux.pg_pax_tables
	 *                     exist?  The PAX build creates it iff
	 *                     USE_PAX_CATALOG=ON.  Its presence is the
	 *                     definitive cluster-wide signal that the catalog
	 *                     backend is in use; absence (with PAX installed)
	 *                     means manifest mode.
	 */
	appendPQExpBufferStr(query,
		"SELECT "
		"  EXISTS(SELECT 1 FROM pg_catalog.pg_am "
		"         WHERE amname = 'pax') AS pax_installed, "
		"  EXISTS(SELECT 1 FROM pg_catalog.pg_class c "
		"         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
		"         WHERE n.nspname = 'pg_ext_aux' "
		"           AND c.relname = 'pg_pax_tables') AS has_pax_tables");

	res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

	pax_installed  = PQgetvalue(res, 0, PQfnumber(res, "pax_installed"));
	has_pax_tables = PQgetvalue(res, 0, PQfnumber(res, "has_pax_tables"));

	if (pax_installed[0] != 't')
		pax_cached_kind = PAX_BACKEND_NONE;
	else if (has_pax_tables[0] == 't')
		pax_cached_kind = PAX_BACKEND_CATALOG;
	else
		pax_cached_kind = PAX_BACKEND_MANIFEST;

	pax_cached_kind_valid = true;

	PQclear(res);
	destroyPQExpBuffer(query);
	return pax_cached_kind;
}

bool
pax_get_aux_oids(Archive *fout, Oid pax_main_oid, PaxAuxOidInfo *out)
{
	PQExpBuffer query;
	PGresult   *res;
	PaxBackendKind kind;
	const char *aux_prefix;

	/* default to NONE so callers reading uninitialised `out` see a safe value */
	out->kind = PAX_BACKEND_NONE;
	out->aux_oid = InvalidOid;
	out->aux_idx_oid = InvalidOid;
	out->aux_relname[0] = '\0';

	kind = pax_cluster_backend_kind(fout);
	if (kind == PAX_BACKEND_NONE)
		return false;

	/* Derive per-relation aux relname from the cluster-wide mode. */
	aux_prefix = (kind == PAX_BACKEND_CATALOG) ? "pg_pax_blocks_" : "pg_manifest_";
	snprintf(out->aux_relname, sizeof(out->aux_relname),
			 "%s%u", aux_prefix, pax_main_oid);

	/*
	 * Look up the aux relation by its (cluster-mode-derived) name.  Yields
	 * zero rows for partition roots and for any PAX-AM relation that for
	 * some reason has no aux table — caller treats that as "nothing to do".
	 *
	 * Catalog-mode aux tables have a single btree index; manifest-mode
	 * aux tables don't (the .meta file on disk holds the block list).
	 * LEFT JOIN handles both shapes.
	 */
	query = createPQExpBuffer();
	appendPQExpBuffer(query,
					  "SELECT c.oid                              AS aux_oid, "
					  "       coalesce(idx.indexrelid::text, '0') AS aux_idx_oid "
					  "FROM   pg_catalog.pg_class c "
					  "JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
					  "LEFT JOIN pg_catalog.pg_index idx ON idx.indrelid = c.oid "
					  "WHERE  n.nspname = 'pg_ext_aux' "
					  "  AND  c.relname = '%s' ",
					  out->aux_relname);

	res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

	if (PQntuples(res) == 1)
	{
		out->kind = kind;
		out->aux_oid = atooid(PQgetvalue(res, 0,
										 PQfnumber(res, "aux_oid")));
		out->aux_idx_oid = atooid(PQgetvalue(res, 0,
											 PQfnumber(res, "aux_idx_oid")));
	}

	PQclear(res);
	destroyPQExpBuffer(query);
	return (out->kind != PAX_BACKEND_NONE);
}
