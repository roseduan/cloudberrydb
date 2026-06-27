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
 * pg_dump_pax_emit.c
 *	  PAX-specific binary-upgrade OID emission, linked into pg_dump only.
 *
 * pg_dump_pax.c is shared by pg_dump, pg_restore, and pg_dumpall (it's
 * just lookup helpers).  The orchestration in this file calls
 * binary_upgrade_set_pg_class_oids_impl() and
 * binary_upgrade_set_type_oids_by_rel_oid_impl(), which live in
 * pg_dump.c — so this object can only be linked into pg_dump.  Keeping
 * the call site as a one-line invocation from pg_dump.c lets us avoid
 * sprinkling PAX-aware logic across the core file.
 *
 * Portions Copyright (c) 2026-Present, HashData Technology Limited.
 *
 * IDENTIFICATION
 *	  src/bin/pg_dump/pg_dump_pax_emit.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "pg_backup_archiver.h"
#include "pg_dump_pax.h"

/*
 * pax_emit_aux_oid_preassignment
 *
 * Single entry point used by pg_dump.c::binary_upgrade_set_pg_class_oids_impl()
 * to splice PAX OID preassignment into the binary-upgrade dump stream.
 * For each PAX leaf relation we queue the OIDs of
 *
 *   pg_ext_aux.pg_pax_blocks_<reloid>     (catalog mode)
 *     -- or --
 *   pg_ext_aux.pg_manifest_<reloid>       (manifest mode)
 *
 * plus, in catalog mode, the aux's btree index.  This lets pg_upgrade
 * hard-link the on-disk <relfilenode>_pax/ directory without having to
 * rebuild the aux on the new cluster.
 *
 * No-op for relations that aren't PAX leaves: non-PAX relations exit
 * via the relam check, and partition roots fall through pax_get_aux_oids()
 * returning false (no aux row exists for them).
 */
void
pax_emit_aux_oid_preassignment(Archive *fout,
							   PQExpBuffer upgrade_buffer,
							   Oid pg_class_oid,
							   Oid pg_class_relam)
{
	PaxAuxOidInfo	pax_aux;

	if (pg_class_relam != PAX_TABLE_AM_OID)
		return;

	if (!pax_get_aux_oids(fout, pg_class_oid, &pax_aux))
		return;					/* partition root or no aux row */

	/*
	 * aux_relname is "pg_pax_blocks_<oid>" or "pg_manifest_<oid>"
	 * depending on the source cluster's PAX backend — we don't care
	 * which here, we just use whatever name pax_get_aux_oids resolved.
	 */
	binary_upgrade_set_pg_class_oids_impl(fout, upgrade_buffer,
										  pax_aux.aux_oid, false,
										  pax_aux.aux_relname);
	binary_upgrade_set_type_oids_by_rel_oid_impl(fout, upgrade_buffer,
												 pax_aux.aux_oid,
												 pax_aux.aux_relname);

	/*
	 * Manifest-mode aux table is a single-row pointer with no index;
	 * only catalog-mode has a btree to preassign.
	 */
	if (OidIsValid(pax_aux.aux_idx_oid))
	{
		char	pax_aux_idxname[64];

		snprintf(pax_aux_idxname, sizeof(pax_aux_idxname),
				 "%s_idx", pax_aux.aux_relname);
		binary_upgrade_set_pg_class_oids_impl(fout, upgrade_buffer,
											  pax_aux.aux_idx_oid, true,
											  pax_aux_idxname);
	}
}
