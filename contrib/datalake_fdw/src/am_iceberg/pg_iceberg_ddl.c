#include "postgres.h"

#include "access/tableam.h"
#include "access/relation.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_lake_table.h"
#include "commands/laketablecmds.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "cdb/cdbvars.h"
#include "utils/timestamp.h"
#include "parser/parse_type.h"
#include "executor/spi.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_type.h"
#include "utils/syscache.h"
#include "fmgr.h"
#include "cdb/cdbdisp_query.h"

#include "../iceberg_catalog_fdw/iceberg_catalog_fdw.h"
#include "include/pg_iceberg_ddl.h"
#include "include/pg_iceberg_am_handler.h"
#include "include/pg_iceberg_catalog.h"
#include "include/pg_iceberg_metadata_tracker.h"
#include "include/pg_iceberg_deletion_queue.h"

static object_access_hook_type old_objectaccess_hook = NULL;

static void iceberg_object_access_hook(ObjectAccessType access, Oid classId,
									   Oid objectId, int subId, void *arg);

void
pg_iceberg_setup_ddl_hooks(void)
{
	old_objectaccess_hook = object_access_hook;
	object_access_hook = iceberg_object_access_hook;

	pg_iceberg_init_metadata_tracking();
}

static void
iceberg_object_access_hook(ObjectAccessType access, Oid classId, Oid objectId,
							int subId, void *arg)
{
	Relation rel;

	if (access == OAT_POST_CREATE && classId == LakeTableRelationId)
	{
		rel = relation_open(objectId, AccessShareLock);

		if ((rel->rd_rel->relkind == RELKIND_RELATION ||
			rel->rd_rel->relkind == RELKIND_MATVIEW) &&
			(subId == 0) && is_iceberg_rel(rel))
		{
			/*
			 * Issue #337: also guard the LakeTable / CREATE ICEBERG TABLE
			 * path here.  iceberg_relation_set_new_filenode already covers
			 * plain CREATE TABLE ... USING iceberg, but the lake-table
			 * custom DDL would otherwise reach pg_iceberg_add_metadata
			 * before the iceberg.pg_iceberg_metadata catalog table exists.
			 */
			pg_iceberg_require_extension_installed();

			if (Gp_role == GP_ROLE_DISPATCH)
			{
				bool is_internal;
				char *metadata_location = pg_iceberg_create_table_with_catalog(rel, &is_internal);

				/*
				 * default_spec_id 0 is correct for both unpartitioned and
				 * PARTITION BY tables: a freshly created table's first spec
				 * (empty or not) always gets id 0.  Only spec evolution --
				 * which we do not support -- assigns higher ids; for adopted
				 * pre-existing tables the spec is cross-checked against the
				 * declaration in pg_iceberg_create_table_with_catalog.
				 */
				pg_iceberg_add_metadata(objectId, metadata_location, NULL, is_internal, 0);
				pfree(metadata_location);
			}
		}

		relation_close(rel, AccessShareLock);
	}
	else if (access == OAT_DROP && classId == RelationRelationId)
	{
		rel = relation_open(objectId, AccessShareLock);

		if ((rel->rd_rel->relkind == RELKIND_RELATION ||
			rel->rd_rel->relkind == RELKIND_MATVIEW) &&
			(subId == 0) && is_iceberg_rel(rel))
		{
			/*
			 * On QD we have to (a) gather the credential context that the
			 * async consumer will need (volume/server/owner_username/qname)
			 * BEFORE pg_lake_table is cleared, because pg_iceberg_get_table_info
			 * resolves the volume/server through pg_lake_table; and (b)
			 * enqueue + remove the metadata.  RemoveLakeTableEntry is the
			 * per-node tail step, so it runs after the QD-only block.
			 */
			if (Gp_role == GP_ROLE_DISPATCH)
			{
				IcebergMetadataInfo *meta_info =
					pg_iceberg_get_metadata_info_missing_ok(objectId);

				/*
				 * Tolerate a missing metadata entry (e.g. a table created
				 * with a bare CREATE TABLE ... USING iceberg before that
				 * path was blocked): the table must remain droppable.
				 */
				if (meta_info != NULL)
				{
					/*
					 * Only builtin (internal-catalog) iceberg tables own their
					 * files and must have them cleaned up on DROP.  External-
					 * catalog tables (is_internal == false) are managed by an
					 * external Iceberg catalog that owns the storage, so we never
					 * enqueue file deletion for them -- only the local pg_iceberg
					 * bookkeeping is dropped below.
					 */
					if (meta_info->is_internal)
					{
						IcebergTableInfo   *table_info;
						char               *owner_username;
						char               *table_qname;
						char               *nspname;
						const char         *relname;

						/*
						 * Resolve the storage context while pg_lake_table is still
						 * intact.  pg_iceberg_get_table_info goes pg_lake_table ->
						 * pg_foreign_volume -> pg_foreign_server to fill in the
						 * volume_name / volume_server_name we need to reconstruct
						 * fileIOConfig on the consumer side.
						 */
						table_info = pg_iceberg_get_table_info(objectId);

						owner_username = GetUserNameFromId(GetUserId(), false);

						nspname = get_namespace_name(rel->rd_rel->relnamespace);
						relname = NameStr(rel->rd_rel->relname);
						table_qname = nspname ?
							psprintf("%s.%s", nspname, relname) :
							psprintf("%s", relname);

						/*
						 * Enqueue the metadata path with full credential context.
						 * The autovacuum-driven consumer (pg_iceberg_av_consumer.c)
						 * will pick it up, reconstruct fileIOConfig, and call
						 * dlagent's /v1/files/cleanup-from-metadata endpoint.
						 */
						pg_iceberg_deletion_queue_insert(
							meta_info->metadata_location,
							objectId,
							table_info->volume_name,
							table_info->volume_server_name,
							owner_username,
							table_qname,
							GetCurrentTimestamp(),
							DELETION_TYPE_METADATA);

						pg_iceberg_free_table_info(table_info);

						if (nspname)
							pfree(nspname);
						pfree(table_qname);
					}

					pg_iceberg_free_metadata_info(meta_info);
					pg_iceberg_remove_metadata(objectId);
				}
				else
					ereport(WARNING,
						(errmsg("iceberg metadata entry not found for table \"%s\", skipping iceberg metadata cleanup",
							RelationGetRelationName(rel))));
			}

			/*
			 * pg_lake_table is a per-node catalog: clean on both QD and QE.
			 * Replaces the former hardcoded RemoveLakeTableEntry() call in
			 * heap_drop_with_catalog().  Must run after the QD-only block
			 * above because pg_iceberg_get_table_info reads pg_lake_table.
			 */
			RemoveLakeTableEntry(objectId);
		}

		relation_close(rel, AccessShareLock);
	}

	if (old_objectaccess_hook)
		old_objectaccess_hook(access, classId, objectId, subId, arg);
}

/*
 * pg_iceberg_truncate_table
 *    Truncate a builtin-catalog iceberg table: commit a metadata-only delete
 *    of all rows (new empty snapshot) and enqueue the pre-truncate metadata
 *    tree for async deletion.  Driven by the TRUNCATE ProcessUtility hook.
 *
 *    QD-only (catalog/agent reachable only from the dispatcher).  Both the
 *    deletion-queue insert and the metadata pointer swap are transactional, so
 *    an aborted TRUNCATE keeps the old files and leaves the table unchanged.
 *    Only the newly-written empty metadata.json would leak on abort -- the same
 *    orphan class tracked for the abort-time metadata cleanup follow-up.
 *
 *    Scope: only native iceberg tables on the builtin catalog.  A non-builtin
 *    (external-catalog) iceberg table errors out -- its storage is owned by the
 *    external catalog.
 */
void
pg_iceberg_truncate_table(Oid relid)
{
	Relation				rel;
	IcebergMetadataInfo	   *meta_info;
	IcebergTableInfo	   *table_info;
	char				   *old_metadata;
	char				   *new_metadata;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	rel = relation_open(relid, AccessShareLock);

	if (!((rel->rd_rel->relkind == RELKIND_RELATION ||
		   rel->rd_rel->relkind == RELKIND_MATVIEW) &&
		  is_iceberg_rel(rel)))
	{
		relation_close(rel, AccessShareLock);
		return;
	}

	meta_info = pg_iceberg_get_metadata_info_missing_ok(relid);
	if (meta_info == NULL)
	{
		/* bare CREATE TABLE ... USING iceberg with no metadata: nothing to do */
		relation_close(rel, AccessShareLock);
		return;
	}

	table_info = pg_iceberg_get_table_info(relid);

	if (!pg_iceberg_is_builtin_catalog(table_info->catalog_server_name))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("TRUNCATE is only supported for builtin-catalog iceberg tables"),
				 errdetail("table \"%s\" is on an external catalog that owns its storage",
						   RelationGetRelationName(rel))));

	old_metadata = pstrdup(meta_info->metadata_location);

	/* Agent commits a metadata-only delete of all rows; returns new metadata. */
	new_metadata = pg_iceberg_truncate_with_catalog(rel, table_info, old_metadata);

	/*
	 * Already-empty table: the agent returns the unchanged location -> no-op.
	 * Must NOT enqueue the live metadata (that would delete the empty table's
	 * own files).
	 */
	if (new_metadata != NULL && strcmp(new_metadata, old_metadata) != 0)
	{
		char	   *owner_username = GetUserNameFromId(GetUserId(), false);
		char	   *nspname = get_namespace_name(rel->rd_rel->relnamespace);
		const char *relname = NameStr(rel->rd_rel->relname);
		char	   *table_qname = nspname ?
			psprintf("%s.%s", nspname, relname) : psprintf("%s", relname);

		/*
		 * Enqueue the OLD metadata tree for async deletion.  Transactional heap
		 * insert: rolls back with the txn, so an aborted TRUNCATE keeps the old
		 * files.  The new (empty) metadata is not reachable from the old one,
		 * so the consumer leaves it intact.
		 */
		pg_iceberg_deletion_queue_insert(old_metadata,
										 relid,
										 table_info->volume_name,
										 table_info->volume_server_name,
										 owner_username,
										 table_qname,
										 GetCurrentTimestamp(),
										 DELETION_TYPE_METADATA);

		/* Swap the catalog pointer to the new empty metadata (CAS on old). */
		pg_iceberg_update_metadata_cas(relid, new_metadata, old_metadata);

		if (nspname)
			pfree(nspname);
		pfree(table_qname);
	}

	pg_iceberg_free_table_info(table_info);
	pg_iceberg_free_metadata_info(meta_info);
	pfree(old_metadata);
	relation_close(rel, AccessShareLock);
}

/*
 * Verify a column has no NULLs before SET NOT NULL (issue #401).  Runs a normal
 * distributed SELECT on the QD (planned + dispatched to the segments, so it sees
 * all data), rather than relying on the standard ATExecSetNotNull verify scan --
 * that scan is an empty no-op on the iceberg AM (see pg_iceberg_am.c).  Raises
 * NOT_NULL_VIOLATION if any NULL exists, aborting the ALTER before attnotnull is
 * set anywhere.
 */
static void
pg_iceberg_verify_column_not_null(Oid relid, const char *colname)
{
	StringInfoData	q;
	bool			has_null;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed while validating SET NOT NULL");

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT 1 FROM %s WHERE %s IS NULL LIMIT 1",
					 quote_qualified_identifier(
						 get_namespace_name(get_rel_namespace(relid)),
						 get_rel_name(relid)),
					 quote_identifier(colname));

	if (SPI_execute(q.data, true, 1) != SPI_OK_SELECT)
	{
		/* Mirror the success path's teardown so the SPI stack is not left
		 * connected if this ever runs inside an enclosing SPI context. */
		pfree(q.data);
		SPI_finish();
		elog(ERROR, "failed to validate SET NOT NULL on column \"%s\"", colname);
	}

	has_null = (SPI_processed > 0);
	pfree(q.data);
	SPI_finish();

	if (has_null)
		ereport(ERROR,
				(errcode(ERRCODE_NOT_NULL_VIOLATION),
				 errmsg("column \"%s\" of relation \"%s\" contains null values",
						colname, get_rel_name(relid))));
}

/*
 * One intercepted ALTER COLUMN TYPE: the column plus its validated new PG type.
 * We apply this to pg_attribute ourselves (pg_iceberg_apply_type_changes) after
 * removing the subcommand from the standard cmd list, so the standard PG table
 * rewrite -- which creates a transient relation and crashes on the iceberg AM --
 * never runs.
 */
typedef struct IcebergTypeChange
{
	char   *colname;
	Oid		newtypid;
	int32	newtypmod;
} IcebergTypeChange;

/*
 * Return true iff changing a column from (oldtypid,oldmod) to (newtypid,newmod)
 * is an allowed Iceberg widening promotion (issue #401): int2/int4->int8,
 * int2->int4, float4->float8, numeric(p,s)->numeric(p',s) with the same scale
 * and non-decreasing precision.  Mirrors Iceberg TypeUtil.isPromotionAllowed
 * (int2/int4 both map to Iceberg int; int8 to long).
 */
static bool
iceberg_is_widening(Oid oldtypid, int32 oldmod, Oid newtypid, int32 newmod)
{
	if ((oldtypid == INT2OID || oldtypid == INT4OID) && newtypid == INT8OID)
		return true;
	if (oldtypid == INT2OID && newtypid == INT4OID)
		return true;
	if (oldtypid == FLOAT4OID && newtypid == FLOAT8OID)
		return true;
	if (oldtypid == NUMERICOID && newtypid == NUMERICOID)
	{
		int		oldp, olds, newp, news;

		if (newmod == -1)
			return true;		/* target unconstrained numeric: always wider */
		if (oldmod == -1)
			return false;		/* unconstrained -> constrained can narrow */
		oldp = ((oldmod - VARHDRSZ) >> 16) & 0xffff;
		olds = (oldmod - VARHDRSZ) & 0xffff;
		newp = ((newmod - VARHDRSZ) >> 16) & 0xffff;
		news = (newmod - VARHDRSZ) & 0xffff;
		return (news == olds && newp >= oldp);
	}
	return false;
}

/*
 * Apply one intercepted ALTER COLUMN TYPE directly to pg_attribute (issue #401),
 * bypassing the standard PG table rewrite.  Updates every type-derived attribute
 * (atttypid/atttypmod/attlen/attbyval/attalign/attstorage/attcollation) from
 * pg_type so tuple deforming stays correct.  Widening only involves built-in
 * (pinned) types, so no pg_depend edit is needed.  Runs on whichever node
 * invokes it (QD directly, QEs via the dispatched SQL function) so every
 * segment's local catalog matches -- required because reads/writes compare the
 * QD plan's column type against each segment's pg_attribute.
 */
static void
pg_iceberg_alter_column_type_local_internal(Oid relid, const char *colname,
											Oid newtypid, int32 newtypmod)
{
	Relation			attrel;
	HeapTuple			atttup;
	HeapTuple			typtup;
	Form_pg_attribute	attform;
	Form_pg_type		typform;

	attrel = table_open(AttributeRelationId, RowExclusiveLock);

	atttup = SearchSysCacheCopyAttName(relid, colname);
	if (!HeapTupleIsValid(atttup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						colname, get_rel_name(relid))));

	typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(newtypid));
	if (!HeapTupleIsValid(typtup))
		elog(ERROR, "cache lookup failed for type %u", newtypid);
	typform = (Form_pg_type) GETSTRUCT(typtup);

	attform = (Form_pg_attribute) GETSTRUCT(atttup);
	attform->atttypid = newtypid;
	attform->atttypmod = newtypmod;
	attform->attlen = typform->typlen;
	attform->attbyval = typform->typbyval;
	attform->attalign = typform->typalign;
	attform->attstorage = typform->typstorage;
	attform->attcollation = typform->typcollation;

	CatalogTupleUpdate(attrel, &atttup->t_self, atttup);

	ReleaseSysCache(typtup);
	heap_freetuple(atttup);
	table_close(attrel, RowExclusiveLock);
	CommandCounterIncrement();
}

/*
 * SQL-callable per-segment entry (dispatched from the QD by
 * pg_iceberg_apply_type_changes).  Applies one ALTER COLUMN TYPE to the local
 * pg_attribute.  Registered in datalake_fdw--1.0.sql.
 *
 * This edits pg_attribute directly, which bypasses the relation ACL path that
 * ordinary DDL goes through, so it must do its own privilege check: without one
 * any session user could rewrite the declared type of any column of any
 * relation.  EXECUTE cannot be revoked from PUBLIC instead -- the QD dispatches
 * this to every segment in the invoking user's own session, so a non-superuser
 * table owner running ALTER TABLE has to be able to call it.  The ownership
 * check is the right gate: the legitimate dispatch path always runs as the user
 * who issued the ALTER TABLE, who therefore already owns the relation.
 */
PG_FUNCTION_INFO_V1(pg_iceberg_alter_column_type_local);
Datum
pg_iceberg_alter_column_type_local(PG_FUNCTION_ARGS)
{
	Oid		relid;
	char   *colname;
	Oid		newtypid;
	int32	newtypmod;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("relation, column, type and typmod must not be NULL")));

	relid = PG_GETARG_OID(0);
	colname = text_to_cstring(PG_GETARG_TEXT_PP(1));
	newtypid = PG_GETARG_OID(2);
	newtypmod = PG_GETARG_INT32(3);

	if (!pg_class_ownercheck(relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, get_rel_name(relid));

	pg_iceberg_alter_column_type_local_internal(relid, colname, newtypid, newtypmod);

	PG_RETURN_VOID();
}

void
pg_iceberg_apply_type_changes(Oid relid, List *typeChanges)
{
	ListCell   *lc;

	if (typeChanges == NIL)
		return;
	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	foreach(lc, typeChanges)
	{
		IcebergTypeChange  *tc = (IcebergTypeChange *) lfirst(lc);
		StringInfoData		sql;

		/*
		 * Propagate the pg_attribute change to every segment (mirrors
		 * pg_iceberg_upsert_location_option): the standard ALTER dispatch is
		 * bypassed for the intercepted type change, so dispatch it explicitly.
		 */
		initStringInfo(&sql);
		appendStringInfo(&sql,
						 "SELECT pg_catalog.pg_iceberg_alter_column_type_local(%u, %s, %u, %d)",
						 relid,
						 quote_literal_cstr(tc->colname),
						 tc->newtypid,
						 tc->newtypmod);
		CdbDispatchCommand(sql.data, DF_CANCEL_ON_ERROR, NULL);
		pfree(sql.data);

		/* Apply on the QD as well. */
		pg_iceberg_alter_column_type_local_internal(relid, tc->colname,
													tc->newtypid, tc->newtypmod);
	}
}

/*
 * Validate an ALTER TABLE against the builtin-catalog schema-evolution rules
 * (issue #401) and build the list of IcebergSchemaOp* to send to the agent.
 *
 * Runs before standard ProcessUtility so unsupported subcommands (notably
 * ALTER COLUMN TYPE, whose PG rewrite would hit the empty iceberg AM stub and
 * SIGSEGV) are rejected before any catalog change or dispatch happens.
 *
 * M1 supports the metadata-only subcommands: ADD COLUMN (must be nullable),
 * DROP COLUMN, DROP NOT NULL, SET NOT NULL.  ALTER COLUMN TYPE / RENAME are
 * handled separately (rewrite path).  Only builtin catalog is allowed.
 */
List *
pg_iceberg_build_alter_ops(Oid relid, List *cmds,
						   List **standardCmds, List **typeChanges)
{
	Relation			rel;
	IcebergTableInfo   *table_info;
	List			   *ops = NIL;
	ListCell		   *lc;

	*standardCmds = NIL;
	*typeChanges = NIL;

	rel = relation_open(relid, AccessShareLock);
	table_info = pg_iceberg_get_table_info(relid);

	if (!pg_iceberg_is_builtin_catalog(table_info->catalog_server_name))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ALTER TABLE is not supported on Iceberg tables on external catalogs"),
				 errhint("Only builtin-catalog Iceberg tables support schema evolution; "
						 "use Spark/Trino/Flink for external/REST/Hive catalogs.")));

	foreach(lc, cmds)
	{
		AlterTableCmd	*cmd = (AlterTableCmd *) lfirst(lc);
		IcebergSchemaOp *op = NULL;
		bool			intercepted = false;	/* handled by us, not standard ALTER */

		switch (cmd->subtype)
		{
			case AT_AddColumn:
			{
				ColumnDef  *def = (ColumnDef *) cmd->def;
				Oid			typid;
				int32		typmod;

				/*
				 * ADD COLUMN IF NOT EXISTS on a column that already exists:
				 * standard ProcessUtility silently skips it (and emits its
				 * NOTICE), so pass the cmd through but build no Iceberg op --
				 * otherwise Iceberg's addColumn rejects the duplicate name.
				 */
				if (cmd->missing_ok &&
					get_attnum(relid, def->colname) != InvalidAttrNumber)
					break;

				/*
				 * Iceberg adds new columns as optional with a null default.
				 * At raw-parse time NOT NULL / PRIMARY KEY / UNIQUE arrive as
				 * entries in def->constraints (def->is_not_null is only set
				 * after transformation), so reject any constraint or default
				 * here -- only a bare nullable column is allowed.
				 */
				if (def->is_not_null || def->constraints != NIL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("ADD COLUMN on an Iceberg table must be a bare nullable column"),
							 errhint("Omit NOT NULL / DEFAULT / PRIMARY KEY / UNIQUE; Iceberg adds new columns as optional.")));
				if (def->raw_default != NULL || def->cooked_default != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("ADD COLUMN with a DEFAULT is not supported on Iceberg tables")));

				typenameTypeIdAndMod(NULL, def->typeName, &typid, &typmod);
				op = (IcebergSchemaOp *) palloc0(sizeof(IcebergSchemaOp));
				op->op = "addColumn";
				op->name = pstrdup(def->colname);
				op->type = pstrdup(mapPostgresToIcebergType(typid, typmod));
				break;
			}
			case AT_DropColumn:
				/*
				 * DROP COLUMN IF EXISTS on a column that does not exist:
				 * standard ProcessUtility skips it (with its NOTICE), so pass
				 * the cmd through but build no Iceberg op.
				 */
				if (cmd->missing_ok &&
					get_attnum(relid, cmd->name) == InvalidAttrNumber)
					break;
				op = (IcebergSchemaOp *) palloc0(sizeof(IcebergSchemaOp));
				op->op = "dropColumn";
				op->name = pstrdup(cmd->name);
				break;
			case AT_DropNotNull:
				op = (IcebergSchemaOp *) palloc0(sizeof(IcebergSchemaOp));
				op->op = "makeOptional";
				op->name = pstrdup(cmd->name);
				break;
			case AT_SetNotNull:
				/*
				 * SET NOT NULL: verify no NULLs up front with a distributed
				 * query (QD only), then let standard ATExecSetNotNull run.  Its
				 * own verify scan is an empty no-op on the iceberg AM, and it
				 * sets attnotnull and dispatches it to every segment.  We mirror
				 * the change onto the Iceberg schema via requireColumn.
				 */
				if (Gp_role == GP_ROLE_DISPATCH)
					pg_iceberg_verify_column_not_null(relid, cmd->name);
				op = (IcebergSchemaOp *) palloc0(sizeof(IcebergSchemaOp));
				op->op = "requireColumn";
				op->name = pstrdup(cmd->name);
				break;
			case AT_AlterColumnType:
			{
				/*
				 * ALTER COLUMN TYPE (widening only).  Intercept it: the standard
				 * PG rewrite creates a transient relation and drives the iceberg
				 * AM into paths it cannot serve, so keep it OUT of the standard
				 * cmd list.  We validate the promotion, mirror it to Iceberg via
				 * updateColumn, apply the pg_attribute change ourselves
				 * (pg_iceberg_apply_type_changes), and rely on read-time type
				 * promotion in the parquet reader to read pre-change data files.
				 */
				ColumnDef		   *def = (ColumnDef *) cmd->def;
				AttrNumber			attnum = get_attnum(relid, cmd->name);
				Form_pg_attribute	oldatt;
				Oid					newtypid;
				int32				newtypmod;
				IcebergTypeChange  *tc;

				if (attnum == InvalidAttrNumber)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" of relation \"%s\" does not exist",
									cmd->name, get_rel_name(relid))));
				if (def->raw_default != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("ALTER COLUMN TYPE ... USING is not supported on Iceberg tables")));

				oldatt = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
				typenameTypeIdAndMod(NULL, def->typeName, &newtypid, &newtypmod);

				if (!iceberg_is_widening(oldatt->atttypid, oldatt->atttypmod,
										 newtypid, newtypmod))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("ALTER COLUMN TYPE on an Iceberg table only supports widening"),
							 errhint("Allowed: integer->bigint, real->double precision, "
									 "numeric(p,s)->numeric(p',s) with the same scale and p' >= p.")));

				op = (IcebergSchemaOp *) palloc0(sizeof(IcebergSchemaOp));
				op->op = "updateColumn";
				op->name = pstrdup(cmd->name);
				op->type = pstrdup(mapPostgresToIcebergType(newtypid, newtypmod));

				tc = (IcebergTypeChange *) palloc0(sizeof(IcebergTypeChange));
				tc->colname = pstrdup(cmd->name);
				tc->newtypid = newtypid;
				tc->newtypmod = newtypmod;
				*typeChanges = lappend(*typeChanges, tc);
				intercepted = true;
				break;
			}
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("this ALTER TABLE subcommand is not supported on Iceberg tables")));
				break;
		}

		if (op != NULL)
			ops = lappend(ops, op);
		/* Everything except intercepted subcommands is handled by standard ALTER. */
		if (!intercepted)
			*standardCmds = lappend(*standardCmds, cmd);
	}

	pg_iceberg_free_table_info(table_info);
	relation_close(rel, AccessShareLock);

	return ops;
}

/*
 * Apply a previously-built schema-evolution op list to the Iceberg side
 * (issue #401).  QD-only: fires one ICEBERG_UPDATE_SCHEMA commit and swaps the
 * catalog metadata pointer (CAS on the pre-ALTER location) to the new metadata.
 * The old metadata.json is retained as Iceberg history (no deletion enqueue).
 */
void
pg_iceberg_alter_apply(Oid relid, List *ops)
{
	Relation				rel;
	IcebergMetadataInfo	   *meta_info;
	IcebergTableInfo	   *table_info;
	char				   *old_metadata;
	char				   *new_metadata;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;
	if (ops == NIL)
		return;

	rel = relation_open(relid, AccessShareLock);

	if (!((rel->rd_rel->relkind == RELKIND_RELATION ||
		   rel->rd_rel->relkind == RELKIND_MATVIEW) &&
		  is_iceberg_rel(rel)))
	{
		relation_close(rel, AccessShareLock);
		return;
	}

	meta_info = pg_iceberg_get_metadata_info_missing_ok(relid);
	if (meta_info == NULL)
	{
		relation_close(rel, AccessShareLock);
		return;
	}

	table_info = pg_iceberg_get_table_info(relid);
	old_metadata = pstrdup(meta_info->metadata_location);

	new_metadata = pg_iceberg_update_schema_with_catalog(rel, table_info,
														 old_metadata, ops);

	if (new_metadata != NULL && strcmp(new_metadata, old_metadata) != 0)
		pg_iceberg_update_metadata_cas(relid, new_metadata, old_metadata);

	pg_iceberg_free_table_info(table_info);
	pg_iceberg_free_metadata_info(meta_info);
	pfree(old_metadata);
	relation_close(rel, AccessShareLock);
}

/*
 * RENAME COLUMN on a builtin iceberg table (issue #401).  RENAME is a pure
 * catalog change (pg_attribute.attname), so the standard rename has already run
 * and dispatched the new name to every segment when this is called.  Here we
 * mirror it onto the Iceberg schema via renameColumn.  Pre-rename data files
 * still carry the old physical column name; they are read by matching on the
 * Iceberg field-id (== PG attnum, stable across rename) in the parquet reader.
 * QD-only; reuses pg_iceberg_alter_apply.  A non-builtin catalog errors (and
 * rolls back) via the builtin guard in createUpdateSchemaRequestJson.
 */
void
pg_iceberg_rename_column(Oid relid, const char *oldname, const char *newname)
{
	IcebergSchemaOp *op;

	op = (IcebergSchemaOp *) palloc0(sizeof(IcebergSchemaOp));
	op->op = "renameColumn";
	op->name = pstrdup(oldname);
	op->newName = pstrdup(newname);

	pg_iceberg_alter_apply(relid, list_make1(op));
}