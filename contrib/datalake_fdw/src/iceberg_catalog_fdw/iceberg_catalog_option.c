#include "iceberg_catalog_option.h"
#include "src/common/parser_option.h"
#include "postgres.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "cdb/cdbutil.h"
#include "utils/syscache.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "utils/lsyscache.h"
#include "catalog/pg_foreign_catalog.h"
#include "utils/array.h"
#include "access/reloptions.h"
#include "src/common/iceberg_constants.h"

/* iceberg options */
/* iceberg catalog server options */

#define DATALAKE_ICEBERG_CATALOG_URL "url"
#define DATALAKE_ICEBERG_CATALOG_POLARIS_SERVER_REALM "polaris_server_realm"

/* iceberg catalog user mapping options */
#define DATALAKE_ICEBERG_CATALOG_USERNAME "username"
#define DATALAKE_ICEBERG_CATALOG_AUTH_METHOD "auth_method"
#define DATALAKE_ICEBERG_CATALOG_KRB_SERVICE_PRINCIPAL "krb_service_principal"
#define DATALAKE_ICEBERG_CATALOG_KRB_CLIENT_PRINCIPAL "krb_client_principal"
#define DATALAKE_ICEBERG_CATALOG_KRB_CLIENT_KEYTAB "krb_client_keytab"
#define DATALAKE_ICEBERG_CATALOG_CLIENT_ID "client_id"
#define DATALAKE_ICEBERG_CATALOG_CLIENT_SECRET "client_secret"
#define DATALAKE_ICEBERG_CATALOG_SCOPE "scope"

// catalog polaris user mapping
#define DATALAKE_ICEBERG_CATALOG_CLIENT_ID "client_id"
#define DATALAKE_ICEBERG_CATALOG_CLIENT_SECRET "client_secret"
#define DATALAKE_ICEBERG_CATALOG_SCOPE "scope"

/* iceberg foreign catalog options */
#define DATALAKE_ICEBERG_CATALOG_NAME "catalog_name"
#define DATALAKE_ICEBERG_CATALOG_DEFAULT_NAMESPACE "default_namespace"
#define DATALAKE_ICEBERG_CATALOG_ENABLE_METADATA_CACHE "enable_metadata_cache"
#define DATALAKE_ICEBERG_CATALOG_METADATA_CACHE_TTL "metadata_cache_ttl"
#define DATALAKE_ICEBERG_CATALOG_AUTO_REFRESH_METADATA "auto_refresh_metadata"
#define DATALAKE_ICEBERG_CATALOG_WAREHOUSE_LOCATION_PREFIX "warehouse_location_prefix"
#define DATALAKE_ICEBERG_CATALOG_SPLIT_SIZE "split_size"
#define DATALAKE_ICEBERG_CATALOG_FILTER_STRING "filter_string"

bool checkIsBuiltinCatalog(const char* catalogServerName, const char* catalogName);

static void
parseHiveCatalogServerOptions(IcebergCatalogServerOptions *options, List *server_options)
{
    options->hive_metastore_uri = getStringOption(server_options, DATALAKE_ICEBERG_CATALOG_URL);
    /* Fallback: also accept "hive_metastore_uri" (legacy option key) */
    if (options->hive_metastore_uri == NULL)
        options->hive_metastore_uri = getStringOption(server_options, "hive_metastore_uri");
}

static void
parsePolarisCatalogServerOptions(IcebergCatalogServerOptions *options, List *server_options)
{
    options->polaris_server_url = getStringOption(server_options, DATALAKE_ICEBERG_CATALOG_URL);

    /*
     * Realm sent as the Polaris-Realm header on every Polaris request
     * (issue #841: it was hardcoded to "POLARIS" in the agent with no way
     * to override it; servers configured with a different realm rejected
     * the OAuth token request with 404 MissingOrInvalidRealm).  Optional;
     * the agent keeps defaulting to "POLARIS" when unset.
     */
    options->polaris_server_realm = getStringOption(server_options,
                                                    DATALAKE_ICEBERG_CATALOG_POLARIS_SERVER_REALM);
}

static void
parseIcebergCatalogServerOptions(IcebergCatalogServerOptions *options, List *server_options)
{
    options->server_type = getStringOption(server_options, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE);
    /* Fallback: also accept "server_type" (legacy option key) */
    if (options->server_type == NULL)
        options->server_type = getStringOption(server_options, "server_type");
    options->server_name = getStringOption(server_options, "server_name");

    if (options->server_type == NULL)
        return;

    if (pg_strcasecmp(options->server_type, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_HIVE) == 0)
    {
        parseHiveCatalogServerOptions(options, server_options);
    }
    else if (pg_strcasecmp(options->server_type, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_POLARIS) == 0)
    {
        parsePolarisCatalogServerOptions(options, server_options);
    }
}

static void
parseHiveUserMappingOptions(HiveUserMappingOptions *options, List *user_options)
{
    options->username = getStringOption(user_options, DATALAKE_ICEBERG_CATALOG_USERNAME);
    options->auth_method = getStringOption(user_options, DATALAKE_ICEBERG_CATALOG_AUTH_METHOD);
    options->krb_service_principal = getStringOption(user_options, DATALAKE_ICEBERG_CATALOG_KRB_SERVICE_PRINCIPAL);
    options->krb_client_principal = getStringOption(user_options, DATALAKE_ICEBERG_CATALOG_KRB_CLIENT_PRINCIPAL);
    options->krb_client_keytab = getStringOption(user_options, DATALAKE_ICEBERG_CATALOG_KRB_CLIENT_KEYTAB);
}

static void
parsePolarisUserMappingOptions(PolarisUserMappingOptions *options, List *user_options)
{
    options->client_id = getStringOption(user_options, DATALAKE_ICEBERG_CATALOG_CLIENT_ID);
    options->client_secret = getStringOption(user_options, DATALAKE_ICEBERG_CATALOG_CLIENT_SECRET);
    options->scope = getStringOption(user_options, DATALAKE_ICEBERG_CATALOG_SCOPE);
}

static void
parseIcebergCatalogUserMappingOptions(IcebergCatalogUserMappingOptions *options, List *user_options, const char *server_type)
{
    if (server_type && pg_strcasecmp(server_type, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_HIVE) == 0)
    {
        parseHiveUserMappingOptions(&options->hive, user_options);
    }
    else if (server_type && pg_strcasecmp(server_type, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_POLARIS) == 0)
    {
        parsePolarisUserMappingOptions(&options->polaris, user_options);
    }
}

static void
parseIcebergForeignCatalogOptions(IcebergForeignCatalogOptions *options, List *foreign_options)
{
    options->catalog_name = getStringOption(foreign_options, DATALAKE_ICEBERG_CATALOG_NAME);
    options->default_namespace = getStringOption(foreign_options, DATALAKE_ICEBERG_CATALOG_DEFAULT_NAMESPACE);
    options->enable_metadata_cache = getBoolOption(foreign_options, DATALAKE_ICEBERG_CATALOG_ENABLE_METADATA_CACHE, false);
    options->metadata_cache_ttl = getIntOption(foreign_options, DATALAKE_ICEBERG_CATALOG_METADATA_CACHE_TTL, 0);
    options->auto_refresh_metadata = getBoolOption(foreign_options, DATALAKE_ICEBERG_CATALOG_AUTO_REFRESH_METADATA, false);
    options->warehouse_location_prefix = getStringOption(foreign_options, DATALAKE_ICEBERG_CATALOG_WAREHOUSE_LOCATION_PREFIX);
    options->total_segment = getgpsegmentCount();
    options->split_size = getIntOption(foreign_options, DATALAKE_ICEBERG_CATALOG_SPLIT_SIZE, 128);
    options->filter_string = getStringOption(foreign_options, DATALAKE_ICEBERG_CATALOG_FILTER_STRING);
}

static void
parseIcebergBuiltinCatalogServerOptions(IcebergCatalogServerOptions *options, List *server_options)
{
    options->server_type = getStringOption(server_options, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE);
    /* Fallback: also accept "server_type" (legacy option key) */
    if (options->server_type == NULL)
        options->server_type = getStringOption(server_options, "server_type");
    if (options->server_type == NULL)
    {
        options->server_type = pstrdup(DATALAKEFDW_ICEBERG_SERVER_BUILTIN);
    }
}

IcebergCatalogOptions*
getIcebergCatalogOptions(const char* catalogServerName, const char* catalogName)
{
    IcebergCatalogOptions *options = (IcebergCatalogOptions *) palloc0(sizeof(IcebergCatalogOptions));

    if (checkIsBuiltinCatalog(catalogServerName, catalogName))
    {
        /* Get foreign server */
        ForeignServer *server = GetForeignServerByName(catalogServerName, false);
        parseIcebergBuiltinCatalogServerOptions(&options->catalog_server, server->options);
    }
    else
    {
        /* Get foreign server */
        ForeignServer *server = GetForeignServerByName(catalogServerName, false);

        /* Get user mapping */
        UserMapping *user = GetUserMapping(GetUserId(), server->serverid);

        /* Get foreign catalog */
        Oid catalogOid = get_foreign_catalog_oid(catalogName, catalogServerName, false);
        HeapTuple tuple = SearchSysCache1(FOREIGNCATALOGOID, ObjectIdGetDatum(catalogOid));

        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "foreign catalog \"%s\" does not exist", catalogName);

        /* Get catalog options */
        Datum optionsDatum;
        bool isnull;
        List *catalogOptions = NIL;

        optionsDatum = SysCacheGetAttr(FOREIGNCATALOGOID, tuple,
                                    Anum_pg_foreign_catalog_fcoptions, &isnull);
        if (!isnull)
        {
            catalogOptions = untransformRelOptions(optionsDatum);
        }

        ReleaseSysCache(tuple);

        /* Parse options */
        parseIcebergCatalogServerOptions(&options->catalog_server, server->options);
        parseIcebergCatalogUserMappingOptions(&options->catalog_user, user->options, options->catalog_server.server_type);
        parseIcebergForeignCatalogOptions(&options->foreign_catalog, catalogOptions);

    }
    return options;
}

IcebergCatalogOptions*
getIcebergPolarisCatalogOptions(const char* catalogServerName, const char* catalogName, List* catalogOptions)
{
    IcebergCatalogOptions *options = (IcebergCatalogOptions *) palloc0(sizeof(IcebergCatalogOptions));

    /* Get foreign server */
    ForeignServer *server = GetForeignServerByName(catalogServerName, false);

    /* Get user mapping */
    UserMapping *user = GetUserMapping(GetUserId(), server->serverid);

    /* Parse options */
    parseIcebergCatalogServerOptions(&options->catalog_server, server->options);
    parseIcebergCatalogUserMappingOptions(&options->catalog_user, user->options, options->catalog_server.server_type);
    parseIcebergForeignCatalogOptions(&options->foreign_catalog, catalogOptions);
    return options;
}

bool checkIsBuiltinCatalog(const char* catalogServerName, const char* catalogName)
{
    bool isBuiltin = true;
    if ((catalogServerName == NULL || catalogName == NULL))
    {
        return isBuiltin;
    }
    /* Get foreign server */
    ForeignServer *server = GetForeignServerByName(catalogServerName, false);
    char *server_type = getStringOption(server->options, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE);
    /* Fallback: also check "server_type" (legacy option key) */
    if (server_type == NULL)
        server_type = getStringOption(server->options, "server_type");
    if (server_type == NULL)
    {
        return isBuiltin;
    }
    if (pg_strcasecmp(server_type, DATALAKEFDW_ICEBERG_SERVER_BUILTIN) == 0)
    {
        return isBuiltin;
    }
    return false;
}

bool checkIsPolarisCatalog(const char* catalogServerName, const char* catalogName)
{
    bool isPolaris = true;
    /* Get foreign server */
    ForeignServer *server = GetForeignServerByName(catalogServerName, false);
    char *server_type = getStringOption(server->options, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE);
    /* Fallback: also check "server_type" (legacy option key) */
    if (server_type == NULL)
        server_type = getStringOption(server->options, "server_type");
    if (server_type == NULL)
    {
        elog(DEBUG1, "iceberg_catalog_create_catalog: skipping catalog create for non-polaris type: %s",
			 server_type ? server_type : "(null)");
        return false;
    }
    if (pg_strcasecmp(server_type, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_POLARIS) == 0)
    {
        return isPolaris;
    }
    return false;
}