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
/* Same name and unit (minutes) as the per-table option it is the default for.
 * 0 (or unset) = no caching.  Supersedes enable_metadata_cache /
 * metadata_cache_ttl / auto_refresh_metadata, which never worked in any
 * released version; those names are now unrecognized and thus ignored, like
 * any other unknown option. */
#define DATALAKE_ICEBERG_CATALOG_METADATA_CACHE "metadata_cache"
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

/*
 * deriveEndpointFromUrl
 *		Return the origin (scheme + host[:port]) of an http(s) URL, or NULL
 *		when the URL is not usable as a derivation source.
 *
 * Path, query and fragment are discarded on purpose: the catalog URL often
 * carries a route prefix ("/iceberg") that must never leak into the object
 * storage address, where it would corrupt request signing.  userinfo is
 * rejected outright.  Bracketed IPv6 authorities keep their brackets.
 *
 * The derivation is only correct for deployments where catalog and object
 * storage share one address (XSKY serves both on a single port).  Setups
 * like AWS S3 Tables use different hosts for the two services, so users
 * should normally configure "endpoint" explicitly; this is a convenience
 * default whose failure mode is loud (connection refused), not silent.
 */
char *
deriveEndpointFromUrl(const char *url)
{
	const char *authority;
	const char *authority_end;
	const char *host_end;
	const char *p;

	if (url == NULL || url[0] == '\0')
		return NULL;

	if (pg_strncasecmp(url, "http://", 7) == 0)
		authority = url + 7;
	else if (pg_strncasecmp(url, "https://", 8) == 0)
		authority = url + 8;
	else
		return NULL;

	authority_end = authority + strcspn(authority, "/?#");
	if (authority_end == authority)
		return NULL;

	/* Reject userinfo: an '@' inside the authority. */
	for (p = authority; p < authority_end; p++)
	{
		if (*p == '@')
			return NULL;
	}

	if (*authority == '[')
	{
		/* Bracketed IPv6: require a closing bracket inside the authority. */
		host_end = memchr(authority, ']', authority_end - authority);
		if (host_end == NULL || host_end == authority + 1)
			return NULL;
		host_end++;				/* past ']' */
	}
	else
	{
		host_end = authority + strcspn(authority, ":");
		if (host_end > authority_end)
			host_end = authority_end;
	}

	/* Optional port: digits only, at least one. */
	if (host_end < authority_end)
	{
		if (*host_end != ':' || host_end + 1 == authority_end)
			return NULL;
		for (p = host_end + 1; p < authority_end; p++)
		{
			if (*p < '0' || *p > '9')
				return NULL;
		}
	}

	return pnstrdup(url, authority_end - url);
}

/*
 * An empty option value is almost never intentional -- it is what a shell
 * script produces when a variable fails to expand.  Treating it as "unset"
 * would defer the mistake to an opaque runtime failure, so reject it at
 * parse time instead.
 */
static char *
getRestOption(List *options, const char *name)
{
	char	   *value = getStringOption(options, name);

	if (value != NULL && value[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("option \"%s\" must not be empty", name)));
	return value;
}

static int parseVendedCredentials(List *option_list);

static void
parseRestCatalogServerOptions(IcebergCatalogServerOptions *options, List *server_options)
{
	char	   *path_style;

	options->rest.url = getRestOption(server_options, DATALAKE_ICEBERG_CATALOG_URL);
	if (options->rest.url == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("option \"url\" is required for a REST catalog server")));

	options->rest.region = getRestOption(server_options,
										 DATALAKE_ICEBERG_CATALOG_REST_REGION);
	options->rest.signing_name = getRestOption(server_options,
											   DATALAKE_ICEBERG_CATALOG_REST_SIGNING_NAME);

	options->rest.endpoint = getRestOption(server_options,
										   DATALAKE_ICEBERG_CATALOG_REST_ENDPOINT);
	if (options->rest.endpoint == NULL)
	{
		options->rest.endpoint = deriveEndpointFromUrl(options->rest.url);
		if (options->rest.endpoint == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value for option \"url\": \"%s\"",
							options->rest.url),
					 errhint("Use an http:// or https:// URL without userinfo, "
							 "or set \"endpoint\" explicitly.")));
	}

	options->rest.vended_credentials = parseVendedCredentials(server_options);

	options->rest.polaris_server_realm =
		getRestOption(server_options, DATALAKE_ICEBERG_CATALOG_POLARIS_SERVER_REALM);

	path_style = getRestOption(server_options,
							   DATALAKE_ICEBERG_CATALOG_REST_PATH_STYLE_ACCESS);
	if (path_style == NULL)
		options->rest.path_style_access = true;
	else if (pg_strcasecmp(path_style, "true") == 0)
		options->rest.path_style_access = true;
	else if (pg_strcasecmp(path_style, "false") == 0)
		options->rest.path_style_access = false;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for option \"path_style_access\": \"%s\"",
						path_style),
				 errhint("Valid values are \"true\" and \"false\".")));

	/*
	 * Absent is the normal case and means "this deployment offers no
	 * maintenance API we can drive", which is what every REST catalog looked
	 * like before this option existed.  Spelling is checked here so a typo
	 * surfaces at CREATE SERVER rather than as a silently unmaintained table.
	 */
	options->rest.maintenance_api =
		getRestOption(server_options,
					  DATALAKE_ICEBERG_CATALOG_REST_MAINTENANCE_API);
	if (options->rest.maintenance_api != NULL &&
		pg_strcasecmp(options->rest.maintenance_api,
					  DATALAKE_ICEBERG_MAINTENANCE_API_S3TABLES) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for option \"maintenance_api\": \"%s\"",
						options->rest.maintenance_api),
				 errhint("The only supported value is \"s3tables\".")));
}

/*
 * parseVendedCredentials
 *		Read the vended_credentials option out of `option_list` as a
 *		tri-state, so an absent option stays distinguishable from "off".
 *
 * Accepted on both the SERVER and the USER MAPPING; the two are reconciled
 * by resolveRestVendedCredentials().  Spelling is rejected here, on whichever
 * object carries it, so a typo cannot read as "on" -- for a switch whose
 * whole job is to narrow what credentials are handed out, silently defaulting
 * a misspelling to the permissive value is the one failure mode that must not
 * happen.
 */
static int
parseVendedCredentials(List *option_list)
{
	char	   *vended = getRestOption(option_list,
									   DATALAKE_ICEBERG_CATALOG_REST_VENDED_CREDENTIALS);

	if (vended == NULL)
		return ICEBERG_VENDED_UNSET;
	if (pg_strcasecmp(vended, "true") == 0 || pg_strcasecmp(vended, "on") == 0)
		return ICEBERG_VENDED_ON;
	if (pg_strcasecmp(vended, "false") == 0 || pg_strcasecmp(vended, "off") == 0)
		return ICEBERG_VENDED_OFF;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid value for option \"vended_credentials\": \"%s\"",
					vended),
			 errhint("Valid values are \"on\" and \"off\".")));
	return ICEBERG_VENDED_UNSET;	/* keep the compiler quiet */
}

/*
 * resolveRestVendedCredentials
 *		Reconcile the SERVER and USER MAPPING settings into the single value
 *		the rest of the code reads, and enforce what it implies.
 *
 * The USER MAPPING wins because the credentials it would fall back to are
 * its own; the SERVER supplies the deployment-wide default.  Historically
 * only the USER MAPPING was ever read, so the same option on a SERVER was
 * accepted, stored, and silently ignored -- leaving an operator who had
 * switched vending off still using vended credentials, with no error and no
 * hint, which for a switch that exists to narrow credential scope is the
 * worst possible direction to fail in.
 */
static void
resolveRestVendedCredentials(RestUserMappingOptions *user,
							 RestCatalogOptions *server)
{
	if (user->vended_credentials == ICEBERG_VENDED_UNSET)
		user->vended_credentials = server->vended_credentials;
	if (user->vended_credentials == ICEBERG_VENDED_UNSET)
		user->vended_credentials = ICEBERG_VENDED_ON;

	/*
	 * Only reachable once both objects have been read, which is why this
	 * check cannot live in the user mapping parser where it started: a
	 * server-level "off" has to be able to trip it too.
	 */
	if (user->vended_credentials == ICEBERG_VENDED_OFF &&
		user->access_key_id == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("vended_credentials is \"off\" but no static credentials are configured"),
				 errhint("Set access_key_id and secret_access_key on the user mapping, "
						 "or remove vended_credentials.")));
}

/*
 * validateRestCrossObjectOptions
 *		Checks that need SERVER and USER MAPPING together, so they can only
 *		run once both have been parsed (CREATE TABLE / first query time):
 *		the two DDL validators cannot see each other's object.
 */
static void
validateRestCrossObjectOptions(IcebergCatalogOptions *options)
{
	RestUserMappingOptions *user;
	RestCatalogOptions *server;

	if (options->catalog_server.server_type == NULL ||
		pg_strcasecmp(options->catalog_server.server_type,
					  DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_REST) != 0)
		return;

	user = &options->catalog_user.rest;
	server = &options->catalog_server.rest;

	resolveRestVendedCredentials(user, server);

	if (pg_strcasecmp(user->auth_method, "sigv4") == 0)
	{
		if (server->region == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
					 errmsg("auth_method \"sigv4\" requires option \"region\" on the catalog server")));
		if (server->signing_name == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
					 errmsg("auth_method \"sigv4\" requires option \"signing_name\" on the catalog server")));
	}
	else if (server->signing_name != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("option \"signing_name\" is set on the catalog server but auth_method is \"%s\"",
						user->auth_method),
				 errhint("signing_name is only used with auth_method \"sigv4\".")));
}

/*
 * validateRestServerOptionList
 *		Entry point for the CREATE SERVER validator: run the rest server
 *		option rules against a bare option list, discarding the result.
 */
void
validateRestServerOptionList(List *server_options)
{
	IcebergCatalogServerOptions scratch;

	memset(&scratch, 0, sizeof(scratch));
	parseRestCatalogServerOptions(&scratch, server_options);
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
	else if (pg_strcasecmp(options->server_type, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_REST) == 0)
	{
		parseRestCatalogServerOptions(options, server_options);
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

/*
 * parseRestUserMappingOptions
 *		Parse and validate the REST user mapping's two credential groups.
 *
 * Pairing is enforced per group: half a credential pair is a configuration
 * accident, and letting it through would surface later as an opaque 403.
 * ak/sk together with auth_method=none is legitimate (data-plane fallback
 * with an anonymous catalog); oauth2 credentials without auth_method=oauth2
 * are not -- the user configured something the settings say never runs.
 */
static void
parseRestUserMappingOptions(RestUserMappingOptions *options, List *user_options)
{
	options->auth_method = getRestOption(user_options,
										 DATALAKE_ICEBERG_CATALOG_REST_AUTH_METHOD);
	options->access_key_id = getRestOption(user_options,
										   DATALAKE_ICEBERG_CATALOG_REST_ACCESS_KEY_ID);
	options->secret_access_key = getRestOption(user_options,
											   DATALAKE_ICEBERG_CATALOG_REST_SECRET_ACCESS_KEY);
	options->client_id = getRestOption(user_options, DATALAKE_ICEBERG_CATALOG_CLIENT_ID);
	options->client_secret = getRestOption(user_options, DATALAKE_ICEBERG_CATALOG_CLIENT_SECRET);
	options->scope = getRestOption(user_options, DATALAKE_ICEBERG_CATALOG_SCOPE);

	options->vended_credentials = parseVendedCredentials(user_options);

	if (options->auth_method == NULL)
		options->auth_method = "none";
	else if (pg_strcasecmp(options->auth_method, "sigv4") != 0 &&
			 pg_strcasecmp(options->auth_method, "oauth2") != 0 &&
			 pg_strcasecmp(options->auth_method, "none") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for option \"auth_method\": \"%s\"",
						options->auth_method),
				 errhint("Valid values are \"sigv4\", \"oauth2\" and \"none\".")));

	if ((options->access_key_id == NULL) != (options->secret_access_key == NULL))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("options \"access_key_id\" and \"secret_access_key\" must be set together")));

	if ((options->client_id == NULL) != (options->client_secret == NULL))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("options \"client_id\" and \"client_secret\" must be set together")));

	if (pg_strcasecmp(options->auth_method, "sigv4") == 0 &&
		options->access_key_id == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("auth_method \"sigv4\" requires \"access_key_id\" and \"secret_access_key\"")));

	if (pg_strcasecmp(options->auth_method, "oauth2") == 0 &&
		options->client_id == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("auth_method \"oauth2\" requires \"client_id\" and \"client_secret\"")));

	if (pg_strcasecmp(options->auth_method, "oauth2") != 0 &&
		options->client_id != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("oauth2 credentials configured but auth_method is \"%s\"",
						options->auth_method),
				 errhint("Set auth_method to \"oauth2\" or remove client_id/client_secret.")));

	/*
	 * With vending off there is no other source of data-plane keys, so
	 * requiring them here turns a later opaque 403 into an actionable error
	 * at configuration time.
	 */
	/*
	 * The "off without static keys" check lives in
	 * resolveRestVendedCredentials(): the option may come from the SERVER
	 * instead, which is not visible from here.
	 */
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
	else if (server_type && pg_strcasecmp(server_type, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_REST) == 0)
	{
		parseRestUserMappingOptions(&options->rest, user_options);
	}
}

static void
parseIcebergForeignCatalogOptions(IcebergForeignCatalogOptions *options, List *foreign_options)
{
    options->catalog_name = getStringOption(foreign_options, DATALAKE_ICEBERG_CATALOG_NAME);
    options->default_namespace = getStringOption(foreign_options, DATALAKE_ICEBERG_CATALOG_DEFAULT_NAMESPACE);
    options->metadata_cache = getIntOption(foreign_options, DATALAKE_ICEBERG_CATALOG_METADATA_CACHE, 0);
    options->warehouse_location_prefix = getStringOption(foreign_options, DATALAKE_ICEBERG_CATALOG_WAREHOUSE_LOCATION_PREFIX);
    options->total_segment = getgpsegmentCount();
    options->split_size = getIntOption(foreign_options, DATALAKE_ICEBERG_CATALOG_SPLIT_SIZE, 128);
    options->filter_string = getStringOption(foreign_options, DATALAKE_ICEBERG_CATALOG_FILTER_STRING);
	options->prefix = getStringOption(foreign_options, DATALAKE_ICEBERG_CATALOG_REST_PREFIX);
	if (options->prefix == NULL)
		options->prefix = options->catalog_name;
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
		validateRestCrossObjectOptions(options);

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
	validateRestCrossObjectOptions(options);
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

/*
 * getCatalogServerType
 *      Read the "type" option of a catalog server, falling back to the
 *      legacy "server_type" key.  Returns NULL when neither is set.
 */
static char *
getCatalogServerType(const char* catalogServerName)
{
    ForeignServer *server;
    char *server_type;

    server = GetForeignServerByName(catalogServerName, false);
    server_type = getStringOption(server->options,
                                  DATALAKE_ICEBERG_CATALOG_SERVER_TYPE);
    /* Fallback: also check "server_type" (legacy option key) */
    if (server_type == NULL)
        server_type = getStringOption(server->options, "server_type");

    return server_type;
}

/*
 * checkIsRestCatalog
 *      Does this catalog server speak the Iceberg REST spec?
 *
 * This is the protocol question.  A REST catalog hands out the location of
 * each table individually, so the location cannot be composed from a
 * statically configured FOREIGN VOLUME the way it is for hive/hadoop.
 * Both "polaris" and the generic "rest" type answer yes.
 *
 * Do not confuse this with checkIsPolarisCatalog() below: that one answers
 * the dialect question and must stay Polaris-only.
 */
bool checkIsRestCatalog(const char* catalogServerName, const char* catalogName)
{
    char *server_type;

    if (catalogServerName == NULL || catalogName == NULL)
        return false;

    server_type = getCatalogServerType(catalogServerName);
    if (server_type == NULL)
        return false;

    return pg_strcasecmp(server_type,
                         DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_POLARIS) == 0 ||
           pg_strcasecmp(server_type,
                         DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_REST) == 0;
}

/*
 * checkIsPolarisCatalog
 *      Is this catalog server a Polaris deployment?
 *
 * This is the dialect question, not the protocol question.  It gates the
 * proprietary Polaris admin API (POST /api/management/v1/catalogs) and the
 * Polaris-Realm header, neither of which belongs to the Iceberg REST spec.
 *
 * "rest" must NOT be accepted here.  A generic REST catalog such as XSKY
 * has no admin API, and treating it as Polaris makes us POST to an
 * endpoint that does not exist.  For "is this a REST catalog?" callers
 * want checkIsRestCatalog() instead.
 */
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

/*
 * checkIsGenericRestCatalog
 *      REST protocol, non-Polaris dialect -- the storage-addressing axis.
 *
 * The two questions above are easy to reach for by the wrong name: asking
 * checkIsRestCatalog() alone at a site that means "vended envelope, no
 * volume" silently drags Polaris in, which is the one deployment that must
 * not change.  Sites that mean this combination say so by name.
 */
bool checkIsGenericRestCatalog(const char* catalogServerName,
                               const char* catalogName)
{
    if (catalogServerName == NULL || catalogName == NULL)
        return false;

    return checkIsRestCatalog(catalogServerName, catalogName) &&
           !checkIsPolarisCatalog(catalogServerName, catalogName);
}

/*
 * checkHasMaintenanceApi
 *      Does this catalog server declare a maintenance API we can drive?
 *
 * A fourth axis, and deliberately not folded into the three above.  Whether
 * the platform serves a maintenance API is a property of the deployment, not
 * of the protocol or the dialect: the same server answers or refuses
 * depending on a switch its operator sets.  Encoding it as a catalog type
 * would tie two independent facts together and would silently downgrade
 * existing servers configured before the option existed.
 *
 * Returns the declared value ("s3tables") or NULL.  Only meaningful for a
 * generic REST catalog; callers are expected to have established that.
 */
char *getCatalogMaintenanceApi(const char* catalogServerName)
{
    ForeignServer *server;

    if (catalogServerName == NULL)
        return NULL;

    server = GetForeignServerByName(catalogServerName, false);
    return getStringOption(server->options,
                           DATALAKE_ICEBERG_CATALOG_REST_MAINTENANCE_API);
}