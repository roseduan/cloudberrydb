#ifndef ICEBERG_CATALOG_OPTION_H
#define ICEBERG_CATALOG_OPTION_H

#include "postgres.h"
#include "nodes/pg_list.h"

/*
 * Server options for the generic Iceberg REST catalog type.
 *
 * endpoint is the object-storage address.  When unset it is derived from
 * url (origin only); users are encouraged to set it explicitly because the
 * derivation only holds for deployments where the catalog and the object
 * storage share one address (e.g. XSKY's three APIs on a single port).
 */
typedef struct RestCatalogOptions
{
	char *url;					/* Iceberg REST base URL (required) */
	char *region;				/* signs catalog requests; data-plane fallback */
	char *signing_name;			/* SigV4 service name (e.g. "s3tables") */
	char *endpoint;				/* object storage address; derived if unset */
	bool path_style_access;	/* default true */
	/*
	 * Which proprietary maintenance API this deployment serves, if any.
	 * The Iceberg REST spec defines none, so compaction is offered -- when it
	 * is offered at all -- by a surface alongside the catalog rather than
	 * inside it.  Absent means the platform exposes none we can drive, which
	 * is the default and leaves VACUUM refusing as before.  Availability is a
	 * deployment property, not a protocol one: the same platform serves the
	 * API only when its operator switches it on, which is why this is an
	 * option of its own rather than another catalog type.
	 */
	char *maintenance_api;		/* NULL, or "s3tables" */

	/*
	 * Realm sent as the Polaris-Realm header, for a REST catalog that happens
	 * to be a Polaris deployment.  Vendor-specific on a type that is
	 * otherwise protocol-only, for the same reason maintenance_api is: the
	 * alternative is that such a deployment simply cannot be reached over
	 * the generic type at all.  A Polaris server configured with a
	 * non-default realm answers the OAuth token request with 404
	 * MissingOrInvalidRealm when the header is absent (issue #841).  Unset
	 * sends no header, which is what every non-Polaris platform wants.
	 */
	char *polaris_server_realm;

	/*
	 * Server-level default for the USER MAPPING option of the same name; see
	 * RestUserMappingOptions.vended_credentials.  Accepted here because that
	 * is where every other "how do we talk to this catalog" setting lives,
	 * and a deployment that never uses vending should be able to say so once
	 * instead of on every user mapping.  A USER MAPPING that sets it wins;
	 * ICEBERG_VENDED_UNSET means this server expressed no preference.
	 */
	int vended_credentials;
} RestCatalogOptions;

/* Structure for Iceberg catalog server options */
typedef struct IcebergCatalogServerOptions
{
	char *server_type;				/* DATALAKE_ICEBERG_CATALOG_SERVER_TYPE */
	char *server_name;				/* config file section name (e.g. hive.conf segment) */
	char *hive_metastore_uri;		/* DATALAKE_ICEBERG_CATALOG_HIVE_METASTORE_URI */
	char *polaris_server_url;		/* DATALAKE_ICEBERG_CATALOG_POLARIS_SERVER_URL */
	char *polaris_server_realm;		/* DATALAKE_ICEBERG_CATALOG_POLARIS_SERVER_REALM */
	RestCatalogOptions rest;
} IcebergCatalogServerOptions;

/* Hive user mapping options */
typedef struct HiveUserMappingOptions
{
	char *username;
	char *auth_method;
	char *krb_service_principal;
	char *krb_client_principal;
	char *krb_client_keytab;
} HiveUserMappingOptions;

/* Polaris user mapping options */
typedef struct PolarisUserMappingOptions
{
	char *client_id;
	char *client_secret;
	char *scope;
} PolarisUserMappingOptions;

/*
 * User mapping options for the generic REST catalog.
 *
 * Two independent credential groups live here.  auth_method selects how
 * catalog HTTP requests are signed (none when unset); access_key_id /
 * secret_access_key are the user's object-storage credentials -- always
 * usable as the data-plane fallback, and under sigv4 the same pair also
 * signs catalog requests.
 */
typedef struct RestUserMappingOptions
{
	char *auth_method;			/* sigv4 | oauth2 | none (default) */
	char *access_key_id;
	char *secret_access_key;
	char *client_id;			/* oauth2 only */
	char *client_secret;		/* oauth2 only */
	char *scope;				/* oauth2 only, optional */

	/*
	 * Ask the catalog to vend data-plane credentials (the Iceberg REST
	 * "vended-credentials" delegation)?  Default true, which is the whole
	 * point of a per-table-bucket catalog: each table's keys are scoped to
	 * that table.  Turning it off makes the request omit the delegation
	 * header, so the catalog returns no credentials and the static
	 * access_key_id/secret_access_key below are used for every table.
	 *
	 * That is strictly weaker isolation, so it is opt-out and never a
	 * fallback: the segment does not silently switch groups when vended
	 * credentials fail, because papering over a catalog fault with broader
	 * static keys is exactly what must not happen quietly.
	 *
	 * Read this only after resolveRestVendedCredentials() has run: the
	 * option may also be set on the SERVER, and until the two objects are
	 * reconciled this field carries only what the USER MAPPING said.
	 * ICEBERG_VENDED_UNSET means it said nothing.
	 */
	int vended_credentials;
} RestUserMappingOptions;

/*
 * Tri-state for vended_credentials.  A plain bool cannot distinguish "the
 * user turned it off" from "the user said nothing", and that distinction is
 * what lets the USER MAPPING override a SERVER-level default instead of
 * silently reinstating it.
 */
#define ICEBERG_VENDED_UNSET	(-1)
#define ICEBERG_VENDED_OFF		0
#define ICEBERG_VENDED_ON		1

/* Structure for Iceberg catalog user mapping options */
typedef struct IcebergCatalogUserMappingOptions
{
	HiveUserMappingOptions hive;
	PolarisUserMappingOptions polaris;
	RestUserMappingOptions rest;
} IcebergCatalogUserMappingOptions;

/* Structure for Iceberg foreign catalog options */
typedef struct IcebergForeignCatalogOptions
{
	char *catalog_name;			 /* DATALAKE_ICEBERG_CATALOG_NAME */
	char *default_namespace;		/* DATALAKE_ICEBERG_CATALOG_DEFAULT_NAMESPACE */
	int metadata_cache;			 /* DATALAKE_ICEBERG_CATALOG_METADATA_CACHE;
								  * minutes, 0 = no caching.  Catalog-level
								  * default for the per-table option of the
								  * same name and unit. */
	char *warehouse_location_prefix; /* DATALAKE_ICEBERG_CATALOG_WAREHOUSE_LOCATION_PREFIX */
	int total_segment;
	int split_size;
	char *filter_string;
	char *prefix;				/* REST route prefix; defaults to catalog_name */
} IcebergForeignCatalogOptions;

typedef struct IcebergCatalogOptions
{
	/* Catalog related options */
	IcebergCatalogServerOptions catalog_server;
	IcebergCatalogUserMappingOptions catalog_user;
	IcebergForeignCatalogOptions foreign_catalog;

} IcebergCatalogOptions;

#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE "type"
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_HIVE "hive"
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_POLARIS "polaris"
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_HADOOP "hadoop"
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_S3 "s3"
/*
 * Generic Iceberg REST catalog (XSKY, AWS S3 Tables, ...).  "polaris" is
 * one particular REST catalog that additionally speaks two things which
 * are not part of the Iceberg REST spec: the Polaris-Realm header and the
 * POST /api/management/v1/catalogs admin API.
 */
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_REST "rest"

/*
 * SQL option names for the generic REST catalog.  These are public API:
 * frozen at design review, hard to change once released.  region,
 * path_style_access and access_key_id/secret_access_key deliberately reuse
 * the names the volume/table-AM line already uses, auth_method reuses
 * hive's user-mapping key.
 */
#define DATALAKE_ICEBERG_CATALOG_REST_REGION "region"
#define DATALAKE_ICEBERG_CATALOG_REST_SIGNING_NAME "signing_name"
#define DATALAKE_ICEBERG_CATALOG_REST_ENDPOINT "endpoint"
#define DATALAKE_ICEBERG_CATALOG_REST_PATH_STYLE_ACCESS "path_style_access"
#define DATALAKE_ICEBERG_CATALOG_REST_AUTH_METHOD "auth_method"
#define DATALAKE_ICEBERG_CATALOG_REST_ACCESS_KEY_ID "access_key_id"
#define DATALAKE_ICEBERG_CATALOG_REST_SECRET_ACCESS_KEY "secret_access_key"
#define DATALAKE_ICEBERG_CATALOG_REST_PREFIX "prefix"
#define DATALAKE_ICEBERG_CATALOG_REST_VENDED_CREDENTIALS "vended_credentials"
#define DATALAKE_ICEBERG_CATALOG_REST_MAINTENANCE_API "maintenance_api"
#define DATALAKE_ICEBERG_MAINTENANCE_API_S3TABLES "s3tables"

extern char *deriveEndpointFromUrl(const char *url);
extern void validateRestServerOptionList(List *server_options);

IcebergCatalogOptions *getIcebergCatalogOptions(const char* catalogServerName, const char* catalogName);

IcebergCatalogOptions*
getIcebergPolarisCatalogOptions(const char* catalogServerName, const char* catalogName, List* catalogOptions);

/*
 * Two different questions get asked about a catalog server.  They used to
 * have a single answer because Polaris was the only REST catalog we spoke
 * to; they must not be confused now that there is a generic "rest" type.
 *
 * checkIsRestCatalog()    -- protocol axis.  True when the catalog speaks
 *                            the Iceberg REST spec, so a table's location
 *                            must be fetched from the catalog per table
 *                            instead of composed from a FOREIGN VOLUME.
 *                            True for both "polaris" and "rest".
 *
 * checkIsPolarisCatalog() -- dialect axis.  True only for "polaris", the
 *                            one implementation that also exposes the
 *                            proprietary admin API.  Accepting "rest" here
 *                            would make us call an endpoint that generic
 *                            REST catalogs do not have.
 *
 * checkIsGenericRestCatalog()
 *                         -- REST protocol minus the Polaris dialect, i.e.
 *                            the tables that address storage per table via a
 *                            vended envelope and have no FOREIGN VOLUME.
 *                            Anything that must leave the Polaris deployment
 *                            behaving exactly as before asks this one, never
 *                            checkIsRestCatalog() on its own.
 */
bool checkIsRestCatalog(const char* catalogServerName, const char* catalogName);
bool checkIsPolarisCatalog(const char* catalogServerName, const char* catalogName);
char *getCatalogMaintenanceApi(const char* catalogServerName);
bool checkIsGenericRestCatalog(const char* catalogServerName,
                               const char* catalogName);
bool checkIsBuiltinCatalog(const char* catalogServerName, const char* catalogName);
#endif