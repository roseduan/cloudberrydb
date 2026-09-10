/*-------------------------------------------------------------------------
 *
 * iceberg_site_config.h
 *    Reader for the iceberg volume site configuration files, s3.conf and
 *    gphdfs.conf.
 *
 * This is the C counterpart of the agent's SiteConfigLoader.  It exists so
 * the QD can resolve a volume's server_name section itself and ship the
 * merged configuration to the QEs, which have no access to those files.
 * The behaviour it must reproduce -- including the error semantics and the
 * YAML 1.1 implicit typing SnakeYAML applies -- is specified in
 * .router/plans/410/W1-equivalence-spec.md; every deviation documented
 * there is deliberate and is marked in the implementation.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/iceberg_site_config.h
 *-------------------------------------------------------------------------
 */
#ifndef ICEBERG_SITE_CONFIG_H
#define ICEBERG_SITE_CONFIG_H

#include "postgres.h"

#include "nodes/pg_list.h"

typedef enum IcebergSiteVolumeKind
{
	ICEBERG_SITE_VOLUME_S3,
	ICEBERG_SITE_VOLUME_HDFS
} IcebergSiteVolumeKind;

/*
 * A section key with no typed slot.  These are forwarded to the agent
 * untouched; nothing in the C code interprets them.  Mirrors the Java
 * VolumeInfo.extraProperties, which is a HashMap -- so the list order here
 * carries no meaning and must not be compared against the agent's.  (The
 * order of the *legacy key* error message is a different matter: that one
 * comes from the section's own LinkedHashMap and is part of the contract.)
 */
typedef struct IcebergSiteExtra
{
	char	   *key;
	char	   *value;
} IcebergSiteExtra;

/*
 * The parsed section.  A NULL string member means the key was absent, or
 * present with a value that YAML resolves to null -- the two are the same
 * thing to the agent, which merges SQL options over this layer per key.
 * Booleans therefore need a companion "_set" flag rather than defaulting
 * to false, so an unwritten key never shadows an SQL option.
 */
typedef struct IcebergSiteVolumeInfo
{
	IcebergSiteVolumeKind kind;
	char	   *server_name;
	char	   *volume_server_type;

	/* S3 section keys */
	char	   *endpoint;
	char	   *region;
	char	   *bucket_name;
	char	   *access_key_id;
	char	   *secret_access_key;
	bool		path_style_access;
	bool		path_style_access_set;

	/* shared by both kinds */
	char	   *username;

	/* HDFS section keys */
	char	   *hdfs_namenode_host;
	char	   *hdfs_namenode_port;
	char	   *hdfs_auth_method;
	char	   *krb_principal;
	char	   *krb_principal_keytab;
	char	   *hadoop_rpc_protection;
	char	   *data_transfer_protocol;
	char	   *data_transfer_protection;
	char	   *dfs_nameservices;
	char	   *dfs_ha_namenodes;
	char	   *dfs_namenode_rpc_address;
	char	   *dfs_client_failover_proxy_provider;
	bool		is_ha_supported;
	bool		is_ha_supported_set;
	bool		dfs_client_use_datanode_hostname;
	bool		dfs_client_use_datanode_hostname_set;

	List	   *extras;			/* of IcebergSiteExtra * */
} IcebergSiteVolumeInfo;

/*
 * Read the section named serverName out of s3.conf or gphdfs.conf, chosen
 * from sqlVolumeType the way the agent chooses it: "hdfs" case-insensitively
 * selects gphdfs.conf, anything else -- including a NULL or unrecognised
 * type -- selects s3.conf.
 *
 * baseDir is the directory the file is resolved against; NULL means the
 * process working directory, which is what the agent uses and therefore the
 * only value production code should pass.  A non-NULL baseDir exists for the
 * differential fixture, mirroring SiteConfigLoader.setBaseDir().
 *
 * location is the fallback source for bucket_name.  It is dead in the
 * iceberg path today -- see the spec -- and production callers must keep
 * passing NULL so the C side does not invent a bucket the agent never had.
 *
 * Returns NULL when serverName is empty (no file is opened at all) or when
 * the file is empty.  Everything else is reported with ereport(ERROR).
 * Allocation happens in CurrentMemoryContext; the caller owns the result.
 */
extern IcebergSiteVolumeInfo *icebergSiteLoadVolume(const char *baseDir,
													const char *serverName,
													const char *sqlVolumeType,
													const char *location);

/*
 * Exposed only so the differential fixture can exercise them directly; the
 * iceberg path reaches them through icebergSiteLoadVolume().
 */
extern char *icebergSiteUnescapeString(const char *path);
extern void icebergSiteParseBucketAndPrefix(const char *location,
											char **bucketName,
											char **prefix);

#endif							/* ICEBERG_SITE_CONFIG_H */
