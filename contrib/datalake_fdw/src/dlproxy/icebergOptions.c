#include "icebergOptions.h"

/* help function */
// static IcebergOptions *getIcebergOptions(Oid foreigntableid);
// static void freeIcebergOptions(IcebergOptions *options);
IcebergOptions *getIcebergConfigV2Options(Oid foreigntableid);
void freeIcebergConfigV2Options(IcebergOptions *options);

/* function */

IcebergOptions *getIcebergConfigV2Options(Oid foreigntableid) {
    //TODO
    return NULL;
}

void freeIcebergConfigV2Options(IcebergOptions *options) {
    //TODO
}

// static IcebergOptions*
// getIcebergOptions(Oid foreigntableid)
// {
//     //TODO we get some options data from fdw for test
//     IcebergOptions *icebergOptions = (IcebergOptions *)palloc(sizeof(IcebergOptions));

//     icebergOptions->catalog_server.server_type = pstrdup("hive");
//     icebergOptions->catalog_server.hive_metastore_uri = pstrdup("thrift://127.0.0.1:9083");
//     icebergOptions->catalog_user.username = pstrdup("hive");
//     icebergOptions->catalog_user.auth_method = pstrdup("simple");
//     icebergOptions->catalog_user.krb_service_principal = pstrdup("hive/hashdata@HASHDATA.CN");
//     icebergOptions->catalog_user.krb_client_principal = pstrdup("hive/hashdata@HASHDATA.CN");
//     icebergOptions->catalog_user.krb_client_keytab = pstrdup("/home/gpadmin/user.keytab");

//     icebergOptions->foreign_catalog.catalog_name = pstrdup("production");
//     icebergOptions->foreign_catalog.default_namespace = pstrdup("default");
//     icebergOptions->foreign_catalog.metadata_cache = 60;
//     icebergOptions->foreign_catalog.warehouse_location_prefix = pstrdup("s3://warehouse/production/");

//     icebergOptions->volume_server.server_type = pstrdup("hive");
//     icebergOptions->volume_server.endpoint = pstrdup("s3://warehouse/production/");
//     icebergOptions->volume_server.region = pstrdup("us-west-2");
//     icebergOptions->volume_server.bucket_name = pstrdup("bucket_name");
//     icebergOptions->volume_server.path_style_access = false;


//     icebergOptions->volume_user.username = pstrdup("hive");
//     icebergOptions->volume_user.aws_access_key_id = pstrdup("xxx");
//     icebergOptions->volume_user.aws_secret_access_key = pstrdup("xxx");

//     icebergOptions->foreign_volume.base_path = pstrdup("/iceberg-warehouse/");
//     icebergOptions->foreign_volume.enable_caching = true;
//     icebergOptions->foreign_volume.allow_writes = true;

//     icebergOptions->table.table_name = pstrdup("test_table");
//     icebergOptions->table.namespace_name = pstrdup("default");
//     icebergOptions->table.base_location = pstrdup("s3://warehouse/production/");

//     return icebergOptions;
// }

// static void
// freeIcebergOptions(IcebergOptions *options)
// {
//     if (options == NULL)
//     {
//         return;
//     }
//     if (options->catalog_server.server_type)
//     {
//         pfree(options->catalog_server.server_type);
//     }
//     if (options->catalog_server.hive_metastore_uri)
//     {
//         pfree(options->catalog_server.hive_metastore_uri);
//     }
//     if (options->catalog_user.username)
//     {
//         pfree(options->catalog_user.username);
//     }
//     if (options->catalog_user.auth_method)
//     {
//         pfree(options->catalog_user.auth_method);
//     }
//     if (options->catalog_user.krb_service_principal)
//     {
//         pfree(options->catalog_user.krb_service_principal);
//     }
//     if (options->catalog_user.krb_client_principal)
//     {
//         pfree(options->catalog_user.krb_client_principal);
//     }
//     if (options->catalog_user.krb_client_keytab)
//     {
//         pfree(options->catalog_user.krb_client_keytab);
//     }
//     if (options->foreign_catalog.catalog_name)
//     {
//         pfree(options->foreign_catalog.catalog_name);
//     }
//     if (options->foreign_catalog.default_namespace)
//     {
//         pfree(options->foreign_catalog.default_namespace);
//     }

//     if (options->foreign_catalog.warehouse_location_prefix)
//     {
//         pfree(options->foreign_catalog.warehouse_location_prefix);
//     }

//     if (options->volume_server.server_type)
//     {
//         pfree(options->volume_server.server_type);
//     }
//     if (options->volume_server.endpoint)
//     {
//         pfree(options->volume_server.endpoint);
//     }
//     if (options->volume_server.region)
//     {
//         pfree(options->volume_server.region);
//     }
//     if (options->volume_server.bucket_name)
//     {
//         pfree(options->volume_server.bucket_name);
//     }

//     if (options->volume_user.username)
//     {
//         pfree(options->volume_user.username);
//     }
//     if (options->volume_user.aws_access_key_id)
//     {
//         pfree(options->volume_user.aws_access_key_id);
//     }
//     if (options->volume_user.aws_secret_access_key)
//     {
//         pfree(options->volume_user.aws_secret_access_key);
//     }

//     if (options->foreign_volume.base_path)
//     {
//         pfree(options->foreign_volume.base_path);
//     }

//     if (options->table.table_name)
//     {
//         pfree(options->table.table_name);
//     }
//     if (options->table.namespace_name)
//     {
//         pfree(options->table.namespace_name);
//     }
//     if (options->table.base_location)
//     {
//         pfree(options->table.base_location);
//     }

//     pfree(options);
// }


