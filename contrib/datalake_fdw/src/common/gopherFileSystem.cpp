/*-------------------------------------------------------------------------
 *
 * gopherFileSystem.cpp
 *    Gopher-based FileSystem implementation.
 *
 *    Extracted from the original fileSystem.cpp and the config-building
 *    logic from fileSystemWrapper.cpp (datalakeCreateGopherConfig).
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/gopherFileSystem.cpp
 *-------------------------------------------------------------------------
 */
#include "gopherFileSystem.h"
#include <iostream>
#include <vector>

extern "C" {
#include "postgres.h"

#include "utils/elog.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "access/xact.h"
#include "cdb/cdbvars.h"
#include "libpq/libpq-be.h"

#include "src/datalake_option.h"
#include "src/datalake_def.h"
#include "src/provider/providerWrapper.h"
#include "util.h"
}

#define GOPHER_BLOCK_SIZE (1024 * 1024 * gopher_local_blocksize_mb)

/*
 * GUC datalake.disable_cache_file (defined in datalake_fdw.c). Default OFF
 * (false) = caching enabled. Gates the gopher cache_strategy so the GUC
 * globally controls block caching for all datalake reads (incl. native
 * iceberg AM tables, whose volume options never set enableCache).
 */
extern "C" bool disableCacheFile;

namespace Datalake {
namespace Internal {

GopherFileSystem::GopherFileSystem()
{
	fs = NULL;
	file = NULL;
	closed = true;
	ufsType = 0;
}

GopherFileSystem::~GopherFileSystem()
{
	if (file != NULL)
	{
		closeFile();
		file = NULL;
	}
	if (fs != NULL)
	{
		destroyHandle();
		fs = NULL;
	}
}

bool GopherFileSystem::checkCanceled(void)
{
	return InterruptPending;
}

/* ----------------------------------------------------------------
 * createHandle - create Gopher connection from storageOptions
 *
 * storageOptions is storageOptions* from datalake_def.h.
 * Internally builds gopherConfig and calls gopherConnect().
 * ----------------------------------------------------------------
 */
int GopherFileSystem::createHandle(void *options)
{
	gopherConfig *conf = buildGopherConfig(options);
	if (conf == NULL)
		throw Error("failed to build gopher config");

	ufsType = conf->ufs_type;

	gopherUserCanceledCallBack(&checkCanceled);

	fs = gopherConnect(*conf);

	freeGopherConfig(conf);

	if (fs == NULL)
		throw Error("failed to connect to gopher: %s.", gopherGetLastError());

	return 0;
}

int GopherFileSystem::openFile(const char *path, int flag)
{
	if (path == NULL)
	{
		return -1;
	}

	/*
	 * Close any file this handle already has open before opening a new one.
	 * A GopherFileSystem holds a single "file" member, so reopening without
	 * closing would leak the previous gopherFile. This mirrors
	 * HdfsFileSystem::openFile and keeps the two backends consistent.
	 */
	if (file != NULL)
	{
		gopherCloseFile(fs, file, true);
		file = NULL;
	}

	closed = false;
	filePath = path;
	std::string gopher_prefix = "";
	std::string prefix = path;
	int num = prefix.find_first_of("/");
	if (num == 0)
	{
		gopher_prefix.append(path);
	}
	else
	{
		gopher_prefix.append("/").append(path);
	}

	elog(DEBUG5, "Datalake foreign table gopherOpenFile path:%s flag:%d block_size:%d",
					gopher_prefix.c_str(), flag, GOPHER_BLOCK_SIZE);

	file = gopherOpenFile(fs, gopher_prefix.c_str(), flag, GOPHER_BLOCK_SIZE, NULL);
	if (file == NULL)
	{
		elog(ERROR, "gopher open \"%s\" failed. error message:%s.", path, gopherGetLastError());
		return -1;
	}
	return 0;
}

int GopherFileSystem::write(void *buff, int64_t size)
{
	elog(DEBUG5, "Datalake foreign table gopherWrite path %s size %ld.", filePath.c_str(), size);

	if (size == 0)
	{
		return size;
	}
	int32_t writtenSize = gopherWrite(fs, file, buff, size);
	if (writtenSize == -1)
	{
		throw Error("failed to write: \"%s\" size %ld %s.", filePath.c_str(), size,
			gopherGetLastError());
	}
	return writtenSize;
}

int GopherFileSystem::read(void *buff, int64_t size) {
	elog(DEBUG5, "Datalake foreign table gopherRead path %s size %ld.", filePath.c_str(), size);

	if (size == 0)
	{
		return size;
	}
	int readSize = gopherRead(fs, file, buff, size);
	if (readSize == -1)
	{
		throw Error("failed to read: \"%s\" size %ld %s.", filePath.c_str(), size,
			gopherGetLastError());
	}
	return readSize;
}

int GopherFileSystem::seek(int64_t position) {
	elog(DEBUG5, "Datalake foreign table gopherSeek path %s postion %ld.", filePath.c_str(), position);

	int ret = gopherSeek(fs, file, position);
	if (ret == -1)
	{
		throw Error("failed to seek: \"%s\" postion %ld %s.", filePath.c_str(), position,
			gopherGetLastError());
	}
	return ret;
}

int GopherFileSystem::getUfsId() {
	elog(DEBUG5, "Datalake foreign table gopherGetUfsId.");

	int ufsId = gopherGetUfsId(fs);
	if (ufsId < 0)
	{
		elog(WARNING, "get invalid gopher ufsId %d %s.", ufsId, gopherGetLastError());
		return -1;
	}
	return ufsId;
}

int GopherFileSystem::closeFile() {
	elog(DEBUG5, "Datalake foreign table gopherCloseFile.");

	if (closed)
	{
		/* file maybe already closed */
		return 0;
	}
	closed = true;
	int ret = gopherCloseFile(fs, file, true);
	if (ret == 0)
	{
		file = NULL;
	}
	else
	{
		file = NULL;
		elog(ERROR, "gopherCloseFile failed path \"%s\" return value %d error message:%s.",
			filePath.c_str(), ret, gopherGetLastError());
	}
	return ret;
}

/* ----------------------------------------------------------------
 * listInfo - list directory via Gopher, return datalakeFileInfo
 * ----------------------------------------------------------------
 */
datalakeFileInfo *GopherFileSystem::listInfo(const char *path, int &count, int recursive, bool iswrite) {
	int numEntries = 0;
	std::string gopher_prefix = "";
	std::string prefix = "";

	if (recursive)
	{
		if (path != NULL)
		{
			prefix = path;
			if (prefix.front() != '/')
			{
				gopher_prefix.append("/").append(prefix);
			}
			else
			{
				gopher_prefix.append(prefix);
			}
		}
	}
	else
	{
		gopher_prefix.append(path);
	}

	if (external_table_debug)
	{
		elog(LOG, "Datalake foreign table gopherListDirectory path %s recursive %d.", gopher_prefix.c_str(), recursive);
	}

	gopherFileInfo *res = gopherListDirectory(fs, gopher_prefix.c_str(), recursive, &numEntries, enable_get_block_location,
											  gp_session_id, gp_command_count, iswrite);
	if (numEntries == -1)
		throw Error("failed to list directory: \"%s\" %s.", path, gopherGetLastError());

	if (res == NULL)
	{
		numEntries = 0;
	}

	if (external_table_debug)
	{
		elog(LOG, "Datalake foreign table list %d files.", numEntries);
	}

	if (numEntries == 0)
	{
		elog(LOG, "Datalake foreign table read empty folder : %s.", gopher_prefix.c_str());
	}

	count = numEntries;

	/* Convert gopherFileInfo → datalakeFileInfo */
	datalakeFileInfo *result = convertFileInfo(res, numEntries);
	return result;
}

datalakeFileInfo* GopherFileSystem::getFileInfo(const char* path) {
	elog(DEBUG5, "Datalake foreign table gopherGetFileInfo path %s.", path);

	gopherFileInfo* info = gopherGetFileInfo(fs, path);
	if (info == NULL)
	{
		throw Error("gopherGetFileInfo failed, path \"%s\" error message %s.", path, gopherGetLastError());
	}

	/* Convert single gopherFileInfo → datalakeFileInfo */
	datalakeFileInfo *result = convertFileInfo(info, 1);
	return result;
}

int GopherFileSystem::destroyHandle() {
	elog(DEBUG5, "Datalake foreign table gopherDisconnect.");

	int ret = gopherDisconnect(fs);
	if (ret == 0)
	{
		fs = NULL;
	}
	else
	{
		fs = NULL;
		throw Error("failed to disconnect to gopher: return value %d, error message %s.", ret, gopherGetLastError());
	}
	return ret;
}

int GopherFileSystem::deleteFile(const char *path) {
	/* per-file delete; NOT gopherBatchDeleteFiles (known-buggy) */
	return gopherDelete(fs, path);
}

/* ----------------------------------------------------------------
 * convertFileInfo - convert gopherFileInfo[] to datalakeFileInfo[]
 *
 * Copies the fields used by datalake_fdw consumers (path, length),
 * then frees the original Gopher info.
 * ----------------------------------------------------------------
 */
datalakeFileInfo*
GopherFileSystem::convertFileInfo(gopherFileInfo *ginfo, int count)
{
	if (ginfo == NULL || count == 0)
		return NULL;

	datalakeFileInfo *result = (datalakeFileInfo*)palloc0(sizeof(datalakeFileInfo) * count);
	for (int i = 0; i < count; i++)
	{
		result[i].path = pstrdup(ginfo[i].mPath);
		result[i].length = ginfo[i].mLength;
		result[i].isDirectory = ginfo[i].mDirectory;
	}

	/* Free the original Gopher file info */
	gopherFreeFileInfo(ginfo, count);
	return result;
}

/* ----------------------------------------------------------------
 * Helper: split string by delimiter
 * ----------------------------------------------------------------
 */
static void splitString(std::string inputStr, std::string delimiter, std::vector<std::string> &out)
{
	std::string str = inputStr + delimiter;
	size_t pos = str.find(delimiter);
	int step = delimiter.size();

	while(pos != str.npos)
	{
		std::string tmp = str.substr(0, pos);
		out.push_back(tmp);
		str = str.substr(pos + step, str.size());
		pos = str.find(delimiter);
	}
}

/* ----------------------------------------------------------------
 * Helper: build HDFS HA configuration
 * ----------------------------------------------------------------
 */
static HdfsHAConfig* getHdfsHAConfig(storageOptions *options)
{
	std::vector<std::string> namenodes;
	if (options->dfs_ha_namenodes)
	{
		splitString(options->dfs_ha_namenodes, ",", namenodes);
	}
	if (namenodes.size() == 0)
	{
		elog(WARNING, "Options set is_ha_supported is true, "
		"but cannot get dfs_ha_namenodes %s please check hdfs ha config.",
			options->dfs_ha_namenodes);
		options->hdfs_ha_configs_num = 0;
		return NULL;
	}

	options->hdfs_ha_configs_num = namenodes.size() + 3;

	HdfsHAConfig *hdfs_ha_configs = (HdfsHAConfig*)palloc0(sizeof(HdfsHAConfig) *
			options->hdfs_ha_configs_num);

	/* ha nameservices */
	hdfs_ha_configs[0].key = pstrdup("dfs.nameservices");
	hdfs_ha_configs[0].value = pstrdup(options->dfs_name_services);

	/* ha namenodes */
	char buffer[1024] = {0};
	sprintf(buffer, "dfs.ha.namenodes.%s", options->dfs_name_services);
	hdfs_ha_configs[1].key = pstrdup(buffer);
	hdfs_ha_configs[1].value = pstrdup(options->dfs_ha_namenodes);

	/* dfs client */
	sprintf(buffer, "dfs.client.failover.proxy.provider.%s", options->dfs_name_services);
	hdfs_ha_configs[2].key = pstrdup(buffer);
	hdfs_ha_configs[2].value = pstrdup(options->dfs_client_failover);

	/* ha namenode rpc addr */
	std::vector<std::string> rpc_addr;
	if (options->dfs_ha_namenode_rpc_addr)
	{
		splitString(options->dfs_ha_namenode_rpc_addr, ",", rpc_addr);
	}

	if (rpc_addr.size() != namenodes.size())
	{
		elog(ERROR, "Invalid Config dfs.namenode.rpc-address: %s and "
		"dfs.ha.namenodes %s , count not equal", options->dfs_ha_namenode_rpc_addr,
			options->dfs_ha_namenodes);
	}

	int num = 0;
	for (int i = 3; i < options->hdfs_ha_configs_num; i++)
	{
		sprintf(buffer, "dfs.namenode.rpc-address.%s.%s",
			options->dfs_name_services,
			namenodes[num].c_str());

		hdfs_ha_configs[i].key = pstrdup(buffer);
		hdfs_ha_configs[i].value = pstrdup(rpc_addr[num].c_str());
		num++;
	}

	return hdfs_ha_configs;
}

/* ----------------------------------------------------------------
 * buildGopherConfig - convert storageOptions to gopherConfig
 *
 * Moved from fileSystemWrapper.cpp datalakeCreateGopherConfig().
 * ----------------------------------------------------------------
 */
gopherConfig*
GopherFileSystem::buildGopherConfig(void *opts)
{
	storageOptions *options = (storageOptions*)opts;
	gopherConfig* conf = (gopherConfig*)palloc0(sizeof(gopherConfig));
	conf->connect_path = pstrdup(options->connect_path);
	conf->connect_plasma_path = pstrdup(options->connect_plasma_path);
	conf->worker_path = pstrdup(options->worker_path);
	conf->master_ip = pstrdup(MyProcPort ? MyProcPort->remote_host : "127.0.0.1");
	conf->external_hdfs_list_use_master = enable_list_in_master;

	if (options->enableCache || !disableCacheFile)
	{
		conf->cache_strategy = GOPHER_CACHE;
	}
	else
	{
		conf->cache_strategy = GOPHER_NOT_CACHE;
	}

	if (pg_strcasecmp(strConvertLow(options->gopherType), "hdfs") == 0)
	{
		conf->ufs_type = HDFS;

		conf->port = options->hdfs_namenode_port;

		if (options->hdfs_namenode_host)
		{
			std::vector<std::string> hostAndPort;
			splitString(options->hdfs_namenode_host, ":", hostAndPort);
			conf->name_node = pstrdup(hostAndPort[0].c_str());
			if (hostAndPort.size() > 1)
			{
				conf->port = std::stoi(hostAndPort[1]);
			}
		}

		if (options->hdfs_auth_method)
		{
			conf->auth_method = pstrdup(options->hdfs_auth_method);
		}

		if (options->hadoop_rpc_protection)
		{
			conf->hadoop_rpc_protection = pstrdup(options->hadoop_rpc_protection);
		}

		if (options->data_transfer_protection)
		{
			conf->data_transfer_protection = pstrdup(options->data_transfer_protection);
		}

		if (conf->auth_method && strcmp(conf->auth_method, "kerberos") == 0)
		{
			if (options->krb_principal)
			{
				conf->krb_principal = pstrdup(options->krb_principal);
			}

			if (options->krb_principal_keytab)
			{
				conf->krb_server_key_file = pstrdup(options->krb_principal_keytab);
			}

			char krb5_ccname[1024] = {0};
			snprintf(krb5_ccname, 1024, "%s/gophermeta/krb5cc_%s", DataDir, conf->krb_principal);
			char* krb5Str = strstr(krb5_ccname, "krb5cc");
			int len = strlen(krb5Str);
			for (int i = 0; i < len; i++)
			{
				if (!isalnum(krb5Str[i]))
				{
					krb5Str[i] = '_';
				}
			}
			conf->krb5_ticket_cache_path = pstrdup(krb5_ccname);
		}

		if (enable_set_hdfs_user && options->hdfs_user)
		{
			conf->hdfs_username = pstrdup(options->hdfs_user);
		}

		conf->data_transfer_protocol = options->data_transfer_protocol;
		conf->is_ha_supported = options->is_ha_supported;

		if (options->is_ha_supported)
		{
			/* parser ha config */
			conf->hdfs_ha_configs = getHdfsHAConfig(options);
			conf->hdfs_ha_configs_num = options->hdfs_ha_configs_num;
		}
	}
	else if (pg_strcasecmp(strConvertLow(options->gopherType), "ftp") == 0)
	{
		conf->ufs_type = FTP;
		conf->config = (ftpConfig *) palloc0(sizeof(ftpConfig));
		std::string ftp_url = "ftp://";
		ftp_url.append(options->host).append("/");
		ftp_url.append(options->ftp_path);
		conf->config->host = pstrdup(ftp_url.c_str());
		conf->config->username = pstrdup(options->ftp_username);
		conf->config->password = pstrdup(options->ftp_password);
	}
	else
	{
		if (pg_strcasecmp(strConvertLow(options->gopherType), "qs") == 0)
		{
			conf->ufs_type = QINGSTOR;
		}
		else if (pg_strcasecmp(strConvertLow(options->gopherType), "huawei") == 0)
		{
			conf->ufs_type = HUAWEI;
		}
		else if (pg_strcasecmp(strConvertLow(options->gopherType), "ks3") == 0)
		{
			conf->ufs_type = KSYUN;
		}
		else if (pg_strcasecmp(strConvertLow(options->gopherType), "cos") == 0)
		{
			conf->ufs_type = QCLOUD;
		}
		else if (pg_strcasecmp(strConvertLow(options->gopherType), "ali") == 0)
		{
			conf->ufs_type = OSS;
		}
		else if (pg_strcasecmp(strConvertLow(options->gopherType), "s3") == 0)
		{
			conf->ufs_type = S3A;
		}
		else if (pg_strcasecmp(strConvertLow(options->gopherType), "s3b") == 0)
		{
			conf->ufs_type = S3AV2;
		}
		else if (pg_strcasecmp(strConvertLow(options->gopherType), "azure") == 0)
		{
			conf->ufs_type = AZURE;
		}
		else if (pg_strcasecmp(strConvertLow(options->gopherType), "gcs") == 0)
		{
			conf->ufs_type = GCS;
		}
		else
		{
			ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("unkonw datalake platform \"%s\"", options->gopherType)));
		}

		if (options->bucket)
		{
			conf->bucket = pstrdup(options->bucket);
		}

		if (options->accessKey)
		{
			conf->access_key = pstrdup(options->accessKey);
		}

		if (options->secretKey)
		{
			conf->secret_key = pstrdup(options->secretKey);
		}

		if (options->host)
		{
			char endpoint[1024] = {0};
			if (options->port > 0)
			{
				char port[100] = {0};
				sprintf(port, "%d", options->port);
				std::string portStr = port;
				std::string ss = ":";
				portStr = ss + portStr;
				std::string hostStr = options->host;
				size_t pos = hostStr.find(portStr);
				if (pos > 0)
				{
					ereport(WARNING,
						errmsg("Please check the server options. Found port %d in the host %s. "
						"datalake support config host and port.",
							options->port, options->host));
				}
				snprintf(endpoint, 1024, "%s:%d", options->host, options->port);
			}
			else
			{
				snprintf(endpoint, 1024, "%s", options->host);
			}
			conf->endpoint = pstrdup(endpoint);
		}

		if (options->region)
		{
			conf->region = pstrdup(options->region);
		}

		conf->useVirtualHost = options->useVirtualHost;
		conf->useHttps = options->useHttps;
		conf->useListV2 = options->useListV2;
	}

	return conf;
}

/* ----------------------------------------------------------------
 * freeGopherConfig - free a gopherConfig built by buildGopherConfig
 *
 * Moved from fileSystemWrapper.cpp datalakeFreeGopherConfig().
 * ----------------------------------------------------------------
 */
void
GopherFileSystem::freeGopherConfig(gopherConfig* conf)
{
	if (conf)
	{
		if (conf->connect_path != NULL)
		{
			pfree(conf->connect_path);
			conf->connect_path = NULL;
		}

		if (conf->connect_plasma_path != NULL)
		{
			pfree(conf->connect_plasma_path);
			conf->connect_plasma_path = NULL;
		}

		if (conf->worker_path != NULL)
		{
			pfree(conf->worker_path);
			conf->worker_path = NULL;
		}

		if (conf->bucket != NULL)
		{
			pfree(conf->bucket);
			conf->bucket = NULL;
		}

		if (conf->access_key != NULL)
		{
			pfree(conf->access_key);
			conf->access_key = NULL;
		}

		if (conf->secret_key != NULL)
		{
			pfree(conf->secret_key);
			conf->secret_key = NULL;
		}

		if (conf->region != NULL)
		{
			pfree(conf->region);
			conf->region = NULL;
		}

		if (conf->endpoint != NULL)
		{
			pfree(conf->endpoint);
			conf->endpoint = NULL;
		}

		if (conf->local_path != NULL)
		{
			pfree(conf->local_path);
			conf->local_path = NULL;
		}

		if (conf->name_node != NULL)
		{
			pfree(conf->name_node);
			conf->name_node = NULL;
		}

		if (conf->uriPrefix != NULL)
		{
			pfree(conf->uriPrefix);
			conf->uriPrefix = NULL;
		}

		if (conf->auth_method != NULL)
		{
			pfree(conf->auth_method);
			conf->auth_method = NULL;
		}

		if (conf->krb_delegation_token != NULL)
		{
			pfree(conf->krb_delegation_token);
			conf->krb_delegation_token = NULL;
		}

		if (conf->krb5_ticket_cache_path != NULL)
		{
			pfree(conf->krb5_ticket_cache_path);
			conf->krb5_ticket_cache_path = NULL;
		}

		if (conf->krb_server_key_file != NULL)
		{
			pfree(conf->krb_server_key_file);
			conf->krb_server_key_file = NULL;
		}

		if (conf->krb_principal != NULL)
		{
			pfree(conf->krb_principal);
			conf->krb_principal = NULL;
		}

		if (conf->hdfs_username != NULL)
		{
			pfree(conf->hdfs_username);
			conf->hdfs_username = NULL;
		}

		if (conf->hadoop_rpc_protection != NULL)
		{
			pfree(conf->hadoop_rpc_protection);
			conf->hadoop_rpc_protection = NULL;
		}

		if (conf->data_transfer_protection != NULL)
		{
			pfree(conf->data_transfer_protection);
			conf->data_transfer_protection = NULL;
		}

		if ((conf->hdfs_ha_configs_num) > 0 && (conf->hdfs_ha_configs != NULL))
		{
			for (int i = 0; i < conf->hdfs_ha_configs_num; i++)
			{
				if (conf->hdfs_ha_configs[i].key)
				{
					pfree(conf->hdfs_ha_configs[i].key);
					conf->hdfs_ha_configs[i].key = NULL;
				}
				if (conf->hdfs_ha_configs[i].value)
				{
					pfree(conf->hdfs_ha_configs[i].value);
					conf->hdfs_ha_configs[i].value = NULL;
				}
			}
			pfree(conf->hdfs_ha_configs);
			conf->hdfs_ha_configs = NULL;
		}

		pfree(conf);
		conf = NULL;
	}
}

} /* namespace Internal */
} /* namespace Datalake */
