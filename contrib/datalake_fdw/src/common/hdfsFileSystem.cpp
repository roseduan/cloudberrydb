/*-------------------------------------------------------------------------
 *
 * hdfsFileSystem.cpp
 *    HDFS FileSystem backend for datalake_fdw, using libhdfs3.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/hdfsFileSystem.cpp
 *-------------------------------------------------------------------------
 */
#include "hdfsFileSystem.h"
#include "backendRegistry.h"

#include <hdfs/hdfs.h>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "src/datalake_def.h"
}

namespace Datalake {
namespace Internal {

/* Default HDFS port when not specified. */
static const int HDFS_DEFAULT_PORT = 8020;

HdfsFileSystem::HdfsFileSystem() {}

HdfsFileSystem::~HdfsFileSystem()
{
	if (file_)
	{
		hdfsCloseFile(fs_, file_);
		file_ = nullptr;
	}
	if (fs_)
	{
		hdfsDisconnect(fs_);
		fs_ = nullptr;
	}
}

/*
 * Parse "host:port" or just "host" into separate host + port.
 * On pure "host", port is left as defaultPort.
 */
static void
splitHostPort(const char *combined, std::string &host, int &port, int defaultPort)
{
	if (!combined || !*combined)
	{
		host = "localhost";
		port = defaultPort;
		return;
	}
	const char *colon = strchr(combined, ':');
	if (colon)
	{
		host.assign(combined, colon - combined);
		port = atoi(colon + 1);
		if (port <= 0)
			port = defaultPort;
	}
	else
	{
		host = combined;
		port = defaultPort;
	}
}

int
HdfsFileSystem::createHandle(void *storageOptions)
{
	auto *opts = static_cast<struct storageOptions *>(storageOptions);
	if (opts == nullptr)
		throw Error("HdfsFileSystem::createHandle: storageOptions is NULL");

	hdfsBuilder *bld = hdfsNewBuilder();
	if (!bld)
		throw Error("hdfsNewBuilder returned NULL");

	/*
	 * HA vs non-HA: in HA the nameservice alias goes as the namenode;
	 * actual NN addresses are supplied via dfs.namenode.rpc-address.<svc>.<id>.
	 */
	if (opts->is_ha_supported && opts->dfs_name_services && *opts->dfs_name_services)
	{
		hdfsBuilderSetNameNode(bld, opts->dfs_name_services);
		hdfsBuilderSetNameNodePort(bld, 0);

		hdfsBuilderConfSetStr(bld, "dfs.nameservices", opts->dfs_name_services);

		if (opts->dfs_ha_namenodes && *opts->dfs_ha_namenodes)
		{
			std::string k = std::string("dfs.ha.namenodes.") + opts->dfs_name_services;
			hdfsBuilderConfSetStr(bld, k.c_str(), opts->dfs_ha_namenodes);

			if (opts->dfs_ha_namenode_rpc_addr && *opts->dfs_ha_namenode_rpc_addr)
			{
				/*
				 * Expected format: "nn1:host1:port1,nn2:host2:port2" (key:host:port).
				 * If the value is "host1:port1,host2:port2" we pair it with the
				 * namenode IDs in order.
				 */
				std::string rpc = opts->dfs_ha_namenode_rpc_addr;
				std::string ids = opts->dfs_ha_namenodes;
				size_t i = 0, j = 0;
				while (true)
				{
					size_t comma_i = ids.find(',', i);
					size_t comma_j = rpc.find(',', j);
					std::string id = ids.substr(i, (comma_i == std::string::npos ? std::string::npos : comma_i - i));
					std::string addr = rpc.substr(j, (comma_j == std::string::npos ? std::string::npos : comma_j - j));
					if (id.empty() || addr.empty())
						break;
					std::string key = std::string("dfs.namenode.rpc-address.") +
						opts->dfs_name_services + "." + id;
					hdfsBuilderConfSetStr(bld, key.c_str(), addr.c_str());
					if (comma_i == std::string::npos || comma_j == std::string::npos)
						break;
					i = comma_i + 1;
					j = comma_j + 1;
				}
			}
		}

		if (opts->dfs_client_failover && *opts->dfs_client_failover)
		{
			std::string k = std::string("dfs.client.failover.proxy.provider.") +
				opts->dfs_name_services;
			hdfsBuilderConfSetStr(bld, k.c_str(), opts->dfs_client_failover);
		}
	}
	else
	{
		/* Non-HA: hdfs_namenode_host may be "host" or "host:port". */
		std::string host;
		int port = (opts->hdfs_namenode_port > 0) ?
			opts->hdfs_namenode_port : HDFS_DEFAULT_PORT;
		splitHostPort(opts->hdfs_namenode_host, host, port, port);

		hdfsBuilderSetNameNode(bld, host.c_str());
		hdfsBuilderSetNameNodePort(bld, (tPort) port);
	}

	/* Authentication */
	if (opts->hdfs_auth_method &&
		pg_strcasecmp(opts->hdfs_auth_method, "kerberos") == 0)
	{
		hdfsBuilderConfSetStr(bld, "hadoop.security.authentication", "kerberos");
		if (opts->krb_principal && *opts->krb_principal)
			hdfsBuilderSetUserName(bld, opts->krb_principal);
		if (opts->krb_principal_keytab && *opts->krb_principal_keytab)
			hdfsBuilderSetKerbTicketCachePath(bld, opts->krb_principal_keytab);
		if (opts->hadoop_rpc_protection && *opts->hadoop_rpc_protection)
			hdfsBuilderConfSetStr(bld, "hadoop.rpc.protection", opts->hadoop_rpc_protection);
	}
	else
	{
		/* simple auth; set effective user if provided */
		if (opts->hdfs_user && *opts->hdfs_user)
			hdfsBuilderSetUserName(bld, opts->hdfs_user);
	}

	/*
	 * Preferred datanode connection style:
	 * - Inside Docker Compose networks the dev container resolves DN
	 *   hostnames (dn1, etc.) via the shared network; force hostname
	 *   mode so NN returns hostnames the client can resolve.
	 */
	hdfsBuilderConfSetStr(bld, "dfs.client.use.datanode.hostname", "true");

	fs_ = hdfsBuilderConnect(bld);
	if (!fs_)
		throw Error("hdfsBuilderConnect failed (check namenode reachability)");

	return 0;
}

int
HdfsFileSystem::openFile(const char *path, int flag)
{
	if (!fs_)
		throw Error("HdfsFileSystem::openFile: handle not created");
	if (file_)
	{
		hdfsCloseFile(fs_, file_);
		file_ = nullptr;
	}

	/*
	 * Caller may pass POSIX flags (O_RDONLY/O_WRONLY/O_CREAT/…) plus the
	 * Gopher-cache bits (O_FNCACHE/O_RDONCE/O_RDTHR) which HDFS ignores.
	 * libhdfs3 only consumes O_RDONLY / O_WRONLY / O_APPEND.
	 */
	int hdfsFlag;
	if ((flag & O_WRONLY) || (flag & O_RDWR))
		hdfsFlag = O_WRONLY | (flag & O_APPEND ? O_APPEND : 0);
	else
		hdfsFlag = O_RDONLY;

	/*
	 * For write opens, HDFS requires the parent directory to exist.
	 * S3 object stores synthesize the prefix on PUT, so callers are
	 * not used to pre-creating dirs. Auto-create on our side so HDFS
	 * behavior matches.
	 */
	if (hdfsFlag & O_WRONLY)
	{
		const char *slash = strrchr(path, '/');
		if (slash && slash != path)
		{
			std::string parent(path, slash - path);
			if (hdfsExists(fs_, parent.c_str()) != 0)
				hdfsCreateDirectoryEx(fs_, parent.c_str(), 0777, 1);
		}
	}

	file_ = hdfsOpenFile(fs_, path, NULL, 0, hdfsFlag, 0, 0, 0);
	if (!file_)
	{
		int savedErrno = errno;
		const char *lastErr = hdfsGetLastError();
		throw Error("hdfsOpenFile(%s, flag=%d) failed: %s (errno=%d: %s)",
					path, hdfsFlag,
					lastErr ? lastErr : "(no detail)",
					savedErrno, strerror(savedErrno));
	}

	return 0;
}

int
HdfsFileSystem::write(void *buff, int64_t size)
{
	if (!file_)
		throw Error("HdfsFileSystem::write: no open file");

	tSize written = hdfsWrite(fs_, file_, buff, (tSize) size);
	if (written < 0)
		throw Error("hdfsWrite failed (returned %d)", (int) written);
	return (int) written;
}

int
HdfsFileSystem::read(void *buff, int64_t size)
{
	if (!file_)
		throw Error("HdfsFileSystem::read: no open file");

	tSize n = hdfsRead(fs_, file_, buff, (tSize) size);
	if (n < 0)
		throw Error("hdfsRead failed (returned %d)", (int) n);
	return (int) n;
}

int
HdfsFileSystem::seek(int64_t position)
{
	if (!file_)
		throw Error("HdfsFileSystem::seek: no open file");

	if (hdfsSeek(fs_, file_, (tOffset) position) != 0)
		throw Error("hdfsSeek(%lld) failed", (long long) position);
	return 0;
}

int
HdfsFileSystem::closeFile()
{
	if (!file_)
		return 0;
	int rc = hdfsCloseFile(fs_, file_);
	file_ = nullptr;
	if (rc != 0)
		throw Error("hdfsCloseFile failed (rc=%d)", rc);
	return 0;
}

int
HdfsFileSystem::getUfsId()
{
	return 9;  /* HDFS family - matches Gopher's convention */
}

int
HdfsFileSystem::destroyHandle()
{
	if (file_)
	{
		hdfsCloseFile(fs_, file_);
		file_ = nullptr;
	}
	if (fs_)
	{
		hdfsDisconnect(fs_);
		fs_ = nullptr;
	}
	return 0;
}

int
HdfsFileSystem::deleteFile(const char *path)
{
	if (!fs_)
		return -1;

	/* recursive = 0: single object, never a directory tree */
	return hdfsDelete(fs_, path, 0);
}

static void
copyEntry(const hdfsFileInfo &src, datalakeFileInfo &dst)
{
	/* mName is a fully-qualified URI like "hdfs://nn1:9000/path/to/file".
	 * The wrapper consumers generally want the path component; to stay
	 * consistent with the Gopher backend behavior we keep the full mName
	 * and let iceberg/hudi layers handle URI parsing. Copy via palloc.
	 */
	dst.path = pstrdup(src.mName ? src.mName : "");
	dst.length = (int64_t) src.mSize;
	dst.isDirectory = (src.mKind == kObjectKindDirectory);
}

void
HdfsFileSystem::listRecursive(const char *path, std::vector<datalakeFileInfo> &out)
{
	int n = 0;
	hdfsFileInfo *entries = hdfsListDirectory(fs_, path, &n, false);
	if (!entries)
	{
		/* empty or nonexistent: silently return */
		return;
	}
	for (int i = 0; i < n; i++)
	{
		datalakeFileInfo info;
		copyEntry(entries[i], info);
		if (info.isDirectory)
		{
			std::string child = entries[i].mName;
			listRecursive(child.c_str(), out);
			if (info.path) pfree(info.path);
		}
		else
		{
			out.push_back(info);
		}
	}
	hdfsFreeFileInfo(entries, n);
}

datalakeFileInfo *
HdfsFileSystem::listInfo(const char *path, int &count, int recursive, bool iswrite)
{
	if (!fs_)
		throw Error("HdfsFileSystem::listInfo: handle not created");

	std::vector<datalakeFileInfo> collected;
	if (recursive)
	{
		listRecursive(path, collected);
	}
	else
	{
		int n = 0;
		hdfsFileInfo *entries = hdfsListDirectory(fs_, path, &n, false);
		if (entries)
		{
			for (int i = 0; i < n; i++)
			{
				datalakeFileInfo info;
				copyEntry(entries[i], info);
				collected.push_back(info);
			}
			hdfsFreeFileInfo(entries, n);
		}
	}

	count = (int) collected.size();
	if (count == 0)
		return nullptr;

	datalakeFileInfo *result = (datalakeFileInfo *)
		palloc(sizeof(datalakeFileInfo) * count);
	for (int i = 0; i < count; i++)
		result[i] = collected[i];
	return result;
}

datalakeFileInfo *
HdfsFileSystem::getFileInfo(const char *path)
{
	if (!fs_)
		throw Error("HdfsFileSystem::getFileInfo: handle not created");

	hdfsFileInfo *entry = hdfsGetPathInfo(fs_, path);
	if (!entry)
		throw Error("hdfsGetPathInfo(%s) failed", path);

	datalakeFileInfo *out = (datalakeFileInfo *) palloc(sizeof(datalakeFileInfo));
	copyEntry(*entry, *out);
	hdfsFreeFileInfo(entry, 1);
	return out;
}

/* ---- Backend registration ----
 * Must be inside the namespace so the macro's cls##_Registrar symbol is
 * a valid identifier (qualified names like ::Datalake::Internal::Cls
 * break the token-concat).
 */
DATALAKE_REGISTER_BACKEND("hdfs", HdfsFileSystem);

} /* namespace Internal */
} /* namespace Datalake */
