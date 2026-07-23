/*-------------------------------------------------------------------------
 *
 * hdfsFileSystem.h
 *    HDFS FileSystem backend for datalake_fdw, using libhdfs3.
 *
 *    Registers under protocol 'hdfs' in BackendRegistry. Supports
 *    simple (no-auth) and Kerberos authentication; HA is supported
 *    when is_ha_supported=true.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/hdfsFileSystem.h
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_HDFS_FILESYSTEM_H
#define DATALAKE_HDFS_FILESYSTEM_H

#include "fileSystem.h"
#include <vector>

/*
 * Forward-declare libhdfs3 opaque types to keep this header free of
 * hdfs/hdfs.h (only the .cpp needs that).
 */
struct HdfsFileSystemInternalWrapper;
struct HdfsFileInternalWrapper;

namespace Datalake {
namespace Internal {

class HdfsFileSystem : public FileSystem {
public:
	HdfsFileSystem();
	~HdfsFileSystem() override;

	int createHandle(void *storageOptions) override;
	int openFile(const char *path, int flag) override;
	int write(void *buff, int64_t size) override;
	int read(void *buff, int64_t size) override;
	int seek(int64_t position) override;
	int closeFile() override;
	int getUfsId() override;
	const char *getName() const override { return "HDFS"; }
	datalakeFileInfo* listInfo(const char *path, int &count,
							   int recursive = 1, bool iswrite = false) override;
	datalakeFileInfo* getFileInfo(const char *path) override;
	int destroyHandle() override;
	int deleteFile(const char *path) override;

private:
	struct HdfsFileSystemInternalWrapper *fs_ = nullptr;
	struct HdfsFileInternalWrapper *file_ = nullptr;

	/* recursive helper for listInfo */
	void listRecursive(const char *path, std::vector<datalakeFileInfo> &out);
};

} /* namespace Internal */
} /* namespace Datalake */

#endif
