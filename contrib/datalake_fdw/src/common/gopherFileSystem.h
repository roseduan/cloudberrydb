/*-------------------------------------------------------------------------
 *
 * gopherFileSystem.h
 *    Gopher-based FileSystem implementation (commercial build only).
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/gopherFileSystem.h
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_GOPHER_FILESYSTEM_H
#define DATALAKE_GOPHER_FILESYSTEM_H

#include "fileSystem.h"
#include "gopher/gopher.h"
#include <string>

namespace Datalake {
namespace Internal {

class GopherFileSystem : public FileSystem {

public:
	GopherFileSystem();
	~GopherFileSystem() override;

	int createHandle(void *storageOptions) override;
	int openFile(const char *path, int flag) override;
	int write(void *buff, int64_t size) override;
	int read(void *buff, int64_t size) override;
	int seek(int64_t position) override;
	int closeFile() override;
	int getUfsId() override;
	datalakeFileInfo* listInfo(const char *path, int &count,
							   int recursive = 1, bool iswrite = false) override;
	datalakeFileInfo* getFileInfo(const char *path) override;
	int destroyHandle() override;
	int deleteFile(const char *path) override;

	/* Expose UFS type for fileSystemWrapper compatibility */
	int getUfsType() const { return ufsType; }

private:
	static bool checkCanceled(void);

	/* Build gopherConfig from storageOptions (storageOptions*) */
	gopherConfig* buildGopherConfig(void *storageOptions);
	void freeGopherConfig(gopherConfig *conf);

	/* Convert gopherFileInfo array to datalakeFileInfo array */
	datalakeFileInfo* convertFileInfo(gopherFileInfo *ginfo, int count);

	gopherFS    fs;
	gopherFile  file;
	std::string filePath;
	bool        closed;
	int         ufsType;
};

} /* namespace Internal */
} /* namespace Datalake */

#endif /* DATALAKE_GOPHER_FILESYSTEM_H */
