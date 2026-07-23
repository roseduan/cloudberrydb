/*-------------------------------------------------------------------------
 *
 * fileSystem.h
 *    Abstract base class for datalake storage file systems.
 *
 *    Concrete implementations: GopherFileSystem (commercial),
 *    S3FileSystem (open-source).
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/fileSystem.h
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_FILESYSTEM_H
#define DATALAKE_FILESYSTEM_H

#include "datalake_storage.h"
#include <exception>
#include <cstdarg>
#include <cstddef>
#include <cstdio>

#define ERROR_STR_LEN 1024

namespace Datalake {
namespace Internal {

struct Error : std::exception
{
	char text[ERROR_STR_LEN];

	Error(char const* fmt, ...) __attribute__((format(printf,2,3)))
	{
		va_list ap;
		va_start(ap, fmt);
		vsnprintf(text, sizeof text, fmt, ap);
		va_end(ap);
	}

	char const* what() const throw() { return text; }
};

class FileSystem {

public:
	virtual ~FileSystem() {}

	/*
	 * Create a storage handle from storageOptions (storageOptions*).
	 * Each implementation converts the options to its own config format.
	 */
	virtual int createHandle(void *storageOptions) = 0;

	virtual int openFile(const char *path, int flag) = 0;

	virtual int write(void *buff, int64_t size) = 0;

	virtual int read(void *buff, int64_t size) = 0;

	virtual int seek(int64_t position) = 0;

	virtual int closeFile() = 0;

	virtual int getUfsId() = 0;

	virtual datalakeFileInfo* listInfo(const char *path, int &count,
									   int recursive = 1, bool iswrite = false) = 0;

	virtual datalakeFileInfo* getFileInfo(const char *path) = 0;

	/*
	 * Note: freeListInfo is NOT in the virtual interface.
	 * datalakeFileInfo uses palloc'd memory in all backends, so
	 * datalakeFreeFileInfo() in fileSystemWrapper.cpp handles freeing
	 * uniformly without virtual dispatch.
	 */

	virtual int destroyHandle() = 0;

	/*
	 * deleteFile - best-effort single-object delete; returns 0 on success,
	 * non-zero otherwise. All shipped backends override with a real delete
	 * (Gopher: gopherDelete, S3: DeleteObject, HDFS: hdfsDelete). The base
	 * default is a no-op returning -1 ("not supported") so a future backend
	 * still compiles. Used by the abort-time staging-file cleanup (#344).
	 */
	virtual int deleteFile(const char *path) { return -1; }

	/*
	 * getName - return a short, human-readable backend name used in
	 * diagnostic messages (e.g. "S3: connection timeout" vs bare
	 * "connection timeout"). Default "Unknown" keeps the commercial
	 * Gopher path compiling without source edits; native backends
	 * override. Inspired by DuckDB's FileSystem::GetName().
	 */
	virtual const char *getName() const { return "Unknown"; }
};

} /* namespace Internal */
} /* namespace Datalake */

#endif /* DATALAKE_FILESYSTEM_H */
