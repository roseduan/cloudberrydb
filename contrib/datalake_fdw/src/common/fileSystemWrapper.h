/*-------------------------------------------------------------------------
 *
 * fileSystemWrapper.h
 *    Public C interface for datalake storage file operations.
 *
 *    This header does NOT include any Gopher-specific types.
 *    The underlying implementation (Gopher or S3) is selected
 *    at compile time inside fileSystemWrapper.cpp.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/fileSystemWrapper.h
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_FILESYSTEMWRAPPER_H
#define DATALAKE_FILESYSTEMWRAPPER_H

#include "datalake_storage.h"

struct ossInternalFileStream;

typedef struct ossInternalFileStream *ossFileStream;

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Create a storage filesystem handle from storageOptions (storageOptions*).
 * Internally selects GopherFileSystem or S3FileSystem based on build config.
 */
ossFileStream datalakeCreateFileSystem(void *storageOptions);

/*
 * Create an independent stream that talks to the same storage as `file`
 * (own connection + own open-file handle). Used where two readers must hold
 * concurrent file positions on the same table, e.g. Hudi MOR's base data-file
 * reader and delta-log reader. Returns NULL if `file` is NULL. Caller owns the
 * result and must release it with datalakeDestroyFileSystem().
 */
ossFileStream datalakeCloneFileStream(ossFileStream file);

int datalakeOpenFile(ossFileStream file, const char *path, int flag);

int datalakeWriteFile(ossFileStream file, void *buff, int64_t size);

int datalakeReadFile(ossFileStream file, void *buff, int64_t size);

int datalakeSeekFile(ossFileStream file, int64_t position);

/*
 * datalakeTellFile - the stream's current logical read/write position.
 *
 * The position is tracked by this wrapper (see ossInternalFileStream::pos
 * in fileSystemWrapper.cpp), not queried from the backend: neither
 * Gopher's C API nor the FileSystem interface exposes a tell. That is
 * sound because every cursor movement inside datalake_fdw goes through
 * datalakeOpenFile/datalakeSeekFile/datalakeReadFile/datalakeWriteFile,
 * so the wrapper is the single place that observes them all. A backend
 * added later must keep routing its I/O through these entry points; one
 * that moves the cursor behind the wrapper's back would make this value
 * drift silently.
 *
 * Returns -1 when file is NULL.
 */
int64_t datalakeTellFile(ossFileStream file);

int datalakeGetUfsId(ossFileStream file);

int datalakeCloseFile(ossFileStream file);

datalakeFileInfo* datalakeListDir(ossFileStream file, const char *path,
								  int *count, int recursive);

datalakeFileInfo* datalakeGetFileInfo(ossFileStream file, const char *path);

void datalakeFreeFileInfo(datalakeFileInfo *list, int count);

int datalakeDestroyHandle(ossFileStream file);

void datalakeDestroyFileSystem(ossFileStream file);

/*
 * datalakeGetLastError - return the last error message from the storage layer.
 *
 * In Gopher mode, delegates to gopherGetLastError().
 * In S3 mode, returns the last exception message captured by the wrapper.
 * The returned pointer is valid until the next datalake API call.
 */
const char *datalakeGetLastError(void);

/*
 * Best-effort delete of a single object `path`, using a fresh filesystem
 * handle built from `storageOpt` (a `storageOptions *`). Returns 0 on
 * success, non-zero otherwise. Never throws.
 */
int datalakeDeleteFileByOptions(void *storageOpt, const char *path);

/*
 * Read the metadata blob that came back piggy-backed on openFile's
 * response. Zero RPC.
 * Returns 0 on a hit; non-zero means a miss / the backend does not
 * support this.
 * *data points into the backend's internal buffer, valid only until the
 * stream is closed via datalakeCloseFile().
 */
int datalakeGetFileMeta(ossFileStream file, uint32_t *producerId, int32_t *type,
						int64_t *offset, const void **data, int32_t *length);

/* Backfill. A failure (including being rejected for exceeding a limit) only ever returns non-zero -- it NEVER elog(ERROR)s. */
int datalakePutFileMeta(ossFileStream file, uint32_t producerId, int32_t type,
						int64_t offset, const void *data, int32_t length);

#ifdef __cplusplus
}
#endif

#endif /* DATALAKE_FILESYSTEMWRAPPER_H */
