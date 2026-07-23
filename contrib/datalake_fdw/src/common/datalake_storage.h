/*-------------------------------------------------------------------------
 *
 * datalake_storage.h
 *    Storage-agnostic public types for datalake_fdw.
 *
 *    These types replace Gopher-specific types (gopherFileInfo, etc.)
 *    in all public interfaces, enabling datalake_fdw to build without
 *    any Gopher dependency.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/datalake_storage.h
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_STORAGE_H
#define DATALAKE_STORAGE_H

#include <stdint.h>
#include <stdbool.h>
#include <fcntl.h>

/*
 * datalakeFileInfo - storage-agnostic file metadata.
 *
 * Replaces gopherFileInfo in all public interfaces. Only contains
 * fields actually used by datalake_fdw consumers (path and length).
 */
typedef struct datalakeFileInfo
{
	char   *path;          /* file path (palloc'd) */
	int64_t length;        /* file size in bytes */
	bool    isDirectory;   /* true if directory entry */
} datalakeFileInfo;

/*
 * File access flags - originally from gopher.h.
 *
 * These constants are needed by all storage backends (Gopher, S3, etc.)
 * for datalakeOpenFile() calls.  When building with Gopher, the
 * authoritative definitions come from gopher/gopher.h; these fallback
 * definitions ensure the open-source build works without it.
 */
#ifndef O_FNCACHE
#define O_FNCACHE 0x00001000
#endif
#ifndef O_RDONCE
#define O_RDONCE 0x00002000
#endif
#ifndef O_RDTHR
#define O_RDTHR 0x000004000
#endif

#endif /* DATALAKE_STORAGE_H */
