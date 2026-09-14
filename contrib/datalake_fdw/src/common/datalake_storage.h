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

/*
 * This FDW's producer identifier in Gopher's file-metadata cache.
 * Gopher does not interpret this value, it just stores and returns it
 * as-is; it exists to keep different producers' metaType values from
 * colliding.
 */
#define DATALAKE_PRODUCER_ID  0x444C4657u   /* "DLFW" */

/*
 * Once released, these values are fixed and must never be reused or
 * reordered -- the blobs are persisted in Gopher's rocksdb, and changing
 * a value would make an old blob get misparsed as a different format.
 * Always append new formats at the end.
 */
typedef enum datalakeFileMetaType
{
	DATALAKE_META_ORC_TAIL       = 1,  /* Phase 1: ORC tail (EOF-anchored) */
	DATALAKE_META_PARQUET_FOOTER = 2,  /* Phase 3 */
	DATALAKE_META_AVRO_HEADER    = 3,  /* Phase 3 */
	DATALAKE_META_PAX_TAIL       = 4,  /* still to be evaluated */
} datalakeFileMetaType;

#endif /* DATALAKE_STORAGE_H */
