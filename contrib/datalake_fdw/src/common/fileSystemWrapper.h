#ifndef DATALAKE_FILESYSTEMWRAPPER_H
#define DATALAKE_FILESYSTEMWRAPPER_H
#include <gopher/gopher.h>

struct ossInternalFileStream;

typedef struct ossInternalFileStream *ossFileStream;

#ifdef __cplusplus
extern "C" {
#endif

gopherConfig* datalakeCreateGopherConfig(void *opt);

void datalakeFreeGopherConfig(gopherConfig* conf);

ossFileStream datalakeCreateFileSystem(gopherConfig *conf);

int datalakeOpenFile(ossFileStream file, const char *path, int flag);

int datalakeWriteFile(ossFileStream file, void *buff, int64_t size);

int datalakeReadFile(ossFileStream file, void *buff, int64_t size);

int datalakeSeekFile(ossFileStream file, int64_t postion);

int datalakeGetUfsId(ossFileStream file);

int datalakeCloseFile(ossFileStream file);

gopherFileInfo* datalakeListDir(ossFileStream file, const char *path, int *count, int recursive);

gopherFileInfo* datalakeGetFileInfo(ossFileStream file, const char* path);

void datalakeFreeListDir(ossFileStream file, gopherFileInfo *list, int count);

int datalakeGopherDestroyHandle(ossFileStream file);

void datalakeDestroyFileSystem(ossFileStream file);

/*
 * Best-effort delete of a single object `path`, using a fresh filesystem handle
 * built from `gopherOpt` (a `gopherOptions *`). Returns 0 on success, non-zero
 * otherwise. Never throws.
 */
int datalakeDeleteFileByOptions(void *gopherOpt, const char *path);
#ifdef __cplusplus
}
#endif


#endif
