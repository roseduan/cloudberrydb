/*-------------------------------------------------------------------------
 *
 * fileSystemWrapper.cpp
 *    C wrapper over the abstract FileSystem class.
 *
 *    Selects the concrete FileSystem implementation at compile time:
 *    GopherFileSystem (commercial) or S3FileSystem (open-source).
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/fileSystemWrapper.cpp
 *-------------------------------------------------------------------------
 */
extern "C" {
#include "postgres.h"
#include "utils/elog.h"
#include "src/datalake_def.h"   /* for struct storageOptions */
}

#include "fileSystemWrapper.h"
#include "fileSystem.h"
#ifdef USE_GOPHER
#include "gopherFileSystem.h"
#else
#include "backendRegistry.h"
/*
 * We no longer include a specific backend header here - backends
 * register themselves via DATALAKE_REGISTER_BACKEND. The wrapper only
 * knows the abstract FileSystem interface and the registry.
 */
#endif
#include <exception>
#include <cstring>
#include <memory>

using Datalake::Internal::FileSystem;

/*
 * Thread-local last-error buffer for datalakeGetLastError().
 * Captures exception messages from the storage backend so callers
 * can include error detail in elog() messages.
 */
static __thread char lastErrorBuf[1024] = {0};

static void
setLastError(const char *msg)
{
	strncpy(lastErrorBuf, msg, sizeof(lastErrorBuf) - 1);
	lastErrorBuf[sizeof(lastErrorBuf) - 1] = '\0';
}
#ifdef USE_GOPHER
using Datalake::Internal::GopherFileSystem;
#else
using Datalake::Internal::BackendRegistry;
#endif

struct ossInternalFileStream {
public:
	ossInternalFileStream(FileSystem *ctx) : context(ctx) {}

	~ossInternalFileStream()
	{
		if (context != NULL)
		{
			delete context;
			context = NULL;
		}
	}

	FileSystem &getContext()
	{
		return *context;
	}

	int type;

	/*
	 * storageOptions pointer used to create this stream; kept so
	 * datalakeCloneFileStream() can build an independent stream with its
	 * own FileSystem (own connection + file handle).  Points at
	 * caller-owned options that outlive the stream (scan-scoped).
	 */
	void *storageOptions = NULL;

private:
	FileSystem *context;
};

#ifdef __cplusplus
extern "C" {
#endif

#define PARAMETER_ASSERT(para, eno) \
    if (!(para)) {  \
        errno = eno; \
        elog(ERROR, "Datalake foreign table Error, Parameter assert failed."); \
    }

/*
 * datalakeCreateFileSystem - create a storage handle from storageOptions.
 *
 * storageOptions is a storageOptions* from datalake_def.h.
 *
 * Commercial (USE_GOPHER): GopherFileSystem drives all backends
 * internally via ufsType.
 *
 * Open-source: BackendRegistry dispatches on the protocol string set
 * by CREATE SERVER ... OPTIONS (protocol '...'). New backends register
 * themselves via DATALAKE_REGISTER_BACKEND; no edit to this file is
 * needed to add one.
 */
ossFileStream datalakeCreateFileSystem(void *storageOptions)
{
	ossInternalFileStream *fileStream = NULL;

	try
	{
#ifdef USE_GOPHER
		/*
		 * Own the FileSystem via unique_ptr until it is safely handed to
		 * the ossInternalFileStream: createHandle() may throw, and the
		 * catch below longjmp's out of elog(ERROR); the unique_ptr frees
		 * the object during stack unwinding so it is not leaked.
		 */
		std::unique_ptr<FileSystem> file(new GopherFileSystem());
		file->createHandle(storageOptions);
		/* Cache UFS type for getFileInfo path adjustment (HDFS = 9) */
		GopherFileSystem *gfs = dynamic_cast<GopherFileSystem*>(file.get());
		fileStream = new ossInternalFileStream(file.release());
		if (gfs)
			fileStream->type = gfs->getUfsType();
#else
		{
			struct storageOptions *opts =
				static_cast<struct storageOptions *>(storageOptions);
			if (opts == NULL || opts->protocol == NULL)
				throw Datalake::Internal::Error(
					"datalakeCreateFileSystem: storageOptions or "
					"protocol is NULL");

			/* unique_ptr guards against a createHandle() throw (see above). */
			std::unique_ptr<FileSystem> file(
				BackendRegistry::instance().create(opts->protocol));
			file->createHandle(storageOptions);
			/*
			 * Each backend reports its own UFS type via getUfsId();
			 * cache it for datalakeGetFileInfo's path-prefix logic.
			 */
			int ufsId = file->getUfsId();
			fileStream = new ossInternalFileStream(file.release());
			fileStream->type = ufsId;
		}
#endif
	}
	catch (std::exception &e)
	{
		setLastError(e.what());
		elog(ERROR, "failed to datalakeCreateFileSystem: %s", e.what());
	}
	catch (...)
	{
		setLastError("internal error");
		elog(ERROR, "failed to datalakeCreateFileSystem: internal error");
	}

	/*
	 * Remember the options this stream was built from so datalakeCloneFileStream()
	 * can materialize an independent stream (own connection + file handle) later.
	 */
	if (fileStream != NULL)
		fileStream->storageOptions = storageOptions;

	return fileStream;
}

/*
 * datalakeCloneFileStream - build a fresh, independent stream that talks to the
 * same storage as `file`, using the storageOptions `file` was created from.
 *
 * The clone has its own FileSystem object (own connection + own single open-file
 * handle), so a reader holding the clone can seek/read concurrently with the
 * reader holding the original without clobbering each other's handle.  Hudi MOR
 * needs this: the base data-file reader and the delta-log reader would otherwise
 * share one handle and corrupt each other's file position.
 *
 * Returns NULL if `file` is NULL.  The caller owns the returned stream and must
 * release it with datalakeDestroyFileSystem().
 */
ossFileStream datalakeCloneFileStream(ossFileStream file)
{
	if (file == NULL)
		return NULL;

	return datalakeCreateFileSystem(file->storageOptions);
}

int datalakeOpenFile(ossFileStream file, const char *path, int flag)
{
	PARAMETER_ASSERT(file != NULL && strlen(path) > 0, EINVAL);
	int ret = 0;
	try
	{
		ret = file->getContext().openFile(path, flag);
	}
	catch (std::exception &e)
	{
		setLastError(e.what());
		elog(ERROR, "failed to open file \"%s\": %s", path, e.what());
	}
	catch (...)
	{
		setLastError("internal error");
		elog(ERROR, "failed to open file \"%s\": internal error", path);
	}
	return ret;
}

int datalakeWriteFile(ossFileStream file, void *buff, int64_t size)
{
	PARAMETER_ASSERT(file != NULL, EINVAL);
	int ret = 0;
	try
	{
		ret = file->getContext().write(buff, size);
	}
	catch (std::exception &e)
	{
		setLastError(e.what());
		elog(ERROR, "failed to write: %s", e.what());
	}
	catch (...)
	{
		setLastError("internal error");
		elog(ERROR, "failed to write: internal error");
	}
	return ret;
}

int datalakeReadFile(ossFileStream file, void *buff, int64_t size)
{
	PARAMETER_ASSERT(file != NULL, EINVAL);
	int ret = 0;
	try
	{
		ret = file->getContext().read(buff, size);
	}
	catch (std::exception &e)
	{
		setLastError(e.what());
		elog(ERROR, "failed to read: %s", e.what());
	}
	catch (...)
	{
		setLastError("internal error");
		elog(ERROR, "failed to read: internal error");
	}
	return ret;
}

int datalakeSeekFile(ossFileStream file, int64_t position)
{
	PARAMETER_ASSERT(file != NULL, EINVAL);
	int ret = 0;
	try
	{
		ret = file->getContext().seek(position);
	}
	catch (std::exception &e)
	{
		/*
		 * Return -1 (do NOT elog(ERROR)) so the caller decides how to react.
		 * seek has legitimate "expected failure" callers: e.g. Hudi's
		 * isBlockCorrupted() probes for EOF via logFileSeek(..., supressError)
		 * and needs -1 back, not a longjmp.  Every caller checks the -1 return
		 * (and re-raises via datalakeGetLastError() when it wants a hard
		 * error), matching the pre-abstraction gopherSeek() contract.
		 */
		setLastError(e.what());
		return -1;
	}
	catch (...)
	{
		setLastError("internal error");
		return -1;
	}
	return ret;
}

int datalakeCloseFile(ossFileStream file)
{
	if (file == NULL)
	{
		return 0;
	}
	int ret = file->getContext().closeFile();
	return ret;
}

datalakeFileInfo *datalakeListDir(ossFileStream file, const char *path, int *count, int recursive)
{
	PARAMETER_ASSERT(file != NULL, EINVAL);
	datalakeFileInfo* result = NULL;
	try
	{
		result = file->getContext().listInfo(path, *count, recursive);
	}
	catch (std::exception &e)
	{
		setLastError(e.what());
		elog(ERROR, "failed to list directory: %s", e.what());
	}
	catch (...)
	{
		setLastError("internal error");
		elog(ERROR, "failed to list directory: internal error");
	}

	return result;
}

void datalakeFreeFileInfo(datalakeFileInfo *list, int count)
{
	if (list == NULL)
		return;

	for (int i = 0; i < count; i++)
	{
		if (list[i].path != NULL)
			pfree(list[i].path);
	}
	pfree(list);
}

datalakeFileInfo* datalakeGetFileInfo(ossFileStream file, const char* path)
{
	PARAMETER_ASSERT(file != NULL && strlen(path) > 0, EINVAL);

	/* hdfs type: ensure path starts with / */
	std::string ufsPath;
	if (file->type == 9)
	{
		if (path[0] != '/')
		{
			std::string delimite = "/";
			ufsPath = delimite + path;
		}
		else
		{
			ufsPath = path;
		}
	}
	else
	{
		ufsPath = path;
	}

	datalakeFileInfo* result = NULL;
	try
	{
		result = file->getContext().getFileInfo(ufsPath.c_str());
	}
	catch (std::exception &e)
	{
		setLastError(e.what());
		elog(ERROR, "failed to exec datalakeGetFileInfo(): %s", e.what());
	}
	catch (...)
	{
		setLastError("internal error");
		elog(ERROR, "failed to exec datalakeGetFileInfo(): internal error");
	}
	return result;
}

int datalakeGetUfsId(ossFileStream file)
{
	if (file == NULL)
	{
		return 0;
	}
	int ret = 0;
	try
	{
		ret = file->getContext().getUfsId();
	}
	catch (std::exception &e)
	{
		setLastError(e.what());
		elog(ERROR, "failed to exec datalakeGetUfsId(): %s", e.what());
	}
	catch (...)
	{
		setLastError("internal error");
		elog(ERROR, "failed to exec datalakeGetUfsId(): internal error");
	}

	return ret;
}

int datalakeDestroyHandle(ossFileStream file)
{
	if (file == NULL)
	{
		return 0;
	}
	int ret = 0;
	try
	{
		ret = file->getContext().destroyHandle();
	}
	catch (std::exception &e)
	{
		setLastError(e.what());
		elog(ERROR, "failed to exec datalakeDestroyHandle(): %s", e.what());
	}
	catch (...)
	{
		setLastError("internal error");
		elog(ERROR, "failed to exec datalakeDestroyHandle(): internal error");
	}

	return ret;
}

void datalakeDestroyFileSystem(ossFileStream file)
{
	if (file == NULL)
		return;

	try
	{
		delete file;
		file = NULL;
	}
	catch (std::exception &e)
	{
		elog(ERROR, "failed to destroy ossFileStream: %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "failed to destory ossFileStream: internal error");
	}
}

const char *datalakeGetLastError(void)
{
#ifdef USE_GOPHER
	return gopherGetLastError();
#else
	return lastErrorBuf;
#endif
}

int datalakeDeleteFileByOptions(void *storageOpt, const char *path)
{
	ossFileStream fs = NULL;
	int rc = -1;

	if (storageOpt == NULL || path == NULL)
		return -1;

	try
	{
		fs = datalakeCreateFileSystem(storageOpt);
		rc = fs->getContext().deleteFile(path);   /* per-file, virtual dispatch */
	}
	catch (std::exception &e)
	{
		elog(WARNING, "datalakeDeleteFileByOptions: %s (path=%s)", e.what(), path);
		rc = -1;
	}
	catch (...)
	{
		elog(WARNING, "datalakeDeleteFileByOptions: unknown error (path=%s)", path);
		rc = -1;
	}

	if (fs)
		datalakeDestroyFileSystem(fs);
	return rc;
}

#ifdef __cplusplus
}
#endif
