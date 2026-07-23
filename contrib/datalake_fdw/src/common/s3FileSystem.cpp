/*-------------------------------------------------------------------------
 *
 * s3FileSystem.cpp
 *    S3-based FileSystem implementation using AWS SDK C++.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/s3FileSystem.cpp
 *-------------------------------------------------------------------------
 */
#include "s3FileSystem.h"
#include "backendRegistry.h"

#include <sstream>
#include <cstring>
#include <unistd.h>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/crt/Api.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "cdb/cdbvars.h"
#include "src/datalake_def.h"
}

/* ================================================================
 * AWS SDK lifecycle
 *
 * PostgreSQL uses fork() to create backend processes. The AWS CRT
 * layer registers global state via __attribute__((constructor)) when
 * the .so is dlopen'd. After fork, this state points into the parent's
 * address space and becomes invalid. Calling Aws::InitAPI() alone is
 * not enough because aws-c-common has an internal "already initialized"
 * flag that persists across fork.
 *
 * Fix: reset the CRT's internal init flag via aws_common_library_clean_up()
 * before calling InitAPI, so the CRT re-initializes its allocator in the
 * child process.
 * ================================================================
 */

static pid_t sdkInitPid = 0;

/*
 * PostgreSQL forks backend processes from postmaster. The CRT's
 * aws-c-common library has a process-global init flag set during
 * dlopen of this .so. After fork, InitAPI's internal call to
 * aws_common_library_init() is a no-op (already initialized),
 * but the CRT ApiHandle and its allocator are stale.
 *
 * Fix: create a per-process CRT ApiHandle BEFORE InitAPI. The ApiHandle
 * constructor calls aws_crt_init() which bootstraps the allocator
 * independently of the aws-c-common init flag.
 */
/*
 * Per-process CRT handle.  A raw pointer (not std::unique_ptr) so that the
 * C++ runtime does NOT run ~ApiHandle() during libc exit() handlers: by that
 * point the AWS SDK/CRT globals (logger, allocator, the default
 * ClientBootstrap/EventLoopGroup) have been torn down in an order that makes
 * ~ApiHandle() / ~ClientBootstrap() dereference freed state -> SIGSEGV in
 * every backend that touched the S3 backend.  Instead we tear the SDK down
 * ourselves at proc_exit time (datalakeS3ShutdownSDK), while everything is
 * still alive.
 */
static Aws::Crt::ApiHandle *s_crtHandle = NULL;

/* SDKOptions kept for the matching ShutdownAPI at proc_exit. */
static Aws::SDKOptions s_sdkOptions;

/*
 * proc_exit callback: shut the AWS SDK down cleanly BEFORE libc runs the
 * C++ static destructors.  Order mirrors init in reverse -- ShutdownAPI
 * (high-level SDK) first, then ~ApiHandle() (CRT layer) -- so the CRT
 * allocator/logger are still valid while each layer releases its globals.
 * Only the process that called InitAPI performs teardown.
 */
static void
datalakeS3ShutdownSDK(int code, Datum arg)
{
	if (sdkInitPid != getpid() || s_crtHandle == NULL)
		return;
	Aws::ShutdownAPI(s_sdkOptions);
	delete s_crtHandle;
	s_crtHandle = NULL;
	sdkInitPid = 0;
}

void
Datalake::Internal::S3FileSystem::ensureSDKInitialized()
{
	pid_t myPid = getpid();
	if (sdkInitPid == myPid)
		return;

	/*
	 * Bootstrap CRT with a fresh ApiHandle for this process.  Any handle
	 * inherited across fork points at the parent's now-invalid CRT state,
	 * so we abandon (leak) it rather than destroy it -- running
	 * ~ApiHandle() on stale state is exactly what crashes at exit.  The
	 * new handle owns this process's CRT allocator for the process
	 * lifetime and is never freed (see note at s_crtHandle).
	 */
	s_crtHandle = new Aws::Crt::ApiHandle();

	s_sdkOptions.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Warn;
	Aws::InitAPI(s_sdkOptions);
	sdkInitPid = myPid;

	/*
	 * Register the controlled teardown so the SDK is shut down at proc_exit
	 * instead of by the C++ runtime at libc exit() (see datalakeS3ShutdownSDK).
	 */
	on_proc_exit(datalakeS3ShutdownSDK, (Datum) 0);

	elog(LOG, "AWS SDK C++ initialized in pid %d", (int)myPid);
}

/* ================================================================
 * S3FileSystem implementation
 * ================================================================
 */
namespace Datalake {
namespace Internal {

S3FileSystem::S3FileSystem()
{
	offset_ = 0;
	fileSize_ = -1;
	isClosed_ = true;
	isWriteMode_ = false;
	partNumber_ = 0;
}

S3FileSystem::~S3FileSystem()
{
	if (!isClosed_)
	{
		try
		{
			closeFile();
		}
		catch (...)
		{
			/* Abort orphaned multipart upload on failure during cleanup */
			abortMultipartUpload();
		}
	}
	else
	{
		/* File was closed but upload may have been left by an earlier error */
		abortMultipartUpload();
	}
}

/* ----------------------------------------------------------------
 * toS3Key - normalize path for S3 (strip leading /)
 * ----------------------------------------------------------------
 */
std::string S3FileSystem::toS3Key(const std::string &path)
{
	if (!path.empty() && path[0] == '/')
		return path.substr(1);
	return path;
}

/* ----------------------------------------------------------------
 * createHandle - parse storageOptions, create S3Client
 * ----------------------------------------------------------------
 */
int S3FileSystem::createHandle(void *options)
{
	ensureSDKInitialized();

	storageOptions *opts = (storageOptions *)options;

	bucket_ = opts->bucket ? opts->bucket : "";
	std::string accessKey = opts->accessKey ? opts->accessKey : "";
	std::string secretKey = opts->secretKey ? opts->secretKey : "";
	std::string region = opts->region ? opts->region : "us-east-1";
	bool useVirtualHost = opts->useVirtualHost;
	bool useHttps = opts->useHttps;

	/* Build endpoint */
	std::string endpoint;
	if (opts->host)
	{
		std::string scheme = useHttps ? "https" : "http";
		endpoint = scheme + "://" + std::string(opts->host);
		if (opts->port > 0)
			endpoint += ":" + std::to_string(opts->port);
	}

	/* Configure AWS S3 client - use S3ClientConfiguration directly */
	Aws::S3::S3ClientConfiguration s3Config;
	s3Config.region = Aws::String(region.c_str());
	s3Config.connectTimeoutMs = 30000;
	s3Config.requestTimeoutMs = 300000;
	s3Config.maxConnections = 25;
	s3Config.retryStrategy = Aws::Client::InitRetryStrategy(3);
	s3Config.useVirtualAddressing = useVirtualHost;
	/*
	 * Skip request body signing for performance. Safe because:
	 * - HTTPS connections verify integrity via TLS
	 * - HTTP connections (MinIO dev) accept unsigned payloads
	 * If a strict S3-compatible endpoint rejects unsigned payloads over
	 * HTTP, change this to RequestDependent.
	 */
	s3Config.payloadSigningPolicy =
		Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;

	if (!endpoint.empty())
		s3Config.endpointOverride = Aws::String(endpoint.c_str());

	if (!useHttps)
		s3Config.scheme = Aws::Http::Scheme::HTTP;
	else
		s3Config.scheme = Aws::Http::Scheme::HTTPS;

	s3Config.verifySSL = useHttps;

	/* Create credentials */
	Aws::Auth::AWSCredentials credentials(
		Aws::String(accessKey.c_str()),
		Aws::String(secretKey.c_str()));

	/* Create S3 client */
	client_ = std::make_shared<Aws::S3::S3Client>(credentials, nullptr, s3Config);

	elog(DEBUG1, "S3FileSystem: endpoint=%s bucket=%s region=%s virtualHost=%d https=%d",
		 endpoint.c_str(), bucket_.c_str(), region.c_str(),
		 useVirtualHost, useHttps);

	return 0;
}

/* ================================================================
 * File operations
 * ================================================================
 */

int S3FileSystem::openFile(const char *path, int flag)
{
	if (path == NULL)
		return -1;

	isClosed_ = false;
	currentPath_ = toS3Key(path);
	offset_ = 0;
	fileSize_ = -1;

	isWriteMode_ = (flag & O_WRONLY) || (flag & O_CREAT);

	/* Gopher-specific cache flags are not applicable to S3 direct access */
	if (flag & (O_FNCACHE | O_RDONCE | O_RDTHR))
		elog(DEBUG1, "S3FileSystem: ignoring Gopher cache flags 0x%x for \"%s\"",
			 flag & (O_FNCACHE | O_RDONCE | O_RDTHR), currentPath_.c_str());

	if (isWriteMode_)
	{
		writeBuffer_.clear();
		completedParts_.clear();
		partNumber_ = 0;
		uploadId_.clear();
		initiateMultipartUpload();
	}

	elog(DEBUG5, "S3FileSystem: openFile key=%s flag=%d write=%d",
		 currentPath_.c_str(), flag, isWriteMode_);

	return 0;
}

int S3FileSystem::read(void *buff, int64_t size)
{
	if (size == 0)
		return 0;

	Aws::S3::Model::GetObjectRequest request;
	request.SetBucket(Aws::String(bucket_.c_str()));
	request.SetKey(Aws::String(currentPath_.c_str()));

	/* Range header */
	std::ostringstream rangeStr;
	rangeStr << "bytes=" << offset_ << "-" << (offset_ + size - 1);
	request.SetRange(Aws::String(rangeStr.str().c_str()));

	auto outcome = client_->GetObject(request);

	if (!outcome.IsSuccess())
	{
		auto &error = outcome.GetError();

		/*
		 * HTTP 416 Range Not Satisfiable means we're reading past EOF.
		 * Return 0 (EOF) instead of throwing, matching Gopher behavior.
		 */
		if (error.GetResponseCode() == Aws::Http::HttpResponseCode::REQUESTED_RANGE_NOT_SATISFIABLE)
			return 0;

		throw Error("S3 GetObject failed for \"%s\": %s - %s",
					currentPath_.c_str(),
					error.GetExceptionName().c_str(),
					error.GetMessage().c_str());
	}

	auto &body = outcome.GetResult().GetBody();
	body.read(static_cast<char*>(buff), size);
	int64_t bytesRead = body.gcount();

	offset_ += bytesRead;
	return (int)bytesRead;
}

int S3FileSystem::write(void *buff, int64_t size)
{
	if (size == 0)
		return 0;

	writeBuffer_.insert(writeBuffer_.end(),
						static_cast<char*>(buff),
						static_cast<char*>(buff) + size);

	/* Flush when buffer exceeds minimum part size */
	while ((int64_t)writeBuffer_.size() >= MIN_PART_SIZE)
		flushWriteBuffer();

	return (int)size;
}

int S3FileSystem::seek(int64_t position)
{
	offset_ = position;
	return 0;
}

int S3FileSystem::closeFile()
{
	if (isClosed_)
		return 0;

	isClosed_ = true;

	if (isWriteMode_)
	{
		try
		{
			/* Flush remaining buffer */
			if (!writeBuffer_.empty())
				flushWriteBuffer();

			if (!uploadId_.empty())
				completeMultipartUpload();
		}
		catch (...)
		{
			/* Abort the orphaned multipart upload before re-throwing */
			abortMultipartUpload();
			currentPath_.clear();
			offset_ = 0;
			fileSize_ = -1;
			isWriteMode_ = false;
			throw;
		}
	}

	currentPath_.clear();
	offset_ = 0;
	fileSize_ = -1;
	isWriteMode_ = false;

	return 0;
}

int S3FileSystem::getUfsId()
{
	return 0;
}

int S3FileSystem::destroyHandle()
{
	client_.reset();
	return 0;
}

int S3FileSystem::deleteFile(const char *path)
{
	if (!client_)
		return -1;

	std::string key = toS3Key(path);

	Aws::S3::Model::DeleteObjectRequest request;
	request.SetBucket(Aws::String(bucket_.c_str()));
	request.SetKey(Aws::String(key.c_str()));

	/* S3 DeleteObject is idempotent: deleting a missing key succeeds. */
	auto outcome = client_->DeleteObject(request);
	return outcome.IsSuccess() ? 0 : -1;
}

/* ----------------------------------------------------------------
 * getFileInfo - S3 HeadObject
 * ----------------------------------------------------------------
 */
datalakeFileInfo* S3FileSystem::getFileInfo(const char *path)
{
	std::string key = toS3Key(path);

	Aws::S3::Model::HeadObjectRequest request;
	request.SetBucket(Aws::String(bucket_.c_str()));
	request.SetKey(Aws::String(key.c_str()));

	auto outcome = client_->HeadObject(request);

	if (!outcome.IsSuccess())
	{
		auto &error = outcome.GetError();
		throw Error("S3 HeadObject failed for \"%s\": %s - %s",
					path,
					error.GetExceptionName().c_str(),
					error.GetMessage().c_str());
	}

	auto &result = outcome.GetResult();

	datalakeFileInfo *info = (datalakeFileInfo*)palloc0(sizeof(datalakeFileInfo));
	info->path = pstrdup(path);
	info->length = result.GetContentLength();
	info->isDirectory = false;

	return info;
}

/* ----------------------------------------------------------------
 * listInfo - S3 ListObjectsV2
 * ----------------------------------------------------------------
 */
datalakeFileInfo* S3FileSystem::listInfo(const char *path, int &count,
										  int recursive, bool iswrite)
{
	std::vector<datalakeFileInfo> results;
	std::string prefix = path ? toS3Key(path) : "";

	/* Ensure prefix ends with / for directory listing */
	if (!prefix.empty() && prefix.back() != '/')
		prefix += "/";

	Aws::String continuationToken;
	bool hasMore = true;

	while (hasMore)
	{
		Aws::S3::Model::ListObjectsV2Request request;
		request.SetBucket(Aws::String(bucket_.c_str()));
		request.SetPrefix(Aws::String(prefix.c_str()));

		if (!recursive)
			request.SetDelimiter("/");

		if (!continuationToken.empty())
			request.SetContinuationToken(continuationToken);

		auto outcome = client_->ListObjectsV2(request);

		if (!outcome.IsSuccess())
		{
			auto &error = outcome.GetError();
			throw Error("S3 ListObjectsV2 failed for \"%s\": %s - %s",
						path,
						error.GetExceptionName().c_str(),
						error.GetMessage().c_str());
		}

		auto &result = outcome.GetResult();

		for (auto &object : result.GetContents())
		{
			datalakeFileInfo fi;
			/*
			 * S3 keys have no leading /, but Gopher paths do.
			 * Prepend / for consistency with downstream consumers
			 * (fragment serialization, hidden-file filtering, etc.).
			 */
			std::string keyWithSlash = "/" + std::string(object.GetKey().c_str());
			fi.path = pstrdup(keyWithSlash.c_str());
			fi.length = object.GetSize();
			fi.isDirectory = false;
			results.push_back(fi);
		}

		/* Non-recursive: subdirectories are returned as CommonPrefixes */
		if (!recursive)
		{
			for (auto &pfx : result.GetCommonPrefixes())
			{
				datalakeFileInfo fi;
				std::string dirKey = "/" + std::string(pfx.GetPrefix().c_str());
				fi.path = pstrdup(dirKey.c_str());
				fi.length = 0;
				fi.isDirectory = true;
				results.push_back(fi);
			}
		}

		if (result.GetIsTruncated())
			continuationToken = result.GetNextContinuationToken();
		else
			hasMore = false;
	}

	count = results.size();

	if (count == 0)
	{
		elog(LOG, "S3FileSystem: empty listing for prefix \"%s\"", path ? path : "");
		return NULL;
	}

	datalakeFileInfo *list = (datalakeFileInfo*)palloc0(sizeof(datalakeFileInfo) * count);
	for (int i = 0; i < count; i++)
		list[i] = results[i];

	return list;
}

/* ================================================================
 * Multipart upload
 * ================================================================
 */

void S3FileSystem::initiateMultipartUpload()
{
	Aws::S3::Model::CreateMultipartUploadRequest request;
	request.SetBucket(Aws::String(bucket_.c_str()));
	request.SetKey(Aws::String(currentPath_.c_str()));

	auto outcome = client_->CreateMultipartUpload(request);

	if (!outcome.IsSuccess())
	{
		auto &error = outcome.GetError();
		throw Error("S3 CreateMultipartUpload failed for \"%s\": %s - %s",
					currentPath_.c_str(),
					error.GetExceptionName().c_str(),
					error.GetMessage().c_str());
	}

	uploadId_ = outcome.GetResult().GetUploadId();

	elog(DEBUG1, "S3FileSystem: initiated multipart upload, id=%s",
		 std::string(uploadId_.c_str()).c_str());
}

void S3FileSystem::flushWriteBuffer()
{
	if (writeBuffer_.empty())
		return;

	partNumber_++;

	/* S3 enforces a hard limit of 10,000 parts per multipart upload */
	if (partNumber_ > 10000)
		throw Error("S3 multipart upload for \"%s\" exceeded 10000 part limit "
					"(wrote > %lld bytes at 5MB per part)",
					currentPath_.c_str(), (long long)(10000LL * MIN_PART_SIZE));

	auto stream = Aws::MakeShared<Aws::StringStream>("UploadPart");
	stream->write(writeBuffer_.data(), writeBuffer_.size());

	Aws::S3::Model::UploadPartRequest request;
	request.SetBucket(Aws::String(bucket_.c_str()));
	request.SetKey(Aws::String(currentPath_.c_str()));
	request.SetUploadId(uploadId_);
	request.SetPartNumber(partNumber_);
	request.SetContentLength(writeBuffer_.size());
	request.SetBody(stream);

	auto outcome = client_->UploadPart(request);

	if (!outcome.IsSuccess())
	{
		auto &error = outcome.GetError();
		throw Error("S3 UploadPart %d failed for \"%s\": %s - %s",
					partNumber_, currentPath_.c_str(),
					error.GetExceptionName().c_str(),
					error.GetMessage().c_str());
	}

	Aws::S3::Model::CompletedPart part;
	part.SetPartNumber(partNumber_);
	part.SetETag(outcome.GetResult().GetETag());
	completedParts_.push_back(part);

	elog(DEBUG1, "S3FileSystem: uploaded part %d, size=%zu",
		 partNumber_, writeBuffer_.size());

	writeBuffer_.clear();
}

void S3FileSystem::completeMultipartUpload()
{
	Aws::S3::Model::CompletedMultipartUpload completedUpload;
	completedUpload.SetParts(
		Aws::Vector<Aws::S3::Model::CompletedPart>(
			completedParts_.begin(), completedParts_.end()));

	Aws::S3::Model::CompleteMultipartUploadRequest request;
	request.SetBucket(Aws::String(bucket_.c_str()));
	request.SetKey(Aws::String(currentPath_.c_str()));
	request.SetUploadId(uploadId_);
	request.SetMultipartUpload(completedUpload);

	auto outcome = client_->CompleteMultipartUpload(request);

	if (!outcome.IsSuccess())
	{
		auto &error = outcome.GetError();
		throw Error("S3 CompleteMultipartUpload failed for \"%s\": %s - %s",
					currentPath_.c_str(),
					error.GetExceptionName().c_str(),
					error.GetMessage().c_str());
	}

	elog(DEBUG1, "S3FileSystem: completed multipart upload for %s (%zu parts)",
		 currentPath_.c_str(), completedParts_.size());

	uploadId_.clear();
	completedParts_.clear();
	partNumber_ = 0;
}

void S3FileSystem::abortMultipartUpload()
{
	if (uploadId_.empty() || !client_)
		return;

	try
	{
		Aws::S3::Model::AbortMultipartUploadRequest request;
		request.SetBucket(Aws::String(bucket_.c_str()));
		request.SetKey(Aws::String(currentPath_.c_str()));
		request.SetUploadId(uploadId_);

		auto outcome = client_->AbortMultipartUpload(request);

		if (outcome.IsSuccess())
			elog(LOG, "S3FileSystem: aborted orphaned multipart upload %s for %s",
				 std::string(uploadId_.c_str()).c_str(), currentPath_.c_str());
		else
			elog(WARNING, "S3FileSystem: failed to abort multipart upload %s for %s",
				 std::string(uploadId_.c_str()).c_str(), currentPath_.c_str());
	}
	catch (...)
	{
		/* Best-effort cleanup, swallow exceptions */
	}

	uploadId_.clear();
	completedParts_.clear();
	partNumber_ = 0;
}

} /* namespace Internal */
} /* namespace Datalake */

/*
 * ---- Backend registration ----
 *
 * Registers S3FileSystem under every protocol name that today maps to
 * an S3-compatible object store. The OSS family (ali, cos, qs, s3b,
 * huawei, ks3) all use the same S3 client code path; per-vendor
 * subclasses are a future refinement if any vendor's signing or
 * endpoint rules diverge.
 *
 * File-scope registrar struct (not DATALAKE_REGISTER_BACKEND) because
 * we register the same factory under multiple names.
 */
using ::Datalake::Internal::BackendRegistry;
using ::Datalake::Internal::FileSystem;
using ::Datalake::Internal::S3FileSystem;

namespace {
struct S3_Registrar {
	S3_Registrar() {
		auto factory = []() -> FileSystem* {
			return new S3FileSystem();
		};
		const char *names[] = {
			"s3", "ali", "cos", "qs", "s3b", "huawei", "ks3"
		};
		auto &reg = BackendRegistry::instance();
		for (const char *n : names)
			reg.registerBackend(n, factory);
	}
};
static S3_Registrar s3_registrar_instance;
} /* anonymous namespace */
