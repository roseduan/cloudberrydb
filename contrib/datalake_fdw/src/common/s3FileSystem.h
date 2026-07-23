/*-------------------------------------------------------------------------
 *
 * s3FileSystem.h
 *    S3-based FileSystem implementation using AWS SDK C++.
 *
 *    Supports AWS S3, MinIO, Aliyun OSS (S3 mode), and other
 *    S3-compatible endpoints via path-style or virtual-hosted access.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/s3FileSystem.h
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_S3_FILESYSTEM_H
#define DATALAKE_S3_FILESYSTEM_H

#include "fileSystem.h"
#include <string>
#include <vector>
#include <memory>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompletedPart.h>

namespace Datalake {
namespace Internal {

class S3FileSystem : public FileSystem {

public:
	S3FileSystem();
	~S3FileSystem() override;

	int createHandle(void *storageOptions) override;
	int openFile(const char *path, int flag) override;
	int write(void *buff, int64_t size) override;
	int read(void *buff, int64_t size) override;
	int seek(int64_t position) override;
	int closeFile() override;
	int getUfsId() override;
	const char *getName() const override { return "S3"; }
	datalakeFileInfo* listInfo(const char *path, int &count,
							   int recursive = 1, bool iswrite = false) override;
	datalakeFileInfo* getFileInfo(const char *path) override;
	int destroyHandle() override;
	int deleteFile(const char *path) override;

private:
	/* AWS SDK lifecycle - once per process */
	static void ensureSDKInitialized();

	/* Multipart upload helpers */
	void initiateMultipartUpload();
	void flushWriteBuffer();
	void completeMultipartUpload();
	void abortMultipartUpload();

	/* Normalize S3 key (strip leading /) */
	std::string toS3Key(const std::string &path);

	/* AWS S3 client */
	std::shared_ptr<Aws::S3::S3Client> client_;

	/* Configuration */
	std::string bucket_;

	/* File state */
	std::string currentPath_;  /* normalized key without leading / */
	int64_t     offset_;
	int64_t     fileSize_;
	bool        isClosed_;
	bool        isWriteMode_;

	/* Write buffering for multipart upload */
	Aws::String uploadId_;
	std::vector<Aws::S3::Model::CompletedPart> completedParts_;
	std::vector<char> writeBuffer_;
	int partNumber_;

	static const int64_t MIN_PART_SIZE = 5 * 1024 * 1024;  /* 5MB S3 minimum */
};

} /* namespace Internal */
} /* namespace Datalake */

#endif /* DATALAKE_S3_FILESYSTEM_H */
