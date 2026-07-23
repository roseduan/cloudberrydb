/*-------------------------------------------------------------------------
 *
 * datalake_random_file.cpp
 *    RandomAccessFile implementation using datalake wrapper API.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/provider/common/datalake_random_file.cpp
 *-------------------------------------------------------------------------
 */
#include "datalake_random_file.h"

#include <parquet/internal/arrow/buffer.h>

extern "C" {
#include "postgres.h"
#include "access/tupdesc.h"
#include "utils/elog.h"
#include "src/common/fileSystemWrapper.h"
}

extern bool disableCacheFile;

#include "utils.h"


DatalakeRandomAccessFile::DatalakeRandomAccessFile(ossFileStream stream, std::string filePath)
	: stream_(stream), filePath_(filePath)
{
	fileSize_ = -1;
	offset_ = 0;
	isClosed_ = true;
}

DatalakeRandomAccessFile::~DatalakeRandomAccessFile()
{
	auto s = Close();
}

parquet_arrow::Status
DatalakeRandomAccessFile::Open()
{
	if (!isClosed_)
		return parquet_arrow::Status::OK();

	int flag = O_RDONLY;
	if (disableCacheFile)
		flag |= O_RDTHR;

	if (datalakeOpenFile(stream_, filePath_.c_str(), flag) != 0)
	{
		std::string message = "failed to open file \"" + filePath_ + "\"";
		return parquet_arrow::Status(parquet_arrow::StatusCode::IOError, message.c_str());
	}

	if (datalakeSeekFile(stream_, 0) == -1)
	{
		std::string message = "failed to seek file \"" + filePath_ + "\"";
		return parquet_arrow::Status(parquet_arrow::StatusCode::IOError, message.c_str());
	}

	isClosed_ = false;

	return ::parquet_arrow::Status::OK();
}

parquet_arrow::Result<int64_t>
DatalakeRandomAccessFile::GetSize()
{
	if (isClosed_)
	{
		RETURN_NOT_OK(Open());

		datalakeFileInfo* fileInfo = datalakeGetFileInfo(stream_, filePath_.c_str());
		if (fileInfo == NULL)
		{
			std::string message = "failed to get file \"" + filePath_ + "\" info";
			return parquet_arrow::Status(parquet_arrow::StatusCode::IOError, message.c_str());
		}
		fileSize_ = fileInfo->length;
		datalakeFreeFileInfo(fileInfo, 1);
	}

	return fileSize_;
}

parquet_arrow::Result<int64_t>
DatalakeRandomAccessFile::ReadAt(int64_t position, int64_t nbytes, void *out)
{
	if (datalakeSeekFile(stream_, position) == -1)
	{
		std::string message = "failed to seek file \"" + filePath_ + "\"";
		return parquet_arrow::Status(parquet_arrow::StatusCode::IOError, message.c_str());
	}

	offset_ = position;

	int64_t bytes = datalakeReadFile(stream_, out, nbytes);
	if (bytes == -1)
	{
		std::string message = "failed to read file \"" + filePath_ + "\"";
		return parquet_arrow::Status(parquet_arrow::StatusCode::IOError, message.c_str());
	}

	offset_ += bytes;
	return bytes;
}

parquet_arrow::Result<std::shared_ptr<parquet_arrow::Buffer>>
DatalakeRandomAccessFile::ReadAt(int64_t position, int64_t nbytes)
{
	PARQUET_ARROW_ASSIGN_OR_RAISE(auto buf, parquet_arrow::AllocateResizableBuffer(nbytes));
	if (nbytes > 0)
	{
		PARQUET_ARROW_ASSIGN_OR_RAISE(int64_t bytesRead, ReadAt(position, nbytes, buf->mutable_data()));
		RETURN_NOT_OK(buf->Resize(bytesRead));
	}

	return std::move(buf);
}

parquet_arrow::Result<int64_t>
DatalakeRandomAccessFile::Read(int64_t nbytes, void *out)
{
	int64_t bytes = datalakeReadFile(stream_, out, nbytes);
	if (bytes == -1)
	{
		std::string message = "failed to read file \"" + filePath_ + "\"";
		return parquet_arrow::Status(parquet_arrow::StatusCode::IOError, message.c_str());
	}

	offset_ += bytes;
	return bytes;
}

parquet_arrow::Result<std::shared_ptr<parquet_arrow::Buffer>>
DatalakeRandomAccessFile::Read(int64_t nbytes)
{
	PARQUET_ARROW_ASSIGN_OR_RAISE(auto buf, parquet_arrow::AllocateResizableBuffer(nbytes));
	if (nbytes > 0)
	{
		PARQUET_ARROW_ASSIGN_OR_RAISE(int64_t bytesRead, Read(nbytes, buf->mutable_data()));
		RETURN_NOT_OK(buf->Resize(bytesRead));
	}

	return std::move(buf);
}

parquet_arrow::Status
DatalakeRandomAccessFile::Seek(int64_t position)
{
	if (datalakeSeekFile(stream_, position) == -1)
	{
		std::string message = "failed to seek file \"" + filePath_ + "\"";
		return parquet_arrow::Status(parquet_arrow::StatusCode::IOError, message.c_str());
	}

	offset_ = position;
	return parquet_arrow::Status::OK();
}

parquet_arrow::Status
DatalakeRandomAccessFile::Close()
{
	if (isClosed_)
		return parquet_arrow::Status::OK();

	isClosed_ = true;
	datalakeCloseFile(stream_);
	return parquet_arrow::Status::OK();
}

parquet_arrow::Result<int64_t>
DatalakeRandomAccessFile::Tell() const
{
	return offset_;
}

bool
DatalakeRandomAccessFile::closed() const
{
	return isClosed_;
}
