/*-------------------------------------------------------------------------
 *
 * datalake_random_file.h
 *    RandomAccessFile implementation using the datalake wrapper API.
 *
 *    Replaces GopherRandomAccessFile which called Gopher API directly.
 *    This version goes through datalakeOpenFile/datalakeReadFile/etc.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/provider/common/datalake_random_file.h
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_RANDOM_FILE_H
#define DATALAKE_RANDOM_FILE_H

#include <parquet/internal/arrow/io/interfaces.h>
#include <parquet/internal/arrow/io/file.h>
#include <string>

struct ossInternalFileStream;
typedef struct ossInternalFileStream *ossFileStream;

class DatalakeRandomAccessFile : public parquet_arrow::io::RandomAccessFile
{
public:
	DatalakeRandomAccessFile(ossFileStream stream, std::string filePath);
	~DatalakeRandomAccessFile();

	parquet_arrow::Status Open();
	parquet_arrow::Result<int64_t> GetSize();
	parquet_arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void *out);
	parquet_arrow::Result<std::shared_ptr<parquet_arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes);
	parquet_arrow::Result<int64_t> Read(int64_t nbytes, void *out);
	parquet_arrow::Result<std::shared_ptr<parquet_arrow::Buffer>> Read(int64_t nbytes);
	parquet_arrow::Result<int64_t> Tell() const;
	parquet_arrow::Status Seek(int64_t position);
	parquet_arrow::Status Close();
	bool closed() const;

private:
	ossFileStream stream_;
	std::string filePath_;
	int64_t fileSize_;
	int64_t offset_;
	bool isClosed_;
};

#endif /* DATALAKE_RANDOM_FILE_H */
