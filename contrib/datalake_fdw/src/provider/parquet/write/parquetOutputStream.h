#ifndef DATALAKE_PARQUETOUTPUTSTREAM_H
#define DATALAKE_PARQUETOUTPUTSTREAM_H

#include <parquet/internal/arrow/io/interfaces.h>
#include <parquet/internal/arrow/io/file.h>
#include "src/common/fileSystemWrapper.h"

class gopherWriteFileSystem : public ::parquet_arrow::io::OutputStream
{
public:
    gopherWriteFileSystem(ossFileStream stream) {
        pos_ = 0;
        closed_ = true;
        filePath_ = "";
        stream_ = stream;
    }

    ~gopherWriteFileSystem();

    bool OpenFile(const std::string& path, bool append = false);

    ::parquet_arrow::Status Close() override;

    bool closed() const override {
        return closed_;
    }

    ::parquet_arrow::Result<int64_t> Tell() const override {
        return pos_;
    }

    ::parquet_arrow::Status Write(const void* data, int64_t nbytes) override;

    int file_descriptor() const {
        // used gopher no file descriptor
        return 0;
    }

    int64_t getBytesWritten() {
        return bytes_written;
    }

private:
    ossFileStream stream_;
    std::string filePath_;
    int64_t pos_;
    bool closed_;
    int64_t bytes_written;
};

#endif

