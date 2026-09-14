#ifndef DATALAKE_PARQUETFILEREADER_H
#define DATALAKE_PARQUETFILEREADER_H

#include "parquetInputStream.h"
#include "src/common/logicalType.h"
#include "src/common/rewrLogical.h"

#include <memory>
#include <string>
#include <iostream>
#include <vector>
#include <parquet/api/reader.h>

namespace Datalake {
namespace Internal {

class parquetFileReader
{
public:
    parquetFileReader();

    bool createParquetReader(ossFileStream ossFile, std::string fileName, readOption options);

    bool createInternalReader(ossFileStream ossFile, std::string fileName);

    void resetParquetReader();

    /* set rowGroup read num */
    void setRowGroup(int row_group_num);

    /* get rowGroup offset in file */
    int64_t rowGroupOffset(int row_group_num);

    /* create parquet scanner for batchRead */
    void createScanners();

    /* reset scanner for next rowgroup to read */
    void resetScanners();

    Datum read(Oid typeOid, int column_index, bool &isNull, int &state);

    void closeParquetReader();

    int getRowGroupNums() {
        return num_row_groups;
    }

    fileState getState()
    {
        return state;
    }

    int getFileColumns()
    {
        return num_columns;
    }

    const std::shared_ptr<::parquet::FileMetaData> &getFileMetadata()
    {
        return file_metadata;
    }

    /* issue #424: expose the per-file gopher stream so the vectorized reader
     * can build a parquet::arrow::FileReader on it without moving
     * parquet_reader (keeps resetParquetReader's Close path valid). */
    const std::shared_ptr<gopherReadFileSystem> &getFileStream()
    {
        return fileStream;
    }

private:
    readOption options;
    int num_row_groups;
    int num_columns;
    std::shared_ptr<::parquet::FileMetaData> file_metadata;
    std::unique_ptr<::parquet::ParquetFileReader> parquet_reader;
    std::shared_ptr<::parquet::RowGroupReader> row_group_reader;
    std::vector<std::shared_ptr<::parquet::Scanner>> scanners;
    std::vector<const ::parquet::ColumnDescriptor*> columnDescriptors;
    std::string name;
	std::shared_ptr<gopherReadFileSystem> fileStream;

    int row_count;
    int row_offset;
    fileState state;
};

}
}

#endif