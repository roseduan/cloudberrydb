#ifndef DATALAKE_PARQUETFILEWRITER_H
#define DATALAKE_PARQUETFILEWRITER_H

#include <memory>
#include <string>
#include <iostream>
#include <vector>
#include <parquet/internal/arrow/io/interfaces.h>
#include <parquet/file_writer.h>
#include "parquetOutputStream.h"
#include "parquetColumnStats.h"
#include "src/provider/provider.h"
#include "src/common/logicalType.h"
#include "src/common/rewrLogical.h"

extern "C" {
#include "src/common/util.h"
#include "src/datalake_def.h"
}


using Datalake::Internal::ParquetLogicalType;
using Datalake::Internal::writeOption;

class vectorBatch {
public:
    vectorBatch(){

    }
    virtual ~vectorBatch(){

    }
    void* buffer;
    bool* notNull;
    int64_t* length;
    int64_t* precision;
    int64_t* scale;
    int num;
};

class StringVectorBatch : public vectorBatch {
public:
    StringVectorBatch(){

    }
    virtual ~StringVectorBatch(){

    }
    char** buffer;
    bool* notNull;
    int64_t* length;
    int64_t* precision;
    int64_t* scale;
    int num;
};

template<typename T>
class columnBatch : public vectorBatch{
public:
    columnBatch() {

    }
    ~columnBatch()
    {

    }

    T* buffer;
    bool* notNull;
    int64_t* length;
    int64_t* precision;
    int64_t* scale;
    int num;
};



class parquetFileWriter : public ParquetLogicalType
{
public:
    parquetFileWriter();

    bool createParquetWriter(ossFileStream ossFile, const std::string &fileName);

    std::shared_ptr<parquet::schema::GroupNode> setupSchema();

    void writeProperties();

    void resetParquetWriter();

    void closeParquetWriter();

    int64_t write(const void* buf, size_t length);

    void init(void *sstate, writeOption option);

    void init(TupleDesc tupdesc, std::shared_ptr<parquet::schema::GroupNode> schema, writeOption option);

    void destroy();

    bool isOpen();

    int64_t getWrittenBytes();

    /*
     * Wire-format v1 column statistics of the file this writer last closed,
     * or empty if none could be collected (issue #442).  Valid from
     * closeParquetWriter() until the next createParquetWriter().
     */
    const std::string& getColumnStatsJson() const { return columnStatsJson; }
private:

    void createColumnBatch();

    void resizeDataBuff(int count, std::vector<char>& buffer, int64_t datalen, int offset);

    void reDistributedDataBuffer(int count, char* oldBufferAddress, char* newBufferAddress);

    void writeToField(int index, const void* data);

    void writeToBatch(int rows);

    bool columnBelongStringType(int attColumn);

private:
    std::string name;
    std::shared_ptr<parquet::schema::GroupNode> schema;
    std::shared_ptr<parquet::WriterProperties> props;
    std::shared_ptr<parquet::ParquetFileWriter> file_writer;
    std::shared_ptr<gopherWriteFileSystem> out_file;
    std::vector<vectorBatch*> batchField;
    std::vector<char> dataBuffer;
    int64_t dataBufferOffset;
    int64_t batchNum;
    parquet::RowGroupWriter* rg_writer;
    int64_t estimated_bytes;

    TupleDesc tupdesc;
    AttrNumber ncolumns;
    writeOption option;
    parquet::ByteArray* byteArray;
    parquet::FixedLenByteArray* fixByteArray;
    parquet::Int96* int96Array;
    int64_t* int64Array;
    int16_t* definition_level;
    std::string columnStatsJson;

    bool openState;
};

#endif