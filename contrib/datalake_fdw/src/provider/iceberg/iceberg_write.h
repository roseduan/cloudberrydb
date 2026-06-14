#pragma once

#include <src/provider/parquet/write/parquetFileWriter.h>
#include "src/common/rewrLogical.h"
#include "src/common/readPolicy.h"
#include "src/provider/provider.h"
#include "src/dlproxy/datalake.h"

extern "C" {
	#include "src/provider/common/utils.h"
	#include "src/provider/common/row_reader.h"
	#include "src/am_iceberg/include/pg_iceberg_pending_delete.h"
}

namespace Datalake {
namespace Internal {

class icebergWrite : public Provider
{
public:
	virtual void createHandler(void *sstate);
	virtual int64_t write(const void *values, int64_t length);
	virtual void destroyHandler();

protected:
	std::string prefix;
	std::string file_name;
	std::string append_file_prefix;
	ossFileStream fileStream;
	writeOption option;
	std::unique_ptr<parquetFileWriter> file_writer;
	dataLakeFdwScanState *ss;
	int sliceIdx = 0;
	List *fileMetas;
	int64_t tuple_num;

protected:
	void initWriteOption();
	void buildFilePrefix(dataLakeOptions *opt);
	void generateNewFileName();

private:
	virtual void appendFileMeta();
};

}
}
