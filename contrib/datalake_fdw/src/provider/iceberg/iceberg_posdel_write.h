#pragma once

#include "src/provider/parquet/write/parquetFileWriter.h"
#include "src/common/rewrLogical.h"
#include "src/common/readPolicy.h"
#include "src/provider/provider.h"
#include "src/dlproxy/datalake.h"
#include "src/provider/iceberg/iceberg_write.h"
#include "src/provider/iceberg/metadata_column.h"

extern "C" {
#include "src/provider/common/utils.h"
#include "src/provider/common/row_reader.h"
}

namespace Datalake {
namespace Internal {

class icebergPosDeleteWrite : public icebergWrite
{
public:
	static inline std::vector<MetadataField> POS_DELETE_SCHEMA{MetadataColumns::DELETE_FILE_PATH, MetadataColumns::DELETE_FILE_POS};
	virtual void createHandler(void *sstate);
	int64_t write(const void *values, int64_t length) override;
private:
	void appendFileMeta();
	/*
	 * Data file (DELETE_FILE_PATH, column 0) referenced by the currently-open
	 * delete parquet.  Used to roll the parquet over whenever the referenced data
	 * file changes, so every position-delete file references exactly one data file
	 * (file-scoped deletes).
	 */
	std::string current_delete_target;
};

}
}
