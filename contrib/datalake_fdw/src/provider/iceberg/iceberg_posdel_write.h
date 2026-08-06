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
	/*
	 * Partition tuple (a PG List* passed as void*: String nodes / NULL cells,
	 * spec order) of the data file the next delete record targets.  Set by the
	 * executor before each write(); NIL for an unpartitioned table.  The delete
	 * file inherits it so the agent commits it under the same partition as the
	 * data file it references.  File-scoped deletes already roll one delete file
	 * per referenced data file (hence per partition), so no per-partition fanout
	 * is needed -- only the partition tuple has to be stamped.
	 */
	void setDeletePartition(void *partitionValues) override;
private:
	void appendFileMeta();
	/*
	 * Data file (DELETE_FILE_PATH, column 0) referenced by the currently-open
	 * delete parquet.  Used to roll the parquet over whenever the referenced data
	 * file changes, so every position-delete file references exactly one data file
	 * (file-scoped deletes).
	 */
	std::string current_delete_target;
	/* Partition tuple supplied by the executor before each write() (spec order);
	 * empty for an unpartitioned table. */
	std::vector<IcebergPartitionValue> pendingPartition;
	/* Partition tuple of the currently-open delete file; stamped in
	 * appendFileMeta().  Captured from pendingPartition when the file is opened. */
	std::vector<IcebergPartitionValue> currentPartition;
};

}
}
