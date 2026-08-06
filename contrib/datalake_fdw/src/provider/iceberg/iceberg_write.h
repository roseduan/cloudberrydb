#pragma once

#include <map>
#include <vector>
#include <string>

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

/* One Iceberg partition column resolved from the PARTITION BY option. */
struct IcebergPartitionColumn
{
	AttrNumber   attno;			/* 1-based attribute number in the tuple desc */
	Oid          typid;			/* column type OID */
	Oid          outputFunc;	/* type output function OID (identity value text) */
	std::string  name;			/* column name (for the data/<col>=<val>/ path) */
};

/* A single partition value: SQL NULL, or its identity text form. */
struct IcebergPartitionValue
{
	bool         isNull;
	std::string  value;			/* type output text; empty when isNull */
};

/*
 * A per-partition open data file in the fanout writer.  Keyed in partWriters
 * by the relative "data/<col>=<val>/..." path so all rows of one partition
 * land in the same file(s).
 */
struct IcebergPartitionFile
{
	std::unique_ptr<parquetFileWriter> writer;
	/*
	 * Each partition needs its own gopher fileStream: a FileSystem handle has
	 * a single active file, so concurrently-open partition files cannot share
	 * one stream.  Owned here; destroyed in destroyHandler().
	 */
	ossFileStream fileStream = NULL;
	std::string  fileName;		/* current open bucket-relative file key */
	int64_t      tupleNum = 0;
	std::string  relativePath;	/* "col=val[/col2=val2...]" */
	std::vector<IcebergPartitionValue> values;	/* partition tuple, spec order */
};

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

	/*
	 * Partition state.  partCols is empty for unpartitioned tables, in which
	 * case the single-stream file_writer path above is used unchanged.
	 */
	std::vector<IcebergPartitionColumn> partCols;
	std::map<std::string, IcebergPartitionFile> partWriters;

	/*
	 * Long-lived context captured in createHandler().  Per-partition writers
	 * (and their column batch buffers) must be allocated here, NOT in the
	 * per-row context that fdwfunction_insertModify resets on every tuple.
	 */
	MemoryContext handlerContext = NULL;

protected:
	void initWriteOption();
	void buildFilePrefix(dataLakeOptions *opt);
	void generateNewFileName();

	/* Partition (fanout) helpers. */
	void parsePartitionColumns();
	void computePartition(TupleTableSlot *slot, std::string &pathOut,
						  std::vector<IcebergPartitionValue> &valuesOut);
	void appendPartitionFileMeta(IcebergPartitionFile &pf);

private:
	virtual void appendFileMeta();
};

}
}
