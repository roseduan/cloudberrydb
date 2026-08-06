#ifndef DATALAKE_PROVIDER_H
#define DATALAKE_PROVIDER_H

#include <map>
#include <vector>
#include <memory>
#include <iostream>

extern "C" 
{
#include "src/datalake_type.h"
}

#include "src/common/fileSystemWrapper.h"

#define DATALAKE_EXPORT_NAME ("datalake")
#define PARQUET_WRITE_SUFFIX ("parquet")
#define AVRO_WRITE_SUFFIX ("avro")
#define ORC_WRITE_SUFFIX ("orc")
#define CSV_WRITE_SUFFIX ("csv")
#define TEXT_WRITE_SUFFIX ("txt")

class Provider {

public:

	virtual void createHandler(void* sstate);

	virtual int64_t read(void *values, void *nulls);

	virtual int64_t read(void *values, void *nulls, void *tid);

	virtual int64_t read(void **recordBatch);

	virtual int64_t readWithBuffer(void* buffer, int64_t length);

	virtual int64_t write(const void* buf, int64_t length);

	virtual void setPartitionValue(void* values, void* nulls);

	/*
	 * Set the identity partition tuple of the data file that the next
	 * position-delete record(s) target, so the position-delete writer can fan
	 * out per partition and stamp each delete file's partition.  The argument
	 * is a PostgreSQL List* (String nodes / NULL cells, spec order) passed as
	 * void* to keep PG node types out of this widely-included header; NULL/NIL
	 * means unpartitioned.  No-op by default; overridden by the Iceberg
	 * position-delete writer.
	 */
	virtual void setDeletePartition(void* partitionValues) {}

	virtual void destroyHandler();

	/* Rewind the scan to the beginning for ExecReScan; no-op by default,
	 * overridden by providers whose reader supports re-scan (e.g. Iceberg). */
	virtual void reScan();

	/* For fast-path scan bypass — returns NULL by default */
	virtual void *getProtocolContext() { return NULL; }

	virtual CompressType getCompressType(char* type);

	virtual const char* getReadFileName();

	virtual std::string generateWriteFileName(const std::string &writePrefix, const std::string &compress, const std::string &suffix);

	virtual std::string generateIcebergUuidFileName(const std::string &writePrefix, const std::string &suffix);

};

std::shared_ptr<Provider> getProvider(DLTblFmt type, DLCmdType cmd, bool vectorization);

#endif //PROVIDER_H
