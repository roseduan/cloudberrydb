// #include "postgres.h"
#include "common.h"
#include "base_reader.h"

extern "C"
{
#include "src/datalake_def.h"
#include "access/tupdesc.h"
#include "utils.h"
}

BaseFileReader::BaseFileReader(MemoryContext rowContext)
	: curGroup_(-1), curRow_(0), numRows_(0), rowContext_(rowContext)
{}

BaseFileReader::~BaseFileReader()
{}

void BaseFileReader::populateRecord(DatalakeInternalRecord *record)
{
	bool   isNull;
	size_t size = typeMap_.size();

	decodeRecord();

	for (size_t attr = 0; attr < size; attr++)
	{
		TypeInfo &typInfo = typeMap_[attr];

		/*
		 * When readFn_ is set (Parquet path), call it directly to avoid
		 * virtual dispatch + type switch on every column of every row.
		 * The NULL readFn_ case handles unmapped columns and the Avro
		 * fallback path.
		 */
		if (typInfo.readFn_)
		{
			isNull = false;
			record->values[attr] = typInfo.readFn_(this, typInfo.columnIndex_, isNull);
			record->nulls[attr] = isNull;
		}
		else if (typInfo.columnIndex_ < 0 || typInfo.fileTypeId_ == InvalidOid)
		{
			record->nulls[attr] = true;
		}
		else
		{
			isNull = false;
			record->values[attr] = readPrimitive(typInfo, isNull);
			record->nulls[attr] = isNull;
		}
	}

	record->position = rowPositions_[curGroup_] + curRow_;
}

bool
BaseFileReader::next(DatalakeInternalRecord *record)
{
	if (curRow_ >= numRows_)
	{
		do
		{
			if (!readNextRowGroup())
				return false;
		}
		while (!numRows_);
	}

	/*
	 * Skip per-row MemoryContextReset and context switch here.
	 * When buffer_ is used (Iceberg/Hudi), populateRecord() does no palloc
	 * so rowContext_ stays empty. The caller (FDW iterateScanStatus) already
	 * resets its own per-row MemoryContext, so any allocations from
	 * variable-length types without buffer_ are still cleaned up each row.
	 */
	populateRecord(record);

	curRow_++;

	return true;
}

int64
BaseFileReader::transformTimestamp(int64 timestamp, TIMEUNIT timeUnit)
{
	switch (timeUnit)
	{
		case TIMEUNIT_MILLIS:
			return timestamp / 1000;
		case TIMEUNIT_MICROS:
			return timestamp / 1000000;
		case TIMEUNIT_NANOS:
			return timestamp / 1000000000;
		default:
			/* Fallback: assume microseconds (Iceberg standard) */
			return timestamp / 1000000;
	}
}
