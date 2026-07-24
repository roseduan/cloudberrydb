#ifndef BASE_READER_H
#define BASE_READER_H
extern "C"
{
#include "postgres.h"
#include "utils/memutils.h"
#include "catalog/pg_type_d.h"
}

#include <vector>
#include <string_view>
#include <utility>
#include <array>

struct DatalakeInternalRecord;
struct List;

typedef enum
{
	TIMEUNIT_MILLIS,
	TIMEUNIT_MICROS,
	TIMEUNIT_NANOS,
	TIMEUNIT_UNKNOWN
} TIMEUNIT;

union ReaderValue
{
	bool    boolValue;
	int32_t int32Value;
	int64_t int64Value;
	float   floatValue;
	double  doubleValue;
};

class BaseFileReader;

/*
 * Per-column function pointer that reads one value from the underlying
 * file format, bypassing virtual dispatch and type-switch overhead.
 * Set during createMapping(); NULL for unmapped columns.
 */
typedef Datum (*ReadColumnFn)(BaseFileReader *reader, int scannerIdx, bool &isNull);

class BaseFileReader
{
protected:
	struct TypeInfo
	{
		Oid pgTypeId_;
		int typeMod_;
		Oid fileTypeId_;
		int columnIndex_;
		TIMEUNIT timeUnit_;
		int scale_;			/* Decimal scale (NUMERIC only) */
		int precision_;		/* Decimal precision (NUMERIC only) */
		int typeLength_;	/* Fixed-length byte size (BPCHAR/FLBA only) */
		ReadColumnFn readFn_;	/* Direct read function, bypasses virtual+switch */
	};

	int curGroup_;
	uint32_t curRow_;
	uint32_t numRows_;
	MemoryContext rowContext_;
	std::vector<TypeInfo> typeMap_;
	std::vector<int64_t> rowPositions_;

	virtual Datum readPrimitive(const TypeInfo &typInfo, bool &isNull) = 0;

	/*
	 * Batch read path.  Formats that can decode a whole column of values at
	 * once (Parquet) override supportsBatchPrimitive() to return true and
	 * implement readBatchPrimitive(); populateRecord() then serves values
	 * from a per-column batch buffer instead of one NextValue() per row,
	 * amortizing per-value dispatch and decimal->numeric conversion.  The
	 * default forwards to readPrimitive() so non-batch formats are unaffected.
	 */
	virtual bool supportsBatchPrimitive() { return false; }
	virtual Datum readBatchPrimitive(const TypeInfo &typInfo, bool &isNull)
	{ return readPrimitive(typInfo, isNull); }

	virtual bool readNextRowGroup() = 0;
	virtual void createMapping(List *columnDesc, bool *attrUsed) = 0;
	virtual void decodeRecord() = 0;

	/*
	 * Virtual so batch-capable readers (Parquet) can serve all columns of a
	 * row in one fused loop; the base implementation is the generic per-column
	 * readFn_/readPrimitive dispatch.  One virtual call per row, not per value.
	 */
	virtual void populateRecord(DatalakeInternalRecord *record);
	int64 transformTimestamp(int64 timestamp, TIMEUNIT timeUnit);

public:
	BaseFileReader(MemoryContext rowContext);
	virtual ~BaseFileReader() = 0;
	bool next(DatalakeInternalRecord *record);
	virtual void open(List *columnDesc, bool *attrUsed, int64_t startOffset, int64_t endOffset) = 0;
	virtual void close() = 0;

	/*
	 * Whether Datums this reader returns for the given record attribute
	 * point into reader-owned memory (a batch slab) rather than into
	 * caller-owned palloc'd chunks.  Callers must not pfree such Datums.
	 * Row-path readers allocate per value, so the default is false.
	 */
	virtual bool
	datumOwnedByReader(int attIdx) const
	{
		(void) attIdx;
		return false;
	}
};

#endif // BASE_READER_H
