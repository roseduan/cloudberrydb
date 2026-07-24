#ifndef PARQUET_READER_H
#define PARQUET_READER_H

#include <memory>
#include <vector>

#include <parquet/api/reader.h>
#include <parquet/internal/arrow/io/interfaces.h>
#include "src/common/fileSystemWrapper.h"
#include "src/common/dataBufferArray.h"
#include "base_reader.h"

using Datalake::Internal::dataBufferArray;
using Datalake::Internal::dataBuff;

class ParquetReader : public BaseFileReader
{
private:
	int numColumns_;
	std::string filePath_;
	ossFileStream fileStream_;
	std::vector<int> rowGroups_;
	std::unique_ptr<parquet::ParquetFileReader> reader_;
	std::vector<std::shared_ptr<parquet::Scanner>> scanners_;
	std::shared_ptr<parquet::FileMetaData> metadata;

	dataBufferArray *buffer_;
	List *quals_;	/* WHERE-clause quals (Expr) for row-group min/max skip; NIL if none */
	int rowGroupsSkipped_ = 0;	/* count of row groups pruned by min/max stats (observability) */

	/*
	 * Batch decimal->numeric conversion.  For NUMERIC columns, instead of
	 * converting one value per row (readDecimal via readPrimitive), we convert
	 * a batch of values at once into a reused buffer with the type switch
	 * hoisted out of the per-value loop, then serve one value per row.  This
	 * amortizes per-value dispatch and sets up SIMD.  Gated per-scan by the
	 * GUC datalake.iceberg_enable_batch_read (read in supportsBatchPrimitive).
	 */
	static constexpr int NUMERIC_BATCH_SIZE = 1024;
	struct NumericColumnBatch
	{
		/*
		 * Fixed-stride slot buffer: each of NUMERIC_BATCH_SIZE slots holds one
		 * NUMERIC at offset slot*stride_.  Allocated once and reused; no
		 * per-value resize() or offset bookkeeping in the fill loop.
		 */
		std::vector<char>     buf;		/* NUMERIC_BATCH_SIZE fixed-size slots */
		std::vector<bool>     isnull;	/* per-slot null flag */
		size_t stride_ = 0;				/* bytes per slot (max NUMERIC size) */
		int filled = 0;					/* number of slots populated this batch */
		int pos = 0;					/* next slot to serve */
	};
	std::vector<NumericColumnBatch> numBatch_;	/* indexed by columnIndex_ */
	void fillNumericBatch(const TypeInfo &typInfo, NumericColumnBatch &b);

	/*
	 * Whole-column batch read path (all supported types).
	 *
	 * Instead of one TypedScanner::NextValue() virtual call per value per
	 * column (with def-level bookkeeping per value), each column is decoded
	 * in COLUMN_BATCH_SIZE chunks straight from TypedColumnReader::ReadBatch()
	 * into a dense array, converted to Datum-ready form in one tight per-type
	 * loop (type switch hoisted out of the per-value path), and then served
	 * per row as a plain array read.  Columns with repetition levels (nested/
	 * repeated) or types outside the known set keep the old Scanner path.
	 * Gated by the same GUC as the numeric batch path
	 * (datalake.iceberg_enable_batch_read); decided once at the first row
	 * group, after any dispatched SET has been applied on the QE.
	 */
	static constexpr int COLUMN_BATCH_SIZE = 1024;
	enum FillKind
	{
		FILL_NONE = 0,
		FILL_BOOL,
		FILL_INT2,
		FILL_INT4,
		FILL_INT8,			/* also TIMEOID */
		FILL_FLOAT4,
		FILL_FLOAT8,
		FILL_DATE,
		FILL_TIMESTAMP,		/* TIMESTAMP/TIMESTAMPTZ from INT64 */
		FILL_NUMERIC_I32,
		FILL_NUMERIC_I64,
		FILL_NUMERIC_FLBA,
		FILL_TEXT,			/* TEXT/VARCHAR/BYTEA from BYTE_ARRAY */
		FILL_BPCHAR,
		FILL_UUID
	};
	struct ColumnBatch
	{
		std::shared_ptr<parquet::ColumnReader> reader;	/* per row group */
		FillKind kind = FILL_NONE;
		int attr = -1;				/* index into typeMap_ */
		int16_t maxDef = 0;			/* max definition level (0 = required) */
		int pos = 0;				/* next slot to serve */
		int filled = 0;				/* slots filled this batch */
		std::vector<Datum> datums;	/* by-value datums / numeric slab pointers */
		std::vector<uint8_t> isnull;
		std::vector<size_t> offsets;	/* varlena/uuid offsets into slab */
		std::vector<char> slab;		/* numeric fixed-stride slots / varlena arena */
		size_t slabUsed = 0;
		std::vector<int16_t> defs;	/* def-level scratch */
		std::vector<char> raw;		/* dense raw value scratch */

		/*
		 * Dictionary-aware conversion (conversion-heavy kinds only).  When the
		 * column chunk is fully dictionary-encoded, the expensive per-value
		 * conversion (decimal->NUMERIC divmod, varlena construction) runs once
		 * per distinct dictionary entry instead of once per row; rows are then
		 * served as dictDatums[index].  dictSlab is stable for a whole row
		 * group, so dictDatums can hold direct pointers.
		 */
		bool dictExposed = false;
		const void *dictConvertedFrom = nullptr;	/* dict ptr already converted */
		std::vector<Datum> dictDatums;
		std::vector<char> dictSlab;
	};
	std::vector<ColumnBatch> colBatch_;	/* indexed by columnIndex_ */
	bool batchMode_ = false;
	bool batchModeDecided_ = false;

	/*
	 * Attributes that actually produce a value per row (projected batch
	 * columns, readFn_ columns, or scanner-path columns).  Unprojected
	 * attributes (columnIndex_ < 0 / unknown file type) are always NULL, so
	 * the fused populateRecord loop pre-fills them once per record buffer and
	 * then iterates only this list: for a wide table with a narrow projection
	 * (e.g. 3 of 23 columns) this removes the dominant per-row loop overhead.
	 */
	std::vector<uint32_t> activeAttrs_;
	void *nullPrefilledRecord_ = nullptr;	/* record buffer already pre-filled */
	FillKind resolveFillKind(const TypeInfo &typInfo);
	static bool fillKindWantsDict(FillKind kind)
	{
		return kind == FILL_NUMERIC_I32 || kind == FILL_NUMERIC_I64 ||
			   kind == FILL_NUMERIC_FLBA || kind == FILL_TEXT ||
			   kind == FILL_BPCHAR;
	}
	void setupBatchColumns();
	void refillColumnBatch(int idx);
	void refillColumnBatchDict(ColumnBatch &b, const TypeInfo &ti);
	void convertDict(ColumnBatch &b, const TypeInfo &ti, const void *dict, int32_t dictLen);
	static Datum serveBatchVal(BaseFileReader *r, int idx, bool &isNull);
	static Datum serveBatchPtr(BaseFileReader *r, int idx, bool &isNull);

	bool invalidFileOffset(int64_t startIndex, int64_t preStartIndex, int64_t preCompressedSize);
	void filterRowGroupByOffset(int64_t startOffset, int64_t endOffset);
	/* issue #297: row-group min/max ("zone map") pruning.
	 * rowGroupMightMatch returns false if quals_ prove row group rgIdx has no
	 * matching rows.  The expr helpers take Expr* / parquet::RowGroupMetaData*
	 * (declared void* to keep PG node types out of this header) and return true
	 * only when they PROVE the row group is excluded. */
	bool rowGroupMightMatch(int rgIdx);
	TIMEUNIT getTimeUnit(const parquet::ColumnDescriptor *field);

	/* Per-type direct read functions — eliminate virtual + switch per column */
	static Datum readBoolColumn(BaseFileReader *r, int idx, bool &isNull);
	static Datum readInt16Column(BaseFileReader *r, int idx, bool &isNull);
	static Datum readInt32Column(BaseFileReader *r, int idx, bool &isNull);
	static Datum readInt64Column(BaseFileReader *r, int idx, bool &isNull);
	static Datum readFloat4Column(BaseFileReader *r, int idx, bool &isNull);
	static Datum readFloat8Column(BaseFileReader *r, int idx, bool &isNull);
	static Datum readDateColumn(BaseFileReader *r, int idx, bool &isNull);

	ReadColumnFn resolveReadFn(const TypeInfo &typInfo);

protected:
	Datum readPrimitive(const TypeInfo &typInfo, bool &isNull);
	bool supportsBatchPrimitive() override;
	Datum readBatchPrimitive(const TypeInfo &typInfo, bool &isNull) override;
	Datum readDecimal(std::shared_ptr<parquet::Scanner> &scannner, const TypeInfo &typinfo, bool &isNull);
	bool readNextRowGroup();
	void createMapping(List *columnDesc, bool *attrUsed);
	void decodeRecord();
	void populateRecord(DatalakeInternalRecord *record) override;

public:
	ParquetReader(MemoryContext rowContext, char *filePath, ossFileStream fileStream, dataBufferArray *buffer, List *quals);
	~ParquetReader();

	void open(List *columnDesc, bool *attrUsed, int64_t startOffset, int64_t endOffset);
	void close();
	bool datumOwnedByReader(int attIdx) const override;
};

#endif // PARQUET_READER_H
