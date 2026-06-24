#include <parquet/internal/arrow/util/decimal.h>
#include <parquet/internal/arrow/result.h>
#include "parquet_reader.h"
#include "common.h"
#include "gopher_random_file.h"
#include "datalake_numeric.h"

extern "C"
{
#include "postgres.h"
#include "access/tupdesc.h"
#include "datatype/timestamp.h"
#include "utils/memutils.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "access/stratnum.h"
#include "catalog/pg_type.h"
#include "utils/date.h"
#include "utils.h"
#include "src/datalake_def.h"
}

/* ---------------------------------------------------------------------------
 * issue #297: row-group min/max ("zone map") pruning helpers.
 *
 * Given the WHERE-clause quals (raw Expr) and a Parquet row group's per-column
 * statistics, decide whether the row group can be skipped entirely.  The rule
 * is strictly conservative: a helper returns "excluded" only when it can PROVE
 * no row in [min,max] satisfies the predicate; anything unhandled (unsupported
 * type/operator, missing stats) returns "not excluded" so the row group is kept
 * and the executor still applies the real qual.  v1 handles integer columns
 * (INT32/INT64) with btree comparison operators and AND/OR composition.
 * --------------------------------------------------------------------------- */
namespace {

/* Row group [min,max] as int64 in PG's internal domain for the column type
 * (integer / date / timestamp).  Converts Parquet's Unix-epoch date/timestamp
 * to PG's 2000-epoch domain using the SAME formulas the value reader uses, so
 * the comparison is consistent.  Returns false for unsupported types, missing
 * stats, or risky unit conversions (timestamp NANOS truncation). */
static bool
pqStatMinMaxInt64(parquet::ColumnChunkMetaData *cc, Oid pgType, int timeUnit,
				  int64_t &mn, int64_t &mx)
{
	auto stats = cc->statistics();
	if (!stats || !stats->HasMinMax())
		return false;

	int64_t rawMin, rawMax;
	switch (cc->type())
	{
		case parquet::Type::INT32:
		{
			auto s = std::static_pointer_cast<parquet::Int32Statistics>(stats);
			rawMin = s->min(); rawMax = s->max(); break;
		}
		case parquet::Type::INT64:
		{
			auto s = std::static_pointer_cast<parquet::Int64Statistics>(stats);
			rawMin = s->min(); rawMax = s->max(); break;
		}
		default:
			return false;
	}

	switch (pgType)
	{
		case INT2OID: case INT4OID: case INT8OID:
			mn = rawMin; mx = rawMax;
			return true;
		case DATEOID:
			/* Parquet DATE = days since Unix epoch; PG DateADT = days since 2000-01-01. */
			mn = rawMin + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE);
			mx = rawMax + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE);
			return true;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			const int64_t off = ((int64_t)(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE))
								 * SECS_PER_DAY * USECS_PER_SEC;
			switch (timeUnit)
			{
				case TIMEUNIT_MILLIS:
					mn = rawMin * 1000 - off; mx = rawMax * 1000 - off; return true;
				case TIMEUNIT_MICROS:
					mn = rawMin - off; mx = rawMax - off; return true;
				default:
					/* NANOS (/1000 truncation risks boundary errors) or unknown: keep. */
					return false;
			}
		}
		default:
			return false;
	}
}

/* Const value as int64 in the column's PG domain; false unless the (non-null)
 * Const type matches the column's domain (int/date/timestamp). */
static bool
pgConstInt64(Const *c, Oid colType, int64_t &out)
{
	if (c->constisnull)
		return false;
	switch (colType)
	{
		case INT2OID: case INT4OID: case INT8OID:
			switch (c->consttype)
			{
				case INT2OID: out = (int64_t) DatumGetInt16(c->constvalue); return true;
				case INT4OID: out = (int64_t) DatumGetInt32(c->constvalue); return true;
				case INT8OID: out = (int64_t) DatumGetInt64(c->constvalue); return true;
				default: return false;
			}
		case DATEOID:
			if (c->consttype != DATEOID) return false;
			out = (int64_t) DatumGetDateADT(c->constvalue);
			return true;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			if (c->consttype != TIMESTAMPOID && c->consttype != TIMESTAMPTZOID) return false;
			out = (int64_t) DatumGetTimestamp(c->constvalue);
			return true;
		default:
			return false;
	}
}

/* btree strategy (1=<,2=<=,3==,4=>=,5=>) for opno, or 0 if not a btree comparison. */
static int
pgOpBtreeStrategy(Oid opno)
{
	int			strat = 0;
	List	   *interp = get_op_btree_interpretation(opno);
	ListCell   *lc;

	foreach(lc, interp)
	{
		OpBtreeInterpretation *opi = (OpBtreeInterpretation *) lfirst(lc);
		if (opi->strategy >= BTLessStrategyNumber &&
			opi->strategy <= BTGreaterStrategyNumber)
		{
			strat = opi->strategy;
			break;
		}
	}
	list_free_deep(interp);
	return strat;
}

/* true iff integer range [mn,mx] provably has NO value v with (v strat c). */
static bool
intRangeExcluded(int strat, int64_t mn, int64_t mx, int64_t c)
{
	switch (strat)
	{
		case BTLessStrategyNumber:			return mn >= c;	/* no v < c   */
		case BTLessEqualStrategyNumber:		return mn >  c;	/* no v <= c  */
		case BTEqualStrategyNumber:			return c < mn || c > mx;
		case BTGreaterEqualStrategyNumber:	return mx <  c;	/* no v >= c  */
		case BTGreaterStrategyNumber:		return mx <= c;	/* no v > c   */
		default:							return false;
	}
}

} /* anonymous namespace */

bool
ParquetReader::opExprExcludesRowGroup(void *opPtr, void *rgPtr)
{
	OpExpr *op = (OpExpr *) opPtr;
	parquet::RowGroupMetaData *rg = (parquet::RowGroupMetaData *) rgPtr;

	if (list_length(op->args) != 2)
		return false;

	Node *larg = (Node *) linitial(op->args);
	Node *rarg = (Node *) lsecond(op->args);
	Var *var = NULL;
	Const *con = NULL;
	bool varOnLeft;

	if (IsA(larg, Var) && IsA(rarg, Const)) { var = (Var *) larg; con = (Const *) rarg; varOnLeft = true; }
	else if (IsA(larg, Const) && IsA(rarg, Var)) { var = (Var *) rarg; con = (Const *) larg; varOnLeft = false; }
	else return false;

	int strat = pgOpBtreeStrategy(op->opno);
	if (strat == 0)
		return false;

	/* (const op var) -> express as (var flipped-op const). */
	if (!varOnLeft)
	{
		switch (strat)
		{
			case BTLessStrategyNumber:			strat = BTGreaterStrategyNumber; break;
			case BTLessEqualStrategyNumber:		strat = BTGreaterEqualStrategyNumber; break;
			case BTGreaterStrategyNumber:		strat = BTLessStrategyNumber; break;
			case BTGreaterEqualStrategyNumber:	strat = BTLessEqualStrategyNumber; break;
			default: break;	/* '=' is symmetric */
		}
	}

	int attno = var->varattno;
	if (attno <= 0 || (size_t) attno > typeMap_.size())
		return false;
	const auto &ti = typeMap_[attno - 1];
	if (ti.columnIndex_ < 0)
		return false;

	int64_t c;
	if (!pgConstInt64(con, ti.pgTypeId_, c))
		return false;

	int64_t mn, mx;
	auto cc = rg->ColumnChunk(ti.columnIndex_);
	if (!pqStatMinMaxInt64(cc.get(), ti.pgTypeId_, (int) ti.timeUnit_, mn, mx))
		return false;

	return intRangeExcluded(strat, mn, mx, c);
}

/* true iff an IS NULL / IS NOT NULL test proves the row group has no matching rows. */
bool
ParquetReader::nullTestExcludesRowGroup(void *ntPtr, void *rgPtr)
{
	NullTest *nt = (NullTest *) ntPtr;
	parquet::RowGroupMetaData *rg = (parquet::RowGroupMetaData *) rgPtr;

	if (nt->arg == NULL || !IsA(nt->arg, Var))
		return false;
	Var *var = (Var *) nt->arg;
	int attno = var->varattno;
	if (attno <= 0 || (size_t) attno > typeMap_.size())
		return false;
	const auto &ti = typeMap_[attno - 1];
	if (ti.columnIndex_ < 0)
		return false;

	auto cc = rg->ColumnChunk(ti.columnIndex_);
	auto stats = cc->statistics();
	if (!stats || !stats->HasNullCount())
		return false;
	int64_t nulls = stats->null_count();

	if (nt->nulltesttype == IS_NULL)
		return nulls == 0;					/* no nulls -> IS NULL excluded */
	else								/* IS_NOT_NULL */
		return nulls == rg->num_rows();		/* all null  -> IS NOT NULL excluded */
}

/* true iff expr proves the row group has no matching rows. */
bool
ParquetReader::exprExcludesRowGroup(void *exprPtr, void *rgPtr)
{
	Expr *expr = (Expr *) exprPtr;

	if (expr == NULL)
		return false;

	switch (nodeTag(expr))
	{
		case T_OpExpr:
			return opExprExcludesRowGroup(expr, rgPtr);
		case T_NullTest:
			return nullTestExcludesRowGroup(expr, rgPtr);
		case T_BoolExpr:
		{
			BoolExpr *b = (BoolExpr *) expr;
			ListCell *lc;

			if (b->boolop == AND_EXPR)
			{
				/* AND excluded if ANY conjunct excludes. */
				foreach(lc, b->args)
					if (exprExcludesRowGroup((void *) lfirst(lc), rgPtr))
						return true;
				return false;
			}
			if (b->boolop == OR_EXPR)
			{
				/* OR excluded only if ALL disjuncts exclude. */
				foreach(lc, b->args)
					if (!exprExcludesRowGroup((void *) lfirst(lc), rgPtr))
						return false;
				return true;
			}
			return false;	/* NOT: conservative, keep */
		}
		default:
			return false;
	}
}

bool
ParquetReader::rowGroupMightMatch(int rgIdx)
{
	if (quals_ == NIL)
		return true;

	auto rg = metadata->RowGroup(rgIdx);
	ListCell *lc;

	foreach(lc, quals_)
	{
		Expr *e = (Expr *) lfirst(lc);
		if (exprExcludesRowGroup((void *) e, (void *) rg.get()))
			return false;
	}
	return true;
}

ParquetReader::ParquetReader(MemoryContext rowContext, char *filePath, gopherFS gopherFilesystem, dataBufferArray *buffer, List *quals)
	: BaseFileReader(rowContext), numColumns_(0), filePath_(filePath), gopherFilesystem_(gopherFilesystem), buffer_(buffer), quals_(quals)
{}

ParquetReader::~ParquetReader()
{}

TIMEUNIT
ParquetReader::getTimeUnit(const parquet::ColumnDescriptor *field)
{
	const auto &logicalType = field->logical_type();
	if (!logicalType->is_timestamp())
		return TIMEUNIT_UNKNOWN;

	const auto *timestampType = dynamic_cast<const parquet::TimestampLogicalType*>(logicalType.get());
	switch (timestampType->time_unit())
	{
		case parquet::LogicalType::TimeUnit::MILLIS:
			return TIMEUNIT_MILLIS;
		case parquet::LogicalType::TimeUnit::MICROS:
			return TIMEUNIT_MICROS;
		case parquet::LogicalType::TimeUnit::NANOS:
			return TIMEUNIT_NANOS;
		default:
			throw Error("parquet error: Unknown timestamp precision");
	}
}

void
ParquetReader::createMapping(List *columnDesc, bool *attrUsed)
{
	int       i;
	int       j;
	ListCell *lc;
	auto      schema = metadata->schema();

	numColumns_ = schema->num_columns();
	foreach_with_count(lc, columnDesc, i)
	{
		DatalakeFieldDescription *entry = (DatalakeFieldDescription *) lfirst(lc);
		TypeInfo typInfo = {entry->typeOid, entry->typeMod, InvalidOid, -1, TIMEUNIT_UNKNOWN, 0, 0, 0, nullptr};
		typeMap_.push_back(typInfo);

		if (!attrUsed[i])
			continue;

		for (j = 0; j < numColumns_; j++)
		{
			const parquet::ColumnDescriptor *field = schema->Column(j);
			auto fieldName = field->name();

			if (pg_strcasecmp(entry->name, fieldName.c_str()) == 0)
			{
				typeMap_[i].columnIndex_ = j;
				typeMap_[i].fileTypeId_ = mapParquetDataType(field->physical_type());
				typeMap_[i].timeUnit_ = getTimeUnit(field);
				typeMap_[i].scale_ = field->type_scale();
				typeMap_[i].precision_ = field->type_precision();
				typeMap_[i].typeLength_ = field->type_length();
				typeMap_[i].readFn_ = resolveReadFn(typeMap_[i]);
				break;
			}
		}
	}
}

bool
ParquetReader::readNextRowGroup()
{
	curGroup_++;

	if ((uint) curGroup_ >= rowGroups_.size())
		return false;

	auto rowGroupReader = reader_->RowGroup(rowGroups_[curGroup_]);
	size_t typeSize = typeMap_.size();

	curRow_ = 0;
	numRows_ = rowGroupReader->metadata()->num_rows();

	/*
	 * Use the full row group size as Scanner batch_size instead of the default 128.
	 * This makes Scanner::ReadBatch() decode the entire column in one call,
	 * so subsequent NextValue() calls are pure array accesses (similar to PAX's
	 * ReadStripe + GetDatum pattern). This reduces ReadBatch call count from
	 * ~(numRows/128) to 1 per column per row group.
	 */
	int64_t scannerBatchSize = numRows_ > 0 ? numRows_ : parquet::DEFAULT_SCANNER_BATCH_SIZE;

	scanners_.clear();
	scanners_.resize(numColumns_);
	for (size_t i = 0; i < typeSize; ++i)
	{
		TypeInfo &typInfo = typeMap_[i];

		if (typInfo.columnIndex_ >= 0)
		{
			scanners_[typInfo.columnIndex_] = parquet::Scanner::Make(
				rowGroupReader->Column(typInfo.columnIndex_), scannerBatchSize);
		}
	}

	return true;
}

void
ParquetReader::open(List *columnDesc, bool *attrUsed, int64 startOffset, int64 endOffset)
{
	std::string filename = convertToGopherPath(filePath_);
	reader_ = parquet::ParquetFileReader::Open(std::make_shared<GopherRandomAccessFile> (gopherFilesystem_, filename));
	metadata = reader_->metadata();
	createMapping(columnDesc, attrUsed);
	filterRowGroupByOffset(startOffset, endOffset);

	/* Report row-group min/max pruning at DEBUG1 for diagnostics. */
	if (rowGroupsSkipped_ > 0)
		elog(DEBUG1,
			 "datalake parquet row-group skip: file=%s skipped=%d of %d row groups by min/max",
			 filePath_.c_str(), rowGroupsSkipped_, metadata->num_row_groups());
}

void
ParquetReader::close()
{
	scanners_.clear();
	reader_->Close();
}

bool
ParquetReader::invalidFileOffset(int64_t startIndex, int64_t preStartIndex, int64_t preCompressedSize)
{
	bool invalid = false;

	// checking the first rowGroup
	if (preStartIndex == 0 && startIndex != 4)
	{
		invalid = true;
		return invalid;
	}

	//calculate start index for other blocks
	int64_t minStartIndex = preStartIndex + preCompressedSize;
	if (startIndex < minStartIndex)
		invalid = true;

	return invalid;
}

void
ParquetReader::filterRowGroupByOffset(int64_t startOffset, int64_t endOffset)
{
	int64_t preStartIndex = 0;
	int64_t preCompressedSize = 0;
	int64_t curRowCount = 0;

	for (int i = 0; i < metadata->num_row_groups(); i++)
	{
		int64_t totalSize = 0;
		int64_t startIndex;

		if (startOffset == -1)
		{
			if (rowGroupMightMatch(i))
			{
				rowGroups_.push_back(i);
				rowPositions_.push_back(0);
			}
			else
				rowGroupsSkipped_++;
			continue;
		}

		auto rowGroup = metadata->RowGroup(i);
		startIndex = rowGroup->file_offset();

		if (invalidFileOffset(startIndex, preStartIndex, preCompressedSize))
		{
			if (preStartIndex == 0)
				startIndex = 4;
			else
				startIndex = preStartIndex + preCompressedSize;
		}

		preStartIndex = startIndex;
		preCompressedSize = rowGroup->total_compressed_size();

		for (int j = 0; j < rowGroup->num_columns(); j++)
		{
			auto col = rowGroup->ColumnChunk(j);
			totalSize += col->total_compressed_size();
		}

		int64_t midPoint = startIndex + totalSize / 2;
		if (midPoint >= startOffset && midPoint < endOffset)
		{
			if (rowGroupMightMatch(i))
			{
				rowGroups_.push_back(i);
				rowPositions_.push_back(curRowCount);
			}
			else
				rowGroupsSkipped_++;
		}

		curRowCount += rowGroup->num_rows();
	}
}

Datum
ParquetReader::readPrimitive(const TypeInfo &typInfo, bool &isNull)
{
	ReaderValue d;
	auto &scanner = scanners_[typInfo.columnIndex_];

	switch (typInfo.pgTypeId_)
	{
		case BOOLOID:
		{
			((parquet::TypedScanner<parquet::BooleanType> *)scanner.get())->NextValue(&d.boolValue, &isNull);
			return BoolGetDatum(d.boolValue);
		}
		case INT2OID:
		{
			/* Parquet stores SMALLINT as INT32; read and truncate */
			((parquet::TypedScanner<parquet::Int32Type> *)scanner.get())->NextValue(&d.int32Value, &isNull);
			return Int16GetDatum((int16) d.int32Value);
		}
		case INT4OID:
		{
			((parquet::TypedScanner<parquet::Int32Type> *)scanner.get())->NextValue(&d.int32Value, &isNull);
			return Int32GetDatum(d.int32Value);
		}
		case TIMEOID:
		case INT8OID:
		{
			((parquet::TypedScanner<parquet::Int64Type> *)scanner.get())->NextValue(&d.int64Value, &isNull);
			return Int64GetDatum(d.int64Value);
		}
		case FLOAT4OID:
		{
			((parquet::TypedScanner<parquet::FloatType> *)scanner.get())->NextValue(&d.floatValue, &isNull);
			return Float4GetDatum(d.floatValue);
		}
		case FLOAT8OID:
		{
			((parquet::TypedScanner<parquet::DoubleType> *)scanner.get())->NextValue(&d.doubleValue, &isNull);
			return Float8GetDatum(d.doubleValue);
		}
		case BYTEAOID:
		case TEXTOID:
		case VARCHAROID:
		{
			parquet::ByteArray value;
			((parquet::TypedScanner<parquet::ByteArrayType> *)scanner.get())->NextValue(&value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);

			if (!buffer_)
			{
				bytea *result = (bytea *) gpdbPalloc(value.len + VARHDRSZ);
				SET_VARSIZE(result, value.len + VARHDRSZ);
				memcpy(VARDATA(result), value.ptr, value.len);
				return PointerGetDatum(result);
			}
			if (value.len + VARHDRSZ > static_cast<uint32>(buffer_->getDataBuffer(typInfo.columnIndex_)->length))
			{
				buffer_->resizeDataBuffer(typInfo.columnIndex_, value.len + VARHDRSZ);
			}
			dataBuff *colBuffer = buffer_->getDataBuffer(typInfo.columnIndex_);
			SET_VARSIZE(colBuffer->buffer, value.len + VARHDRSZ);
			memcpy(VARDATA(colBuffer->buffer), value.ptr, value.len);
			return PointerGetDatum(colBuffer->buffer);
		}
		case BPCHAROID:
		{
			/*
			 * BPCHAR on Iceberg is written as variable-length BYTE_ARRAY +
			 * UTF8 (Iceberg `string`; see commit 48bf8311b1a / issue #321),
			 * with trailing spaces stripped at write time so external engines
			 * (Spark/Trino) see clean strings.  To keep PG CHAR(N) semantics
			 * identical to a heap table, re-pad the value with blanks up to the
			 * declared length N on read, mirroring PG's bpchar() coercion
			 * (src/backend/utils/adt/varchar.c).  N comes from atttypmod
			 * (typeMod_ == N + VARHDRSZ); typeMod_ < VARHDRSZ means an
			 * unbounded `char` with no length, which is never padded.
			 */
			parquet::ByteArray value;
			((parquet::TypedScanner<parquet::ByteArrayType> *)scanner.get())->NextValue(&value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);

			int pad = 0;
			if (typInfo.typeMod_ >= (int) VARHDRSZ)
			{
				int maxlen = typInfo.typeMod_ - VARHDRSZ;	/* declared char count N */
				/*
				 * Count UTF-8 code points directly (every byte that is not a
				 * 10xxxxxx continuation byte starts a character) rather than
				 * pg_mbstrlen_with_len(): the on-disk Iceberg `string` is always
				 * UTF-8, so the character count must not depend on the database
				 * encoding -- under a non-UTF-8 server encoding pg_mbstrlen_with_len
				 * would mis-count and pad to the wrong length.
				 */
				int charlen = 0;
				for (uint32 k = 0; k < value.len; k++)
					if (((unsigned char) value.ptr[k] & 0xC0) != 0x80)
						charlen++;
				if (charlen < maxlen)
					pad = maxlen - charlen;					/* blanks are single-byte */
			}
			uint32 totalLen = value.len + pad;

			if (!buffer_)
			{
				bytea *result = (bytea *) gpdbPalloc(totalLen + VARHDRSZ);
				SET_VARSIZE(result, totalLen + VARHDRSZ);
				memcpy(VARDATA(result), value.ptr, value.len);
				if (pad > 0)
					memset(VARDATA(result) + value.len, ' ', pad);
				return PointerGetDatum(result);
			}
			if (totalLen + VARHDRSZ > static_cast<uint32>(buffer_->getDataBuffer(typInfo.columnIndex_)->length))
			{
				buffer_->resizeDataBuffer(typInfo.columnIndex_, totalLen + VARHDRSZ);
			}
			dataBuff *colBuffer = buffer_->getDataBuffer(typInfo.columnIndex_);
			SET_VARSIZE(colBuffer->buffer, totalLen + VARHDRSZ);
			memcpy(VARDATA(colBuffer->buffer), value.ptr, value.len);
			if (pad > 0)
				memset(VARDATA(colBuffer->buffer) + value.len, ' ', pad);
			return PointerGetDatum(colBuffer->buffer);
		}
		case UUIDOID:
		{
			parquet::ByteArray value;
			((parquet::TypedScanner<parquet::ByteArrayType> *)scanner.get())->NextValue(&value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);

			if (!buffer_)
			{
				bytea *result = (bytea *) gpdbPalloc(value.len);
				memcpy(VARDATA(result), value.ptr, 16);
				return PointerGetDatum(result);
			}	
			dataBuff *colBuffer = buffer_->getDataBuffer(typInfo.columnIndex_);
			memcpy(colBuffer->buffer, value.ptr, 16);
			return PointerGetDatum(colBuffer->buffer);
		}
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			/*
			 * Direct conversion from Parquet timestamp to PG timestamp.
			 *
			 * PG stores timestamps as microseconds since PG epoch (2000-01-01).
			 * Parquet stores timestamps since Unix epoch (1970-01-01) in
			 * millis/micros/nanos. The old code divided to seconds then
			 * multiplied back to microseconds, losing sub-second precision.
			 */
			static const int64 UNIX_TO_PG_EPOCH_USECS =
				((int64)(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)) * SECS_PER_DAY * USECS_PER_SEC;

			((parquet::TypedScanner<parquet::Int64Type> *)scanner.get())->NextValue(&d.int64Value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);

			int64 pgTimestamp;
			switch (typInfo.timeUnit_)
			{
				case TIMEUNIT_MILLIS:
					pgTimestamp = d.int64Value * 1000 - UNIX_TO_PG_EPOCH_USECS;
					break;
				case TIMEUNIT_MICROS:
					pgTimestamp = d.int64Value - UNIX_TO_PG_EPOCH_USECS;
					break;
				case TIMEUNIT_NANOS:
					pgTimestamp = d.int64Value / 1000 - UNIX_TO_PG_EPOCH_USECS;
					break;
				default:
					/*
					 * Fallback for files without explicit timestamp LogicalType
					 * (e.g., legacy INT96 or INT64 without annotation).
					 * Default to microseconds — the Iceberg standard.
					 */
					pgTimestamp = d.int64Value - UNIX_TO_PG_EPOCH_USECS;
					break;
			}
			return TimestampGetDatum(pgTimestamp);
		}
		case DATEOID:
		{
			((parquet::TypedScanner<parquet::Int32Type> *)scanner.get())->NextValue(&d.int32Value, &isNull);
			return DateADTGetDatum(d.int32Value + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
		}
		case NUMERICOID:
			return readDecimal(scanner, typInfo, isNull);

		default:
			throw Error("unsupported column type oid: \"%u\"", typInfo.pgTypeId_);
	}

	PG_RETURN_DATUM(0);
}

Datum
ParquetReader::readDecimal(std::shared_ptr<parquet::Scanner> &scanner, const TypeInfo &typInfo, bool &isNull)
{
	ReaderValue d;
	parquet::FixedLenByteArray value;
	int scale = typInfo.scale_;
	char *out_buf = nullptr;

	if (!buffer_)
	{
		out_buf = (char *) gpdbPalloc(NUMERIC_HDRSZ + IntDigitsTraits<__int128>::digits * sizeof(NumericDigit));
	}
	else
	{
		dataBuff *res = buffer_->getDataBuffer(typInfo.columnIndex_);
		out_buf = (char *) res->buffer;
	}

	// iceberg only support int4, int8, fixed-len
	switch (typInfo.fileTypeId_)
	{
		case INT4OID:
		{
			((parquet::TypedScanner<parquet::Int32Type> *)scanner.get())->NextValue(&d.int32Value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);
			int_to_numeric_with_scale(d.int32Value, scale, (Numeric) out_buf);
			return NumericGetDatum(out_buf);
		}
		case INT8OID:
		{
			((parquet::TypedScanner<parquet::Int64Type> *)scanner.get())->NextValue(&d.int64Value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);
			int_to_numeric_with_scale(d.int64Value, scale, (Numeric) out_buf);
			return NumericGetDatum(out_buf);
		}
		default:
		{
			((parquet::TypedScanner<parquet::FLBAType> *)scanner.get())->NextValue(&value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);
			/*
			 * For DECIMAL with precision <= 18, the value fits in int64.
			 * Use native 64-bit division instead of __int128 software division,
			 * which is ~5x faster on aarch64.
			 */
			if (typInfo.precision_ > 0 && typInfo.precision_ <= 18)
				int_to_numeric_with_scale(FLBA_to_int64(value.ptr, typInfo.typeLength_), scale, (Numeric) out_buf);
			else
				int_to_numeric_with_scale(FLBA_to_int128(value.ptr, typInfo.typeLength_), scale, (Numeric) out_buf);
			return NumericGetDatum(out_buf);
		}
	}

	PG_RETURN_DATUM(0);
}

void ParquetReader::decodeRecord() {}

/*
 * Per-type direct read functions.  Called via function pointer from
 * populateRecord(), bypassing virtual dispatch and the type switch.
 * Each covers the hot path for one physical Parquet type; complex types
 * (TEXT, BPCHAR, UUID, TIMESTAMP, NUMERIC) still go through readPrimitive
 * because they need buffer management or multi-step conversion.
 */

Datum
ParquetReader::readBoolColumn(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	bool value;
	((parquet::TypedScanner<parquet::BooleanType> *)self->scanners_[idx].get())->NextValue(&value, &isNull);
	return BoolGetDatum(value);
}

Datum
ParquetReader::readInt16Column(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	int32_t value;
	((parquet::TypedScanner<parquet::Int32Type> *)self->scanners_[idx].get())->NextValue(&value, &isNull);
	return Int16GetDatum((int16) value);
}

Datum
ParquetReader::readInt32Column(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	int32_t value;
	((parquet::TypedScanner<parquet::Int32Type> *)self->scanners_[idx].get())->NextValue(&value, &isNull);
	return Int32GetDatum(value);
}

Datum
ParquetReader::readInt64Column(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	int64_t value;
	((parquet::TypedScanner<parquet::Int64Type> *)self->scanners_[idx].get())->NextValue(&value, &isNull);
	return Int64GetDatum(value);
}

Datum
ParquetReader::readFloat4Column(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	float value;
	((parquet::TypedScanner<parquet::FloatType> *)self->scanners_[idx].get())->NextValue(&value, &isNull);
	return Float4GetDatum(value);
}

Datum
ParquetReader::readFloat8Column(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	double value;
	((parquet::TypedScanner<parquet::DoubleType> *)self->scanners_[idx].get())->NextValue(&value, &isNull);
	return Float8GetDatum(value);
}

Datum
ParquetReader::readDateColumn(BaseFileReader *r, int idx, bool &isNull)
{
	auto *self = static_cast<ParquetReader *>(r);
	int32_t value;
	((parquet::TypedScanner<parquet::Int32Type> *)self->scanners_[idx].get())->NextValue(&value, &isNull);
	return DateADTGetDatum(value + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
}

/*
 * Resolve which direct read function to use for a column.  Returns NULL
 * for complex types that need buffer management or multi-step conversion;
 * those fall back to readPrimitive() via virtual dispatch.
 */
ReadColumnFn
ParquetReader::resolveReadFn(const TypeInfo &typInfo)
{
	switch (typInfo.pgTypeId_)
	{
		case BOOLOID:
			return readBoolColumn;
		case INT2OID:
			return readInt16Column;
		case INT4OID:
			return readInt32Column;
		case TIMEOID:
		case INT8OID:
			return readInt64Column;
		case FLOAT4OID:
			return readFloat4Column;
		case FLOAT8OID:
			return readFloat8Column;
		case DATEOID:
			return readDateColumn;
		default:
			/*
			 * BPCHAR, TEXT, BYTEA, UUID, TIMESTAMP, NUMERIC — these need
			 * buffer_ access or multi-step logic, handled by readPrimitive.
			 */
			return nullptr;
	}
}
