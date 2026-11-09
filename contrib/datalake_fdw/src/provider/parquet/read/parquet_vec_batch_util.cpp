/*-------------------------------------------------------------------------
 *
 * parquet_vec_batch_util.cpp
 *    Shared engine-Arrow batch helpers extracted from parquetReadRecordBatch
 *    (issue #424).  See parquet_vec_batch_util.h.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 *-------------------------------------------------------------------------
 */
#include <parquet/arrow/reader.h>
#include <parquet/internal/arrow/api.h>
#include <parquet/internal/arrow/c/bridge.h>
#include <parquet/types.h>			/* parquet::FixedLenByteArray */

#include "parquet_vec_batch_util.h"
#include "src/provider/common/datalake_numeric.h"	/* FLBA_to_int64 / FLBA_to_int128 */

extern "C" {
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/fmgrprotos.h"
#include "catalog/pg_type.h"
}

namespace Datalake {
namespace Internal {

static void checkVecTypmodRepresentable(int32 typeMod);

/*
 * Copy a narrow fixed-width arrow array into its widened form, nulls kept.
 * Only the widenings Iceberg schema evolution allows use this (int32 -> int64,
 * float -> double), so the conversion is always value-preserving.  #482
 */
template <typename InArray, typename OutBuilder>
static std::shared_ptr<::parquet_arrow::Array>
widenFixedWidth(const std::shared_ptr<::parquet_arrow::Array> &in,
				const std::shared_ptr<::parquet_arrow::DataType> &outType,
				::parquet_arrow::MemoryPool *pool)
{
	auto		src = std::static_pointer_cast<InArray>(in);
	const int64_t n = in->length();
	OutBuilder	b(outType, pool);
	std::shared_ptr<::parquet_arrow::Array> out;

	PARQUET_VEC_CHECK(b.Reserve(n), "widen reserve");

	const bool	has_null = (in->null_count() != 0);

	for (int64_t i = 0; i < n; i++)
	{
		if (has_null && src->IsNull(i))
		{
			b.UnsafeAppendNull();
			continue;
		}
		b.UnsafeAppend(src->Value(i));
	}
	PARQUET_VEC_CHECK(b.Finish(&out), "widen finish");
	return out;
}

/*
 * parquet::arrow's output Arrow type comes from the FILE schema, not the PG
 * column type, so we normalize per column (issue #424 DESIGN R1#1/2/6):
 *   - date32: file value is days since 1970-01-01; the engine reads the raw
 *     date32 as a PG DateADT (days since 2000-01-01) -> subtract 10957.
 *   - int2: the writer stores it as an un-annotated INT32 -> narrow to int16.
 *   - text/varchar: un-annotated BYTE_ARRAY reads back as binary -> re-tag utf8.
 *   - numeric: reads back as decimal128; re-pack into the engine's numeric128
 *     16-byte layout, carried as fixed_size_binary(16) here and re-tagged "D128"
 *     at export time (see parquetVecRetagNumeric128).
 * int4/int8/float4/float8 already match (schema compat validated at open time).
 */
void
normalizeParquetVecColumn(parquet_arrow::MemoryPool *pool, Oid typeOid,
					int32 typeMod,
					const std::shared_ptr<::parquet_arrow::Array> &in,
					std::shared_ptr<::parquet_arrow::Array> &out_arr,
					std::shared_ptr<::parquet_arrow::DataType> &out_type)
{
	int64_t n = in->length();

	switch (typeOid)
	{
		case DATEOID:
		{
			/* parquet::arrow yields the FILE's type; we only handle date32.  A
			 * date64 (or other) file column would over-read the cast, so refuse
			 * it clearly rather than corrupt. */
			if (in->type_id() != ::parquet_arrow::Type::DATE32)
				elog(ERROR, "datalake parquet vectorized read: date column expects "
					 "arrow date32, got %s; run with vectorization off",
					 in->type()->ToString().c_str());
			auto din = std::static_pointer_cast<::parquet_arrow::Date32Array>(in);
			::parquet_arrow::Date32Builder b(pool);
			PARQUET_VEC_CHECK(b.Reserve(n), "date reserve");
			for (int64_t i = 0; i < n; i++)
			{
				if (din->IsNull(i))
					b.UnsafeAppendNull();
				else
					b.UnsafeAppend(din->Value(i) - 10957);
			}
			PARQUET_VEC_CHECK(b.Finish(&out_arr), "date finish");
			out_type = ::parquet_arrow::date32();
			break;
		}
		case INT2OID:
		{
			/*
			 * Our writer widens int2 to an un-annotated INT32 (arrow int32), but
			 * a Spark/Hive file may store it natively as INT16 (arrow int16).
			 * Handle both -- int16 passes through, int32 is narrowed -- and
			 * refuse anything else instead of over-reading an unchecked cast.
			 */
			if (in->type_id() == ::parquet_arrow::Type::INT16)
			{
				out_arr = in;
				out_type = ::parquet_arrow::int16();
				break;
			}
			if (in->type_id() != ::parquet_arrow::Type::INT32)
				elog(ERROR, "datalake parquet vectorized read: int2 column expects "
					 "arrow int16 or int32, got %s; run with vectorization off",
					 in->type()->ToString().c_str());
			auto iin = std::static_pointer_cast<::parquet_arrow::Int32Array>(in);
			::parquet_arrow::Int16Builder b(pool);
			PARQUET_VEC_CHECK(b.Reserve(n), "int2 reserve");
			for (int64_t i = 0; i < n; i++)
			{
				if (iin->IsNull(i))
					b.UnsafeAppendNull();
				else
					b.UnsafeAppend((int16_t) iin->Value(i));
			}
			PARQUET_VEC_CHECK(b.Finish(&out_arr), "int2 finish");
			out_type = ::parquet_arrow::int16();
			break;
		}
		case BOOLOID:
		{
			/* parquet BOOLEAN -> engine boolean, pass through. */
			if (in->type_id() != ::parquet_arrow::Type::BOOL)
				elog(ERROR, "datalake parquet vectorized read: bool column expects "
					 "arrow bool, got %s; run with vectorization off",
					 in->type()->ToString().c_str());
			out_arr = in;
			out_type = ::parquet_arrow::boolean();
			break;
		}
		case TIMESTAMPOID:
		{
			/*
			 * The engine carries a PG timestamp as arrow timestamp[us] whose
			 * int64 value IS the PG internal value (microseconds since
			 * 2000-01-01), the same way DATE is carried as the raw PG DateADT.
			 * A spec-compliant parquet TIMESTAMP is microseconds/... since the
			 * Unix epoch (1970-01-01), so convert the file unit to micros and
			 * shift the epoch by 946684800000000us (= 10957 days, the same gap
			 * the date path applies).  timestamptz is intentionally unsupported
			 * (not in PGTypeToArrowID).
			 */
			if (in->type_id() != ::parquet_arrow::Type::TIMESTAMP)
				elog(ERROR, "datalake parquet vectorized read: timestamp column expects "
					 "arrow timestamp, got %s; run with vectorization off",
					 in->type()->ToString().c_str());
			auto tt = std::static_pointer_cast<::parquet_arrow::TimestampType>(in->type());
			auto tin = std::static_pointer_cast<::parquet_arrow::TimestampArray>(in);
			const int64_t PG_EPOCH_US = INT64CONST(946684800000000); /* 1970->2000 */
			::parquet_arrow::TimestampBuilder b(
				::parquet_arrow::timestamp(::parquet_arrow::TimeUnit::MICRO), pool);
			PARQUET_VEC_CHECK(b.Reserve(n), "timestamp reserve");
			for (int64_t i = 0; i < n; i++)
			{
				if (tin->IsNull(i))
				{
					b.UnsafeAppendNull();
					continue;
				}
				int64_t v = tin->Value(i);
				switch (tt->unit())
				{
					case ::parquet_arrow::TimeUnit::SECOND: v *= INT64CONST(1000000); break;
					case ::parquet_arrow::TimeUnit::MILLI:  v *= INT64CONST(1000);    break;
					case ::parquet_arrow::TimeUnit::MICRO:                            break;
					case ::parquet_arrow::TimeUnit::NANO:   v /= INT64CONST(1000);    break;
				}
				b.UnsafeAppend(v - PG_EPOCH_US);
			}
			PARQUET_VEC_CHECK(b.Finish(&out_arr), "timestamp finish");
			out_type = ::parquet_arrow::timestamp(::parquet_arrow::TimeUnit::MICRO);
			break;
		}
		case BPCHAROID:
			/*
			 * CHAR(N) keeps its blanks.  The zero-copy re-tag below hands the
			 * file's bytes straight to the engine, and Parquet holds a CHAR(N)
			 * with its trailing spaces stripped, so 'ab' reached the engine
			 * where a heap table holds 'ab      '.  The row readers have padded
			 * since #321; this one has to build a new array to do it, which
			 * costs the re-tag -- but only for CHAR(N) columns, and only when
			 * the column declares a length (typeMod < VARHDRSZ is an unbounded
			 * `char`, never padded).
			 *
			 * Characters are counted as UTF-8 code points, not bytes, and not
			 * with pg_mbstrlen_with_len(): the bytes on disk are UTF-8 whatever
			 * the database encoding is.
			 */
			if (typeMod >= (int32) VARHDRSZ &&
				(in->type_id() == ::parquet_arrow::Type::BINARY ||
				 in->type_id() == ::parquet_arrow::Type::STRING))
			{
				const int maxlen = typeMod - VARHDRSZ;
				auto bin = std::static_pointer_cast<::parquet_arrow::BinaryArray>(in);
				::parquet_arrow::StringBuilder b(pool);

				PARQUET_VEC_CHECK(b.Reserve(n), "bpchar reserve");
				for (int64_t i = 0; i < n; i++)
				{
					if (bin->IsNull(i))
					{
						PARQUET_VEC_CHECK(b.AppendNull(), "bpchar null");
						continue;
					}

					std::string_view v = bin->GetView(i);
					int charlen = 0;

					for (size_t k = 0; k < v.size(); k++)
						if (((unsigned char) v[k] & 0xC0) != 0x80)
							charlen++;

					if (charlen < maxlen)
					{
						std::string padded(v.data(), v.size());

						padded.append((size_t) (maxlen - charlen), ' ');
						PARQUET_VEC_CHECK(b.Append(padded), "bpchar append padded");
					}
					else
						PARQUET_VEC_CHECK(b.Append(v.data(), (int32_t) v.size()),
										  "bpchar append");
				}
				PARQUET_VEC_CHECK(b.Finish(&out_arr), "bpchar finish");
				out_type = ::parquet_arrow::utf8();
				break;
			}
			/* unbounded char, or a type the text leg refuses below */
			/* FALLTHROUGH */
		case TEXTOID:
		case VARCHAROID:
		{
			if (in->type_id() == ::parquet_arrow::Type::BINARY)
			{
				/*
				 * binary and utf8 (32-bit offsets) have an identical physical
				 * layout -- validity bitmap + int32 offsets + data bytes -- so
				 * re-tag zero-copy by reusing the input's buffers under a utf8
				 * type instead of copying every string through a StringBuilder
				 * (that per-element copy was a large slice of the scan CPU).
				 * This keeps the old "bytes as-is, not UTF-8 validated" behavior
				 * the per-element Append() path had, and preserves any slice
				 * offset the input array carries.
				 */
				const std::shared_ptr<::parquet_arrow::ArrayData> &ad = in->data();
				auto utf8_ad = ::parquet_arrow::ArrayData::Make(
					::parquet_arrow::utf8(), ad->length, ad->buffers,
					ad->null_count, ad->offset);
				out_arr = ::parquet_arrow::MakeArray(utf8_ad);
				out_type = ::parquet_arrow::utf8();
			}
			else if (in->type_id() == ::parquet_arrow::Type::STRING)
			{
				/* already utf8 (32-bit offsets) -- pass through unchanged. */
				out_arr = in;
				out_type = in->type();
			}
			else
			{
				/*
				 * Refuse anything else.  large_utf8 (LARGE_STRING) is deliberately
				 * NOT passed through: it exports as the C-ABI format "U" (64-bit
				 * offsets) whereas the engine's text path -- like the ORC vec
				 * reader -- only handles utf8 "u" (32-bit offsets), so passing it
				 * through would make the engine read offsets at the wrong width.
				 * checkDataTypeCompatible also permits FIXED_LEN_BYTE_ARRAY, which
				 * maps to fixed_size_binary(N) (not a valid utf8 payload, and a
				 * 16-wide one must not be confused with numeric128).
				 */
				elog(ERROR, "datalake parquet vectorized read: text/varchar column "
					 "expects arrow utf8 or binary, got %s; run with "
					 "vectorization off", in->type()->ToString().c_str());
			}
			break;
		}
		case NUMERICOID:
		{
			/*
			 * The engine numeric128 bridge only handles a parquet DECIMAL column
			 * that parquet::arrow maps to decimal128, and only within the 120-bit
			 * value budget the scale header leaves (precision <= 35, non-negative
			 * scale).  parquet::arrow's type comes from the FILE, not the PG
			 * column, so validate it here rather than trusting the planner's
			 * PG-column precision guard: a numeric column physically stored as a
			 * plain int/binary reads back as int32/int64/binary (not decimal128),
			 * and a DECIMAL file column may declare precision up to 38 even when
			 * the PG column is numeric(<=35).  Either case would over-read the
			 * buffer or overflow encodeNumeric128, so refuse it with a clear error
			 * (the row engine reads such files -- run with vectorization off).
			 */
			if (in->type_id() != ::parquet_arrow::Type::DECIMAL128)
				elog(ERROR, "datalake parquet vectorized read: numeric column expects a "
					 "parquet DECIMAL (decimal128), got arrow type %s; run with "
					 "vectorization off", in->type()->ToString().c_str());
			auto dtype = std::static_pointer_cast<::parquet_arrow::Decimal128Type>(in->type());
			int32_t precision = dtype->precision();
			int32_t scale = dtype->scale();
			if (precision > 35 || scale < 0 || scale > precision)
				elog(ERROR, "datalake parquet vectorized read: numeric decimal128(%d,%d) "
					 "outside supported range (precision <= 35, 0 <= scale <= precision); "
					 "run with vectorization off", precision, scale);
			/*
			 * The declared numeric(p,s) is part of the answer, not a hint:
			 * restate each value in the declared scale and refuse what the
			 * declared precision cannot hold (#487).
			 */
			const bool applyTypmod = numericTypmodApplies(typeMod, precision,
														  scale);
			int32_t outScale = scale;

			if (applyTypmod)
			{
				checkVecTypmodRepresentable(typeMod);
				outScale = DL_NUMERIC_TYPMOD_SCALE(typeMod);
			}
			auto din = std::static_pointer_cast<::parquet_arrow::Decimal128Array>(in);
			::parquet_arrow::FixedSizeBinaryBuilder b(::parquet_arrow::fixed_size_binary(16), pool);
			PARQUET_VEC_CHECK(b.Reserve(n), "numeric reserve");
			/*
			 * Hot path: after Reserve(n) use UnsafeAppend -- the checked
			 * Append()/AppendNull() each build and destroy an arrow::Status and
			 * re-check capacity per value, which perf showed to be a large part
			 * of the scan (Status::~Status alone ~5%).  Also hoist the null test:
			 * when the column has no nulls (the common case) skip the per-element
			 * IsNull() bitmap probe entirely.
			 */
			const bool has_null = (din->null_count() != 0);
			for (int64_t i = 0; i < n; i++)
			{
				if (has_null && din->IsNull(i))
				{
					b.UnsafeAppendNull();
					continue;
				}
				/* arrow stores each decimal128 value as a little-endian 16-byte
				 * two's-complement int (bytes[0..7]=low, [8..15]=high on this
				 * little-endian target). */
				const uint8_t *p = din->GetValue(i);
				uint64_t low;
				int64_t  high;
				memcpy(&low, p, sizeof(low));
				memcpy(&high, p + sizeof(low), sizeof(high));
				uint8_t enc[16];
				if (applyTypmod)
				{
					__int128 v = numericRescaleUnscaled(
						(((__int128) high) << 64) | low, scale, typeMod);

					low = (uint64_t) v;
					high = (int64_t) (v >> 64);
				}
				encodeNumeric128(low, high, outScale, enc);
				b.UnsafeAppend(enc);
			}
			PARQUET_VEC_CHECK(b.Finish(&out_arr), "numeric finish");
			out_type = ::parquet_arrow::fixed_size_binary(16);
			break;
		}
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		{
			/*
			 * These pass through unchanged, but parquet::arrow reports the FILE's
			 * type: checkDataTypeCompatible accepts INT32/INT64/INT96 for the int
			 * family and FLOAT/DOUBLE for the float family, so an int4 stored as
			 * INT64 (or a float4 stored as DOUBLE) would hand the engine a
			 * wrong-width column for the slot.  Require the exact arrow width and
			 * refuse a mismatch instead of silently corrupting.
			 */
			::parquet_arrow::Type::type want =
				(typeOid == INT4OID)   ? ::parquet_arrow::Type::INT32 :
				(typeOid == INT8OID)   ? ::parquet_arrow::Type::INT64 :
				(typeOid == FLOAT4OID) ? ::parquet_arrow::Type::FLOAT :
										 ::parquet_arrow::Type::DOUBLE;
			if (in->type_id() != want)
			{
				/*
				 * Iceberg schema evolution promotes a column in place: int ->
				 * long and float -> double leave the old data files alone, so a
				 * file written before the ALTER still holds the narrow physical
				 * type while the PG column is now the wide one.  Widen here, as
				 * the row reader does (#401), rather than refusing the file.
				 * Only these two widenings: anything else -- notably a
				 * NARROWING, an int4 column over a file that holds INT64 --
				 * would lose value bits, and would hand the engine a column
				 * whose width does not match the slot it reads it into.
				 * #482
				 */
				if (typeOid == INT8OID &&
					in->type_id() == ::parquet_arrow::Type::INT32)
					out_arr = widenFixedWidth<::parquet_arrow::Int32Array,
											  ::parquet_arrow::Int64Builder>(
								  in, ::parquet_arrow::int64(), pool);
				else if (typeOid == FLOAT8OID &&
						 in->type_id() == ::parquet_arrow::Type::FLOAT)
					out_arr = widenFixedWidth<::parquet_arrow::FloatArray,
											  ::parquet_arrow::DoubleBuilder>(
								  in, ::parquet_arrow::float64(), pool);
				else
					elog(ERROR, "datalake parquet vectorized read: column type %u expects a "
						 "matching fixed-width arrow type, got %s; run with vectorization off",
						 typeOid, in->type()->ToString().c_str());
				out_type = out_arr->type();
				break;
			}
			out_arr = in;
			out_type = in->type();
			break;
		}
		default:
			/* Unreachable: the planner whitelist (is_parquet_t1_type) restricts
			 * scan column types to the cases above. */
			elog(ERROR, "datalake parquet vectorized read: unsupported column type %u",
				 typeOid);
			break;
	}
}

/*
 * The declared numeric(p,s) has to be one the engine's numeric128 can carry:
 * its scale sits in the top 8 bits of the high word, leaving 120 signed bits
 * for the value (see encodeNumeric128).  numericTypmodRescalesInPlace() keeps
 * every converted value under 10^35 < 2^119, which is exactly that budget --
 * the same reason the file-precision guards next to the call sites exist.
 * Anything wider has to be read by the row engine, so say so rather than
 * encode a value that would silently lose its top bits (#487).
 */
static void
checkVecTypmodRepresentable(int32 typeMod)
{
	if (!numericTypmodRescalesInPlace(typeMod))
		elog(ERROR, "datalake parquet vectorized read: declared numeric(%d,%d) "
			 "is outside the vectorized reader's range (precision 1..%d, "
			 "scale 0..%d); run with vectorization off",
			 DL_NUMERIC_TYPMOD_PRECISION(typeMod),
			 DL_NUMERIC_TYPMOD_SCALE(typeMod),
			 DL_NUMERIC_INPLACE_MAX_PRECISION,
			 DL_NUMERIC_INPLACE_MAX_PRECISION);
}

void
appendNumeric128FromFLBA(::parquet_arrow::FixedSizeBinaryBuilder &b,
					const ::parquet::FixedLenByteArray *vals,
					const int16_t *defs, int16_t maxDef, int64_t levels,
					int typeLength, int precision, int scale, int32 typeMod)
{
	/* Declared numeric(p,s) that the file does not already satisfy (#487). */
	const bool	applyTypmod = numericTypmodApplies(typeMod, precision, scale);
	int32_t		outScale = scale;

	if (applyTypmod)
	{
		checkVecTypmodRepresentable(typeMod);
		outScale = DL_NUMERIC_TYPMOD_SCALE(typeMod);
	}

	/* precision<=18 always fits in int64 even when stored in a wider FLBA, so
	 * take the cheap single-bswap64 path (mirrors the row reader). */
	const bool fits64 = (precision > 0 && precision <= 18);
	int64_t vi = 0;					/* dense (non-null) value cursor */
	uint8_t enc[16];
	for (int64_t i = 0; i < levels; i++)
	{
		if (defs != nullptr && defs[i] < maxDef)
		{
			b.UnsafeAppendNull();
			continue;
		}
		const uint8_t *p = vals[vi++].ptr;	/* big-endian two's-complement */
		if (fits64 && !applyTypmod)
		{
			int64_t v = FLBA_to_int64(p, typeLength);
			encodeNumeric128((uint64_t) v, (v < 0 ? INT64CONST(-1) : INT64CONST(0)),
							 scale, enc);
		}
		else
		{
			__int128 v = fits64 ? (__int128) FLBA_to_int64(p, typeLength)
								: FLBA_to_int128(p, typeLength);

			if (applyTypmod)
				v = numericRescaleUnscaled(v, scale, typeMod);
			encodeNumeric128((uint64_t) v, (int64_t) (v >> 64), outScale, enc);
		}
		b.UnsafeAppend(enc);
	}
}

void
parquetVecAddColOrCreate(std::shared_ptr<::parquet_arrow::RecordBatch> &out,
					const std::shared_ptr<::parquet_arrow::Field> &field_id,
					const std::shared_ptr<::parquet_arrow::Array> &column,
					int64_t nrows, int batch_index)
{
	if (!out || out->num_columns() == 0)
	{
		::parquet_arrow::FieldVector fields = {field_id};
		::parquet_arrow::ArrayVector arrays = {column};
		auto schema = std::make_shared<::parquet_arrow::Schema>(
						fields, std::make_shared<::parquet_arrow::KeyValueMetadata>());
		out = ::parquet_arrow::RecordBatch::Make(schema, nrows, arrays);
	}
	else
	{
		auto res = out->AddColumn(batch_index, field_id, column);
		PARQUET_VEC_CHECK(res.status(), "add column");
		out = res.ValueOrDie();
	}
}

/*
 * Constant column for a partition key (issue #424 DESIGN R1#7 adds the DATE /
 * VARCHAR branches the ORC helper lacked).  The partition Datum is already a PG
 * value, so a DATE constant is already a PG DateADT and must NOT be
 * epoch-shifted (unlike the file's data date columns).
 */
void
parquetVecAddConstColumn(parquet_arrow::MemoryPool *pool,
					std::shared_ptr<::parquet_arrow::RecordBatch> &out,
					std::vector<int> &numericColIndices, TupleDesc tupdesc,
					int mpp_index, int batch_index, const std::string &partitionkey,
					Datum mpp_datum, int64_t nrows)
{
	Oid typeOid = tupdesc->attrs[mpp_index].atttypid;

	switch (typeOid)
	{
		case INT2OID:
		{
			int16_t v = DatumGetInt16(mpp_datum);
			::parquet_arrow::Int16Builder b(pool);
			PARQUET_VEC_CHECK(b.Reserve(nrows), "part int2 reserve");
			for (int64_t i = 0; i < nrows; i++)
				b.UnsafeAppend(v);
			std::shared_ptr<::parquet_arrow::Array> col;
			PARQUET_VEC_CHECK(b.Finish(&col), "part int2 finish");
			parquetVecAddColOrCreate(out, ::parquet_arrow::field(partitionkey, ::parquet_arrow::int16()),
									  col, nrows, batch_index);
			break;
		}
		case INT4OID:
		{
			int32_t v = DatumGetInt32(mpp_datum);
			::parquet_arrow::Int32Builder b(pool);
			PARQUET_VEC_CHECK(b.Reserve(nrows), "part int4 reserve");
			for (int64_t i = 0; i < nrows; i++)
				b.UnsafeAppend(v);
			std::shared_ptr<::parquet_arrow::Array> col;
			PARQUET_VEC_CHECK(b.Finish(&col), "part int4 finish");
			parquetVecAddColOrCreate(out, ::parquet_arrow::field(partitionkey, ::parquet_arrow::int32()),
									  col, nrows, batch_index);
			break;
		}
		case INT8OID:
		{
			int64_t v = DatumGetInt64(mpp_datum);
			::parquet_arrow::Int64Builder b(pool);
			PARQUET_VEC_CHECK(b.Reserve(nrows), "part int8 reserve");
			for (int64_t i = 0; i < nrows; i++)
				b.UnsafeAppend(v);
			std::shared_ptr<::parquet_arrow::Array> col;
			PARQUET_VEC_CHECK(b.Finish(&col), "part int8 finish");
			parquetVecAddColOrCreate(out, ::parquet_arrow::field(partitionkey, ::parquet_arrow::int64()),
									  col, nrows, batch_index);
			break;
		}
		case FLOAT4OID:
		{
			float v = DatumGetFloat4(mpp_datum);
			::parquet_arrow::FloatBuilder b(pool);
			PARQUET_VEC_CHECK(b.Reserve(nrows), "part float4 reserve");
			for (int64_t i = 0; i < nrows; i++)
				b.UnsafeAppend(v);
			std::shared_ptr<::parquet_arrow::Array> col;
			PARQUET_VEC_CHECK(b.Finish(&col), "part float4 finish");
			parquetVecAddColOrCreate(out, ::parquet_arrow::field(partitionkey, ::parquet_arrow::float32()),
									  col, nrows, batch_index);
			break;
		}
		case FLOAT8OID:
		{
			double v = DatumGetFloat8(mpp_datum);
			::parquet_arrow::DoubleBuilder b(pool);
			PARQUET_VEC_CHECK(b.Reserve(nrows), "part float8 reserve");
			for (int64_t i = 0; i < nrows; i++)
				b.UnsafeAppend(v);
			std::shared_ptr<::parquet_arrow::Array> col;
			PARQUET_VEC_CHECK(b.Finish(&col), "part float8 finish");
			parquetVecAddColOrCreate(out, ::parquet_arrow::field(partitionkey, ::parquet_arrow::float64()),
									  col, nrows, batch_index);
			break;
		}
		case DATEOID:
		{
			/* already a PG DateADT -- do NOT epoch-shift (unlike data columns). */
			int32_t v = (int32_t) DatumGetDateADT(mpp_datum);
			::parquet_arrow::Date32Builder b(pool);
			PARQUET_VEC_CHECK(b.Reserve(nrows), "part date reserve");
			for (int64_t i = 0; i < nrows; i++)
				b.UnsafeAppend(v);
			std::shared_ptr<::parquet_arrow::Array> col;
			PARQUET_VEC_CHECK(b.Finish(&col), "part date finish");
			parquetVecAddColOrCreate(out, ::parquet_arrow::field(partitionkey, ::parquet_arrow::date32()),
									  col, nrows, batch_index);
			break;
		}
		case NUMERICOID:
		{
			/* Parse the constant into a decimal128 (unscaled int128 + scale) via
			 * arrow's parser, then re-pack into the engine numeric128 layout --
			 * the same encoding the data-column path applies to file values. */
			char *s = DatumGetCString(DirectFunctionCall1(numeric_out, mpp_datum));
			::parquet_arrow::Decimal128 dec;
			int32_t prec = 0, scl = 0;
			auto st = ::parquet_arrow::Decimal128::FromString(std::string(s), &dec, &prec, &scl);
			pfree(s);
			PARQUET_VEC_CHECK(st, "part numeric parse");
			uint8_t enc[16];
			encodeNumeric128(dec.low_bits(), dec.high_bits(), scl, enc);
			::parquet_arrow::FixedSizeBinaryBuilder b(::parquet_arrow::fixed_size_binary(16), pool);
			PARQUET_VEC_CHECK(b.Reserve(nrows), "part numeric reserve");
			for (int64_t i = 0; i < nrows; i++)
				PARQUET_VEC_CHECK(b.Append(enc), "part numeric append");
			std::shared_ptr<::parquet_arrow::Array> col;
			PARQUET_VEC_CHECK(b.Finish(&col), "part numeric finish");
			parquetVecAddColOrCreate(out, ::parquet_arrow::field(partitionkey, ::parquet_arrow::fixed_size_binary(16)),
									  col, nrows, batch_index);
			numericColIndices.push_back(batch_index);
			break;
		}
		case BOOLOID:
		{
			bool v = DatumGetBool(mpp_datum);
			::parquet_arrow::BooleanBuilder b(pool);
			PARQUET_VEC_CHECK(b.Reserve(nrows), "part bool reserve");
			for (int64_t i = 0; i < nrows; i++)
				b.UnsafeAppend(v);
			std::shared_ptr<::parquet_arrow::Array> col;
			PARQUET_VEC_CHECK(b.Finish(&col), "part bool finish");
			parquetVecAddColOrCreate(out, ::parquet_arrow::field(partitionkey, ::parquet_arrow::boolean()),
									  col, nrows, batch_index);
			break;
		}
		case TIMESTAMPOID:
		{
			/* already a PG Timestamp (us since 2000) -- do NOT epoch-shift. */
			int64_t v = (int64_t) DatumGetTimestamp(mpp_datum);
			::parquet_arrow::TimestampBuilder b(
				::parquet_arrow::timestamp(::parquet_arrow::TimeUnit::MICRO), pool);
			PARQUET_VEC_CHECK(b.Reserve(nrows), "part timestamp reserve");
			for (int64_t i = 0; i < nrows; i++)
				b.UnsafeAppend(v);
			std::shared_ptr<::parquet_arrow::Array> col;
			PARQUET_VEC_CHECK(b.Finish(&col), "part timestamp finish");
			parquetVecAddColOrCreate(out, ::parquet_arrow::field(partitionkey,
									  ::parquet_arrow::timestamp(::parquet_arrow::TimeUnit::MICRO)),
									  col, nrows, batch_index);
			break;
		}
		case BPCHAROID:
		case TEXTOID:
		case VARCHAROID:
		{
			char *s = (typeOid == VARCHAROID)
						? DatumGetCString(DirectFunctionCall1(varcharout, mpp_datum))
						: (typeOid == BPCHAROID)
						? DatumGetCString(DirectFunctionCall1(bpcharout, mpp_datum))
						: DatumGetCString(DirectFunctionCall1(textout, mpp_datum));
			int len = (int) strlen(s);
			::parquet_arrow::StringBuilder b(pool);
			PARQUET_VEC_CHECK(b.Reserve(nrows), "part text reserve");
			for (int64_t i = 0; i < nrows; i++)
				PARQUET_VEC_CHECK(b.Append(s, len), "part text append");
			pfree(s);
			std::shared_ptr<::parquet_arrow::Array> col;
			PARQUET_VEC_CHECK(b.Finish(&col), "part text finish");
			parquetVecAddColOrCreate(out, ::parquet_arrow::field(partitionkey, ::parquet_arrow::utf8()),
									  col, nrows, batch_index);
			break;
		}
		default:
			elog(ERROR, "datalake parquet vectorized: unsupported partition key "
				 "column \"%s\" type %u", partitionkey.c_str(), typeOid);
	}
}

/*
 * Trailing NULL column for a used table column absent from the file.  Only the
 * trailing (schema-evolution) case is in T1 scope (issue #424 DESIGN R1#5).
 */
void
parquetVecAddNullColumn(parquet_arrow::MemoryPool *pool,
					std::shared_ptr<::parquet_arrow::RecordBatch> &out,
					std::vector<int> &numericColIndices, TupleDesc tupdesc,
					int mpp_index, int batch_index, const std::string &field_name,
					int64_t nrows)
{
	Oid typeOid = tupdesc->attrs[mpp_index].atttypid;
	std::shared_ptr<::parquet_arrow::DataType> dt;

	switch (typeOid)
	{
		case INT2OID:	dt = ::parquet_arrow::int16();	break;
		case INT4OID:	dt = ::parquet_arrow::int32();	break;
		case INT8OID:	dt = ::parquet_arrow::int64();	break;
		case FLOAT4OID:	dt = ::parquet_arrow::float32();	break;
		case FLOAT8OID:	dt = ::parquet_arrow::float64();	break;
		case DATEOID:	dt = ::parquet_arrow::date32();	break;
		case NUMERICOID:	dt = ::parquet_arrow::fixed_size_binary(16);	break;
		case BOOLOID:		dt = ::parquet_arrow::boolean();	break;
		case TIMESTAMPOID:	dt = ::parquet_arrow::timestamp(::parquet_arrow::TimeUnit::MICRO);	break;
		case BPCHAROID:
		case TEXTOID:
		case VARCHAROID:	dt = ::parquet_arrow::utf8();	break;
		default:
			elog(ERROR, "datalake parquet vectorized: unsupported column \"%s\" "
				 "type %u for NULL fill", field_name.c_str(), typeOid);
	}

	std::unique_ptr<::parquet_arrow::ArrayBuilder> builder;
	PARQUET_VEC_CHECK(::parquet_arrow::MakeBuilder(pool, dt, &builder), "make null builder");
	PARQUET_VEC_CHECK(builder->AppendNulls(nrows), "append nulls");
	std::shared_ptr<::parquet_arrow::Array> col;
	PARQUET_VEC_CHECK(builder->Finish(&col), "finish null column");

	parquetVecAddColOrCreate(out, ::parquet_arrow::field(field_name, dt), col, nrows, batch_index);
	if (typeOid == NUMERICOID)
		numericColIndices.push_back(batch_index);
}

/*
 * Two-arrows numeric bridge: numeric columns were re-encoded into the engine's
 * numeric128 byte layout but carried as fixed_size_binary(16), which arrow-19
 * exports with the C-ABI format "w:16".  The engine's fork bridge tags
 * numeric128 as "D128" and imports "D" back to numeric128() -- a private format
 * char arrow-19 cannot emit.  Re-tag exactly the columns we numeric-encoded
 * (tracked in numericColIndices as they were added) so the engine reconstructs
 * numeric128.  We key off the recorded indices rather than the "w:16" format
 * string so a future 16-byte fixed-binary T1 type can never be mistaken for
 * numeric.  The "D128" literal outlives the engine's synchronous import, and
 * arrow-19's release frees its own private_data, not this borrowed format
 * pointer.
 */
void
parquetVecRetagNumeric128(struct ArrowSchema *schema,
					const std::vector<int> &numericColIndices)
{
	for (int idx : numericColIndices)
	{
		if (idx >= 0 && idx < schema->n_children)
		{
			struct ArrowSchema *child = schema->children[idx];

			/*
			 * Verify the format in every build, not only under Assert.  Assert
			 * compiles out with NDEBUG, and the old `if (child)` guard only
			 * prevented a NULL dereference: a stale or wrong index in
			 * numericColIndices would re-tag a column that is not
			 * fixed_size_binary(16), and the engine would then decode that
			 * column's raw bytes as numeric128 -- silent data corruption in
			 * release builds.  Skipping a mismatch is always safe: the column
			 * simply keeps the type arrow-19 exported.
			 */
			if (child && child->format && strcmp(child->format, "w:16") == 0)
				child->format = "D128";
		}
	}
}

}
}
