/*-------------------------------------------------------------------------
 *
 * parquet_scanner_guard.h
 *	  Checked casts from parquet::Scanner / parquet::ColumnReader to the
 *	  typed subclass a decoding leg expects.
 *
 * Both parquet read paths pick their decoder from the type the foreign table
 * declares and reach it through a cast that asserts a physical type rather
 * than checking one.  A TypedScanner<Int64Type> built over an INT32 column
 * still answers NextValue(int64_t *) and takes eight bytes out of a
 * four-byte buffer; the ByteArray legs are worse, since they go on to memcpy
 * through a {ptr, len} pair rebuilt from whatever those bytes happened to be.
 *
 * The helpers below check the cast instead of asserting it.  The template
 * argument is the leg's own statement of what it can decode, so this is not
 * a second source of truth: there is no table of (declared type, physical
 * type) pairs to drift out of step with the legs it describes.  A leg that
 * reads several physical forms keeps its own switch and names each form at
 * its own cast.
 *
 * This refuses a mismatch; it does not widen one.  Teaching a leg to decode
 * a further physical type is a change to that leg, and shows up here only as
 * a new cast.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARQUET_SCANNER_GUARD_H
#define PARQUET_SCANNER_GUARD_H

#include <memory>
#include <parquet/column_reader.h>
#include <parquet/column_scanner.h>
#include <parquet/types.h>

extern "C"
{
#include "postgres.h"
#include "utils/builtins.h"
}

/*
 * Shared by both helpers: the column is bound and its physical type is the
 * one this leg decodes, or we raise.
 */
static inline void
dlCheckPhysicalType(const parquet::ColumnDescriptor *descr,
					::parquet::Type::type wanted,
					Oid typeOid,
					const char *fileName)
{
	if (descr == nullptr)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("no parquet column is bound for a column of type %s",
						format_type_be(typeOid)),
				 errdetail("Parquet file \"%s\".",
						   fileName != nullptr ? fileName : "?")));

	if (descr->physical_type() == wanted)
		return;

	ereport(ERROR,
			(errcode(ERRCODE_DATATYPE_MISMATCH),
			 errmsg("column \"%s\" cannot be read as type %s",
					descr->name().c_str(), format_type_be(typeOid)),
			 errdetail("Parquet file \"%s\" stores it as physical type %s, "
					   "which this reader decodes as %s.",
					   fileName != nullptr ? fileName : "?",
					   ::parquet::TypeToString(descr->physical_type()).c_str(),
					   ::parquet::TypeToString(wanted).c_str()),
			 errhint("Declare the column with a type matching the file.")));
}

/*
 * Per-value path: parquet::Scanner -> parquet::TypedScanner<DType>.
 */
template <typename DType>
inline parquet::TypedScanner<DType> *
dlCheckedScanner(const std::shared_ptr<parquet::Scanner> &scanner,
				 Oid typeOid,
				 const char *fileName)
{
	if (scanner == nullptr)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("no parquet scanner is bound for a column of type %s",
						format_type_be(typeOid)),
				 errdetail("Parquet file \"%s\".",
						   fileName != nullptr ? fileName : "?")));

	dlCheckPhysicalType(scanner->descr(), DType::type_num, typeOid, fileName);
	return static_cast<parquet::TypedScanner<DType> *>(scanner.get());
}

/*
 * Same, for a caller that already holds the descriptor.  Saves the virtual
 * ColumnReader::descr() this would otherwise make once per value, and makes
 * a column with no descriptor bound an error rather than a silent pass.
 */
template <typename DType>
inline parquet::TypedScanner<DType> *
dlCheckedScanner(const std::shared_ptr<parquet::Scanner> &scanner,
				 const parquet::ColumnDescriptor *descr,
				 Oid typeOid,
				 const char *fileName)
{
	if (scanner == nullptr)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("no parquet scanner is bound for a column of type %s",
						format_type_be(typeOid)),
				 errdetail("Parquet file \"%s\".",
						   fileName != nullptr ? fileName : "?")));

	dlCheckPhysicalType(descr, DType::type_num, typeOid, fileName);
	return static_cast<parquet::TypedScanner<DType> *>(scanner.get());
}

/*
 * Batch path: parquet::ColumnReader -> parquet::TypedColumnReader<DType>.
 */
template <typename DType>
inline parquet::TypedColumnReader<DType> *
dlCheckedReader(const std::shared_ptr<parquet::ColumnReader> &reader,
				Oid typeOid,
				const char *fileName)
{
	if (reader == nullptr)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("no parquet column reader is bound for a column of type %s",
						format_type_be(typeOid)),
				 errdetail("Parquet file \"%s\".",
						   fileName != nullptr ? fileName : "?")));

	dlCheckPhysicalType(reader->descr(), DType::type_num, typeOid, fileName);
	return static_cast<parquet::TypedColumnReader<DType> *>(reader.get());
}

#endif							/* PARQUET_SCANNER_GUARD_H */
