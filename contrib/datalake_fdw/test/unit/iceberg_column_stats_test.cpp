/*-------------------------------------------------------------------------
 *
 * iceberg_column_stats_test.cpp
 *    Tests for the write-time column statistics collector (issue #442).
 *
 * Each case writes a real parquet file with the schema shapes the datalake
 * writer emits, then asserts on the wire-format v1 JSON the collector reads
 * back out of that file's footer.  What is being checked is not that the JSON
 * is well formed but that it says what the agent's own footer reader would
 * have concluded -- and, where the two must differ, that it errs by widening
 * a bound and never by narrowing one.
 *
 * Builds and runs without PostgreSQL or a cluster:
 *
 *     make -C contrib/datalake_fdw/test/unit check
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/test/unit/iceberg_column_stats_test.cpp
 *-------------------------------------------------------------------------
 */
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/internal/arrow/io/file.h>
#include <parquet/internal/arrow/io/memory.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include "parquetColumnStats.h"

using Datalake::Internal::buildIcebergColumnStatsJson;
using parquet::ConvertedType;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::NodePtr;
using parquet::schema::NodeVector;
using parquet::schema::PrimitiveNode;

static int	failures = 0;
static int	checks = 0;

static void
check(bool ok, const std::string &what, const std::string &detail)
{
	checks++;
	if (ok)
		return;
	failures++;
	std::fprintf(stderr, "FAIL: %s\n      %s\n", what.c_str(), detail.c_str());
}

static void
checkContains(const std::string &haystack, const std::string &needle,
			  const std::string &what)
{
	check(haystack.find(needle) != std::string::npos, what,
		  "expected to find " + needle + "\n      in " + haystack);
}

static void
checkMissing(const std::string &haystack, const std::string &needle,
			 const std::string &what)
{
	check(haystack.find(needle) == std::string::npos, what,
		  "expected NOT to find " + needle + "\n      in " + haystack);
}

/*
 * The slice of the JSON describing one column, so a case can assert about
 * that column without matching text that belongs to its neighbours.
 */
static std::string
column(const std::string &json, int fieldId)
{
	std::string	key = "{\"id\":" + std::to_string(fieldId) + ",";
	size_t		start = json.find(key);
	size_t		depth = 0;

	if (start == std::string::npos)
		return std::string();

	for (size_t i = start; i < json.size(); i++)
	{
		if (json[i] == '{')
			depth++;
		else if (json[i] == '}' && --depth == 0)
			return json.substr(start, i - start + 1);
	}
	return std::string();
}

/* Base64 of a literal byte string, so a case can spell out exact bound bytes. */
static std::string
b64(const std::string &raw)
{
	static const char *tbl =
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	std::string	out;

	for (size_t i = 0; i < raw.size(); i += 3)
	{
		unsigned	n = (unsigned char) raw[i] << 16;
		size_t		have = 1;

		if (i + 1 < raw.size())
		{
			n |= (unsigned) (unsigned char) raw[i + 1] << 8;
			have++;
		}
		if (i + 2 < raw.size())
		{
			n |= (unsigned) (unsigned char) raw[i + 2];
			have++;
		}

		out += tbl[(n >> 18) & 0x3f];
		out += tbl[(n >> 12) & 0x3f];
		out += have > 1 ? tbl[(n >> 6) & 0x3f] : '=';
		out += have > 2 ? tbl[n & 0x3f] : '=';
	}
	return out;
}

/* Little-endian image of an integer, which is how parquet encodes one. */
template<typename T>
static std::string
le(T v)
{
	std::string	out(sizeof(T), '\0');

	std::memcpy(&out[0], &v, sizeof(T));
	return out;
}

static std::string
bound(const std::string &lo, const std::string &hi)
{
	return "{\"lo\":\"" + b64(lo) + "\",\"hi\":\"" + b64(hi) + "\"}";
}

static NodePtr
leaf(const std::string &name, int fieldId, Type::type physical,
	 ConvertedType::type converted = ConvertedType::NONE,
	 Repetition::type rep = Repetition::OPTIONAL)
{
	return PrimitiveNode::Make(name, rep, physical, converted, -1, -1, -1,
							   fieldId);
}

/*
 * A parquet file under construction, mirroring how the datalake writer builds
 * one: leaf columns carrying field ids, one or more row groups.
 */
class TestFile
{
public:
	/*
	 * name is what the fixture is called when fixtures are being written out
	 * (see fixtureDir): the file lands on disk so the agent's own test can
	 * read the very same bytes back.  Without it the file stays in memory.
	 */
	explicit TestFile(const std::string &name, const NodeVector &fields,
					  int maxStatisticsSize = -1)
		: name_(name), fields_(fields)
	{
		parquet::WriterProperties::Builder builder;

		builder.enable_statistics();
		if (maxStatisticsSize >= 0)
			builder.max_statistics_size((size_t) maxStatisticsSize);

		std::shared_ptr<parquet_arrow::io::OutputStream> sink;

		if (fixtureDir().empty())
			sink = parquet_arrow::io::BufferOutputStream::Create().ValueOrDie();
		else
		{
			path_ = fixtureDir() + "/" + name + ".parquet";
			sink = parquet_arrow::io::FileOutputStream::Open(path_).ValueOrDie();
		}

		writer_ = parquet::ParquetFileWriter::Open(
			sink,
			std::static_pointer_cast<GroupNode>(
				GroupNode::Make("schema", Repetition::OPTIONAL, fields)),
			builder.build());
	}

	/* Where fixtures are written, or empty when they are not. */
	static std::string &fixtureDir()
	{
		static std::string dir;

		return dir;
	}

	/*
	 * Buffered row groups, like the datalake writer's own, so columns may be
	 * filled in any order and a row group closes only when the next opens.
	 */
	void newRowGroup()
	{
		if (rg_ != nullptr)
			rg_->Close();
		rg_ = writer_->AppendBufferedRowGroup();
	}

	/*
	 * Write one column chunk.  defLevels empty means a REQUIRED column, where
	 * parquet takes no definition levels at all; otherwise level 1 is a value
	 * and level 0 a null, and values holds only the non-null ones.
	 */
	template<typename WriterType, typename ValueType>
	void write(int col, const std::vector<int16_t> &defLevels,
			   const std::vector<ValueType> &values)
	{
		WriterType *w = static_cast<WriterType *>(rg_->column(col));
		int64_t		n = defLevels.empty() ? (int64_t) values.size()
										  : (int64_t) defLevels.size();

		w->WriteBatch(n, defLevels.empty() ? nullptr : defLevels.data(),
					  nullptr, values.empty() ? nullptr : values.data());
	}

	/* std::vector<bool> has no contiguous storage, so booleans go raw. */
	void writeBool(int col, const std::vector<int16_t> &defLevels,
				   const bool *values, int64_t count)
	{
		parquet::BoolWriter *w =
			static_cast<parquet::BoolWriter *>(rg_->column(col));

		w->WriteBatch(defLevels.empty() ? count : (int64_t) defLevels.size(),
					  defLevels.empty() ? nullptr : defLevels.data(), nullptr,
					  values);
	}

	std::string collect()
	{
		int64_t		rows;
		std::string	json;

		if (rg_ != nullptr)
			rg_->Close();
		writer_->Close();
		rows = writer_->metadata()->num_rows();
		json = buildIcebergColumnStatsJson(writer_->metadata().get());

		if (!path_.empty())
			writeSidecar(rows, json);
		return json;
	}

private:
	/*
	 * The sidecar is what the agent's differential test reads: the payload the
	 * segment would have sent for this file, plus the row count that travels
	 * with it and the column names the metrics config is keyed by.  Everything
	 * else it needs it takes from the parquet file itself, which is the point
	 * -- both sides start from the same bytes.
	 */
	void writeSidecar(int64_t rows, const std::string &json)
	{
		std::string	sidecar = path_.substr(0, path_.size() - 8) + ".json";
		FILE	   *out = std::fopen(sidecar.c_str(), "w");

		if (out == nullptr)
		{
			std::fprintf(stderr, "cannot write fixture %s\n", sidecar.c_str());
			return;
		}

		std::fprintf(out, "{\"file\":\"%s.parquet\",\"record_count\":%lld,\n",
					 name_.c_str(), (long long) rows);
		std::fprintf(out, " \"columns\":[");
		for (size_t i = 0; i < fields_.size(); i++)
		{
			std::fprintf(out, "%s{\"id\":%d,\"name\":\"%s\"}",
						 i > 0 ? "," : "", fields_[i]->field_id(),
						 fields_[i]->name().c_str());
		}
		std::fprintf(out, "],\n \"column_stats\":%s}\n",
					 json.empty() ? "null" : json.c_str());
		std::fclose(out);
	}

	std::string	name_;
	NodeVector	fields_;
	std::string	path_;
	std::unique_ptr<parquet::ParquetFileWriter> writer_;
	parquet::RowGroupWriter *rg_ = nullptr;
};

static parquet::ByteArray
ba(const std::string &s)
{
	return parquet::ByteArray((uint32_t) s.size(),
							  reinterpret_cast<const uint8_t *>(s.data()));
}

/*
 * Every physical type the datalake writer emits, so the tag the agent keys
 * its decoding off is pinned to what the FILE says rather than to what the
 * table column was declared as.  "char", time and interval all land here as
 * plain BYTE_ARRAY, which is exactly why they have to read back as binary:
 * the agent's old footer path called them binary too, and truncating them as
 * strings would not have matched it.
 */
static void
testTypeTags()
{
	NodeVector	fields;

	fields.push_back(leaf("c_bool", 1, Type::BOOLEAN));
	fields.push_back(leaf("c_int", 2, Type::INT32));
	fields.push_back(leaf("c_long", 3, Type::INT64));
	fields.push_back(leaf("c_float", 4, Type::FLOAT));
	fields.push_back(leaf("c_double", 5, Type::DOUBLE));
	fields.push_back(PrimitiveNode::Make("c_date", Repetition::OPTIONAL,
										 parquet::LogicalType::Date(),
										 Type::INT32, -1, 6));
	fields.push_back(PrimitiveNode::Make(
		"c_ts", Repetition::OPTIONAL,
		parquet::LogicalType::Timestamp(true,
										parquet::LogicalType::TimeUnit::MICROS),
		Type::INT64, -1, 7));
	fields.push_back(leaf("c_text", 8, Type::BYTE_ARRAY, ConvertedType::UTF8));
	fields.push_back(leaf("c_bytea", 9, Type::BYTE_ARRAY));
	fields.push_back(PrimitiveNode::Make("c_numeric", Repetition::OPTIONAL,
										 Type::FIXED_LEN_BYTE_ARRAY,
										 ConvertedType::DECIMAL, 5, 10, 2, 10));

	TestFile	f("types", fields);
	std::vector<int16_t> one = {1};
	std::string	text = "hello";
	std::string	bin = "\x01\x02";
	/* 12345 as decimal(10,2), big-endian in the schema's 5 bytes. */
	std::string	dec("\x00\x00\x00\x30\x39", 5);

	bool		boolValue = true;

	f.newRowGroup();
	f.writeBool(0, one, &boolValue, 1);
	f.write<parquet::Int32Writer, int32_t>(1, one, {42});
	f.write<parquet::Int64Writer, int64_t>(2, one, {42});
	f.write<parquet::FloatWriter, float>(3, one, {1.5f});
	f.write<parquet::DoubleWriter, double>(4, one, {1.5});
	f.write<parquet::Int32Writer, int32_t>(5, one, {19000});
	f.write<parquet::Int64Writer, int64_t>(6, one, {1700000000000000LL});
	f.write<parquet::ByteArrayWriter, parquet::ByteArray>(7, one, {ba(text)});
	f.write<parquet::ByteArrayWriter, parquet::ByteArray>(8, one, {ba(bin)});
	f.write<parquet::FixedLenByteArrayWriter, parquet::FixedLenByteArray>(
		9, one,
		{parquet::FixedLenByteArray(
			reinterpret_cast<const uint8_t *>(dec.data()))});

	std::string json = f.collect();

	checkContains(column(json, 1), "\"t\":\"bool\"", "BOOLEAN tags as bool");
	checkContains(column(json, 2), "\"t\":\"i32\"", "plain INT32 tags as i32");
	checkContains(column(json, 3), "\"t\":\"i64\"", "plain INT64 tags as i64");
	checkContains(column(json, 4), "\"t\":\"f32\"", "FLOAT tags as f32");
	checkContains(column(json, 5), "\"t\":\"f64\"", "DOUBLE tags as f64");
	checkContains(column(json, 6), "\"t\":\"date\"", "INT32+Date tags as date");
	checkContains(column(json, 7), "\"t\":\"tstz\"",
				  "INT64+Timestamp(UTC) tags as tstz");
	checkContains(column(json, 8), "\"t\":\"str\"",
				  "BYTE_ARRAY+UTF8 tags as str");
	checkContains(column(json, 9), "\"t\":\"bin\"",
				  "BYTE_ARRAY with no converted type tags as bin -- the "
				  "\"char\"/time/interval case");
	checkContains(column(json, 10), "\"t\":\"dec(10,2)\"",
				  "FLBA+DECIMAL carries precision and scale");

	checkContains(column(json, 2), bound(le<int32_t>(42), le<int32_t>(42)),
				  "int bound is the plain little-endian value, unaltered");
	checkContains(column(json, 8), bound("hello", "hello"),
				  "string bound is the UTF-8 bytes, unaltered");
	checkContains(column(json, 10), bound(dec, dec),
				  "decimal bound is the fixed-width big-endian image the "
				  "writer wrote; the agent renormalizes it");
	checkContains(json, "\"v\":1", "payload declares wire format v1");
}

/*
 * value_count counts nulls, null_count counts only them, and a column with
 * nothing but nulls reports both without claiming a range.  Getting the first
 * one wrong is the trap: Statistics::num_values() excludes nulls and would
 * make the manifest's valueCounts disagree with its nullValueCounts.
 */
static void
testCounts()
{
	NodeVector	fields;

	fields.push_back(leaf("c_some", 1, Type::INT32));
	fields.push_back(leaf("c_none", 2, Type::INT32));

	TestFile	f("counts", fields);
	std::vector<int16_t> mixed = {1, 0, 1, 0};
	std::vector<int16_t> allNull = {0, 0, 0, 0};

	f.newRowGroup();
	f.write<parquet::Int32Writer, int32_t>(0, mixed, {1, 5});
	f.write<parquet::Int32Writer, int32_t>(1, allNull, std::vector<int32_t>());

	std::string json = f.collect();

	checkContains(column(json, 1), "\"vc\":4",
				  "value_count includes the nulls");
	checkContains(column(json, 1), "\"nc\":2", "null_count counts the nulls");
	checkContains(column(json, 1), bound(le<int32_t>(1), le<int32_t>(5)),
				  "bound spans the non-null values");

	checkContains(column(json, 2), "\"vc\":4",
				  "an all-null column still reports its value count");
	checkContains(column(json, 2), "\"nc\":4",
				  "an all-null column is all nulls");
	checkMissing(column(json, 2), "\"rg\"",
				 "an all-null column claims no range");
}

/*
 * Bounds are reported per row group and merged by the agent with Iceberg's
 * comparators, because parquet's own comparator and Iceberg's disagree on
 * strings outside the basic plane.  A row group holding only nulls drops out
 * of the merge rather than poisoning it.
 */
static void
testRowGroups()
{
	NodeVector	fields;

	fields.push_back(leaf("c_int", 1, Type::INT32));
	fields.push_back(leaf("c_gap", 2, Type::INT32));

	TestFile	f("rowgroups", fields);
	std::vector<int16_t> two = {1, 1};
	std::vector<int16_t> twoNull = {0, 0};

	f.newRowGroup();
	f.write<parquet::Int32Writer, int32_t>(0, two, {10, 20});
	f.write<parquet::Int32Writer, int32_t>(1, twoNull, std::vector<int32_t>());

	f.newRowGroup();
	f.write<parquet::Int32Writer, int32_t>(0, two, {30, 40});
	f.write<parquet::Int32Writer, int32_t>(1, two, {7, 9});

	std::string json = f.collect();

	checkContains(column(json, 1),
				  "\"rg\":[" + bound(le<int32_t>(10), le<int32_t>(20)) + "," +
				  bound(le<int32_t>(30), le<int32_t>(40)) + "]",
				  "each row group reports its own bound, unmerged");
	checkContains(column(json, 1), "\"vc\":4",
				  "counts are summed across row groups");

	checkContains(column(json, 2),
				  "\"rg\":[" + bound(le<int32_t>(7), le<int32_t>(9)) + "]",
				  "the all-null row group contributes no bound");
	checkContains(column(json, 2), "\"nc\":2",
				  "but it still contributes its nulls");
}

/*
 * A bound past the ceiling takes the whole column's bounds with it, counts
 * included but range excluded.  Half a bound would be worse than none: the
 * agent would merge the surviving row groups into a range that excludes rows
 * this file really holds, and a scan would skip it.
 */
static void
testOversizedBound()
{
	NodeVector	fields;

	fields.push_back(leaf("c_text", 1, Type::BYTE_ARRAY, ConvertedType::UTF8));
	fields.push_back(leaf("c_small", 2, Type::BYTE_ARRAY, ConvertedType::UTF8));

	TestFile	f("oversized", fields);
	std::vector<int16_t> one = {1};
	std::string	huge(ICEBERG_COLUMN_BOUND_MAX_BYTES + 1, 'x');
	std::string	small = "ok";

	f.newRowGroup();
	f.write<parquet::ByteArrayWriter, parquet::ByteArray>(0, one, {ba(huge)});
	f.write<parquet::ByteArrayWriter, parquet::ByteArray>(1, one, {ba(small)});

	std::string json = f.collect();

	checkMissing(column(json, 1), "\"rg\"",
				 "an oversized bound drops the column's whole range");
	checkContains(column(json, 1), "\"vc\":1",
				  "an oversized bound keeps the column's counts");
	checkContains(column(json, 2), bound("ok", "ok"),
				  "a neighbouring column is unaffected");
}

/*
 * The single-sided drop.  parquet drops an oversized min and an oversized max
 * independently, and the metadata reader still answers HasMinMax() -- so a
 * naive collector would publish the surviving side next to an empty string
 * and quietly claim a range the data does not have.  An empty upper bound is
 * refused for exactly that reason; an empty lower bound is not, because too
 * low is safe.
 */
static void
testSingleSidedDrop()
{
	NodeVector	fields;

	fields.push_back(leaf("c_text", 1, Type::BYTE_ARRAY, ConvertedType::UTF8));

	/* Small enough that the long value's max is dropped but "" survives. */
	TestFile	f("single_sided_drop", fields, 16);
	std::vector<int16_t> two = {1, 1};
	std::string	empty;
	std::string	longish(64, 'z');

	f.newRowGroup();
	f.write<parquet::ByteArrayWriter, parquet::ByteArray>(
		0, two, {ba(empty), ba(longish)});

	std::string json = f.collect();

	checkMissing(column(json, 1), "\"rg\"",
				 "a dropped max never reads back as an empty upper bound");
	checkContains(column(json, 1), "\"vc\":2",
				  "the column still reports its counts");
}

/*
 * A legitimately empty lower bound survives: "" really is the smallest byte
 * string, so believing it can only widen the range.
 */
static void
testEmptyLowerBound()
{
	NodeVector	fields;

	fields.push_back(leaf("c_text", 1, Type::BYTE_ARRAY, ConvertedType::UTF8));

	TestFile	f("empty_lower", fields);
	std::vector<int16_t> two = {1, 1};
	std::string	empty;
	std::string	other = "m";

	f.newRowGroup();
	f.write<parquet::ByteArrayWriter, parquet::ByteArray>(
		0, two, {ba(empty), ba(other)});

	std::string json = f.collect();

	checkContains(column(json, 1), bound("", "m"),
				  "an empty string as the minimum is a real bound");
}

/*
 * Position-delete files carry Iceberg's reserved field ids, and their
 * file_path bound is what scopes the delete to one data file -- correctness,
 * not pruning.  Their columns are REQUIRED, so they arrive with no definition
 * levels at all.
 */
static void
testPositionDeleteShape()
{
	NodeVector	fields;

	fields.push_back(leaf("file_path", 2147483546, Type::BYTE_ARRAY,
						  ConvertedType::UTF8, Repetition::REQUIRED));
	fields.push_back(leaf("pos", 2147483545, Type::INT64, ConvertedType::NONE,
						  Repetition::REQUIRED));

	TestFile	f("posdelete", fields);
	std::string	path = "s3://b/data/f.parquet";
	std::vector<int16_t> none;

	f.newRowGroup();
	f.write<parquet::ByteArrayWriter, parquet::ByteArray>(
		0, none, {ba(path), ba(path)});
	f.write<parquet::Int64Writer, int64_t>(1, none, {0, 7});

	std::string json = f.collect();

	checkContains(column(json, 2147483546), bound(path, path),
				  "file_path bound is exactly the one data file referenced");
	checkContains(column(json, 2147483546), "\"nc\":0",
				  "a REQUIRED column has no nulls");
	checkContains(column(json, 2147483545),
				  bound(le<int64_t>(0), le<int64_t>(7)),
				  "pos bound spans the deleted ordinals");
}

int
main(int argc, char **argv)
{
	/*
	 * With a directory argument the cases also leave their parquet files and
	 * payloads on disk, as fixtures for the agent's differential test.
	 */
	if (argc > 1)
		TestFile::fixtureDir() = argv[1];

	testTypeTags();
	testCounts();
	testRowGroups();
	testOversizedBound();
	testSingleSidedDrop();
	testEmptyLowerBound();
	testPositionDeleteShape();

	std::printf("%d checks, %d failures\n", checks, failures);
	return failures == 0 ? 0 : 1;
}
