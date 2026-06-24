#ifndef PARQUET_READER_H
#define PARQUET_READER_H

#include <memory>
#include <vector>

#include <parquet/api/reader.h>
#include <parquet/internal/arrow/io/interfaces.h>
#include <gopher/gopher.h>
#include "src/common/dataBufferArray.h"
#include "base_reader.h"

using Datalake::Internal::dataBufferArray;
using Datalake::Internal::dataBuff;

class ParquetReader : public BaseFileReader
{
private:
	int numColumns_;
	std::string filePath_;
	gopherFS gopherFilesystem_;
	std::vector<int> rowGroups_;
	std::unique_ptr<parquet::ParquetFileReader> reader_;
	std::vector<std::shared_ptr<parquet::Scanner>> scanners_;
	std::shared_ptr<parquet::FileMetaData> metadata;

	dataBufferArray *buffer_;
	List *quals_;	/* WHERE-clause quals (Expr) for row-group min/max skip; NIL if none */
	int rowGroupsSkipped_ = 0;	/* count of row groups pruned by min/max stats (observability) */

	bool invalidFileOffset(int64_t startIndex, int64_t preStartIndex, int64_t preCompressedSize);
	void filterRowGroupByOffset(int64_t startOffset, int64_t endOffset);
	/* issue #297: row-group min/max ("zone map") pruning.
	 * rowGroupMightMatch returns false if quals_ prove row group rgIdx has no
	 * matching rows.  The expr helpers take Expr* / parquet::RowGroupMetaData*
	 * (declared void* to keep PG node types out of this header) and return true
	 * only when they PROVE the row group is excluded. */
	bool rowGroupMightMatch(int rgIdx);
	bool exprExcludesRowGroup(void *expr, void *rgMeta);
	bool opExprExcludesRowGroup(void *opExpr, void *rgMeta);
	bool nullTestExcludesRowGroup(void *nullTest, void *rgMeta);
	bool saoExprExcludesRowGroup(void *scalarArrayOpExpr, void *rgMeta);
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
	Datum readDecimal(std::shared_ptr<parquet::Scanner> &scannner, const TypeInfo &typinfo, bool &isNull);
	bool readNextRowGroup();
	void createMapping(List *columnDesc, bool *attrUsed);
	void decodeRecord();

public:
	ParquetReader(MemoryContext rowContext, char *filePath, gopherFS gopherFilesystem, dataBufferArray *buffer, List *quals);
	~ParquetReader();

	void open(List *columnDesc, bool *attrUsed, int64_t startOffset, int64_t endOffset);
	void close();
};

#endif // PARQUET_READER_H
