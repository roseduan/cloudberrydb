#ifndef DATALAKE_PARQUETREAD_H
#define DATALAKE_PARQUETREAD_H

#include "parquetFileReader.h"
#include "src/common/readPolicy.h"
#include "src/provider/provider.h"
#include "src/common/dataBufferArray.h"
#include "src/provider/common/rowgroup_filter.h"


namespace Datalake {
namespace Internal {
class parquetRead : public Provider, public readLogical
{
public:

	virtual void createHandler(void *sstate);

	virtual int64_t read(void *values, void *nulls);

	virtual void destroyHandler();

private:
	virtual bool createPolicy();

	virtual bool getNextGroup();

	virtual bool readNextFile();

	bool getRowGropFromSmallFile(metaInfo info);

	bool getRowGropFromBigFile(metaInfo info);

	virtual fileState getFileState();

	virtual bool getRow(Datum *values, bool *nulls);

	bool convertToDatum(Datum *values, bool *nulls);

	void restart();

	bool checkSchemaCompatibility();

	/* issue #297: row-group min/max pushdown for the generic parquet path.
	 * Built (positional table<->parquet column map) when a file is opened. */
	void buildRowGroupFilterCols();
	bool rowGroupKept(int rgIdx);

	readBlockPolicy blockPolicy;
	std::vector<int> rowGroupNums;
    std::vector<int> tempRowGroupNums;
	int curRowGroupNum;
	parquetFileReader fileReader;
	std::vector<RowGroupColMeta> rgFilterCols;
};

}
}

#endif