#include "parquetRead.h"

extern "C" {
	#include "src/common/random_segment.h"
    #include "src/datalake_fragment.h"
}

namespace Datalake {
namespace Internal {

void parquetRead::createHandler(void *sstate)
{
	initParameter(sstate);
	datalakeExecSegment(selected_segments, segId, segnum, &exec, &dummy_segid, &dummy_segnums);
	if (!exec)
	{
		return;
	}
	blockSerial = dummy_segid;
	initializeColumnValue();
	createPolicy();
	initFileStream();
	readNextGroup();
}

bool parquetRead::createPolicy()
{
	std::vector<ListContainer> lists;
	int64_t totalsize = 0;
	if (scanstate->options->hiveOption->hivePartitionKey != NULL)
	{
		List *fragment = datalakeGetNextPartitionFragmentList(scanstate->options, &totalsize);
		extraFragmentLists(lists, fragment);
		datalakeFreeFragmentLists(fragment);
		scanstate->options->hiveOption->curPartition += 1;
	}
	else
	{
		List *fragment = datalakeGetFragmentList(scanstate->options, &totalsize);
		extraFragmentLists(lists, fragment);
		datalakeFreeFragmentLists(fragment);
	}

	blockPolicy.build(dummy_segid, dummy_segnums, BLOCK_POLICY_SIZE, lists);
	if (!nPartitionKey)
	{
		blockPolicy.distBlock();
		blockSerial = blockPolicy.start;
	}
	return exec;
}

void parquetRead::restart()
{
	rowGroupNums.clear();
	tempRowGroupNums.clear();
	curRowGroupNum = 0;
	last = false;
	fileReader.closeParquetReader();
	blockPolicy.resetReadBlockPolicy();
	initializeColumnValue();
	createPolicy();
	readNextGroup();
}

bool parquetRead::checkSchemaCompatibility()
{
    auto file_schema = fileReader.getFileMetadata()->schema();
    ParquetLogicalType parquet_type;
    int columnInFile = file_schema->num_columns();
    for (int i = 0; i < columnInFile; i++)
    {
        Oid typeOid = tupdesc->attrs[i].atttypid;\
        const auto &des = file_schema->Column(i);
        if (!parquet_type.checkDataTypeCompatible(typeOid, des->physical_type()))
        {
            elog(ERROR, "Type Mismatch: columnIndex %d. MPP type %s. Parquet type %s. External Type Mapping %s",
                i, parquet_type.getColTypeName(typeOid).c_str(), des->name().c_str(), parquet_type.getTypeMappingSupported().c_str());
        }
    }
	return false;
}

bool parquetRead::getNextGroup()
{
	int size = tempRowGroupNums.size();
	for (; curRowGroupNum < size; curRowGroupNum++)
	{
		fileReader.setRowGroup(tempRowGroupNums[curRowGroupNum]);
		fileReader.createScanners();
		curRowGroupNum++;
		return true;
	}
	return false;
}

bool parquetRead::readNextFile()
{
	if (blockSerial >= blockPolicy.end)
    {
        return false;
    }
    tempRowGroupNums.clear();
    curRowGroupNum = 0;
    auto it = blockPolicy.block.find(blockSerial);
    if (it != blockPolicy.block.end())
    {
        metaInfo info = it->second;
        if (info.exists)
        {
            if (info.fileLength <= info.blockSize)
            {
                return getRowGropFromSmallFile(info);
            }
            else
            {
                return getRowGropFromBigFile(info);
            }
        }
    }
    else
    {
        int64_t blockCount = blockPolicy.block.size();
        elog(ERROR, "Datalake foreign table internal error. block index %d "
        "not found in parquet block policy. block count %ld", blockSerial, blockCount);
    }

    return true;
}

/*
 * issue #297: build the column metadata used for row-group min/max pruning.
 * The generic parquet reader maps table column i to Parquet column i
 * positionally (see checkSchemaCompatibility).  timeUnit is left unset (-1) so
 * timestamps fall through to "keep" -- date/int/numeric/text/bool still prune.
 */
void parquetRead::buildRowGroupFilterCols()
{
	rgFilterCols.clear();
	auto schema = fileReader.getFileMetadata()->schema();
	int ncol = schema->num_columns();
	int natts = tupdesc->natts;

	for (int i = 0; i < natts; i++)
	{
		RowGroupColMeta m;
		m.pgType = tupdesc->attrs[i].atttypid;
		m.colIdx = (i < ncol) ? i : -1;
		m.scale = 0;
		m.precision = 0;
		m.typeLength = 0;
		m.timeUnit = -1;
		if (m.colIdx >= 0)
		{
			const auto &col = schema->Column(i);
			m.scale = col->type_scale();
			m.precision = col->type_precision();
			m.typeLength = col->type_length();
		}
		rgFilterCols.push_back(m);
	}
}

/* true if row group rgIdx should be read (not provably excluded by quals). */
bool parquetRead::rowGroupKept(int rgIdx)
{
	if (scanstate->quals == NIL)
		return true;
	auto rg = fileReader.getFileMetadata()->RowGroup(rgIdx);
	bool keep = !rowGroupExcludedByQuals(scanstate->quals, rg.get(), rgFilterCols);
	if (!keep)
		elog(DEBUG1, "datalake parquet row-group skip: file=%s rg=%d skipped by min/max",
			 curFileName.c_str(), rgIdx);
	return keep;
}

bool parquetRead::getRowGropFromSmallFile(metaInfo info)
{
	fileReader.closeParquetReader();
    if (!fileReader.createParquetReader(fileStream, info.fileName, options))
    {
        /* this file format not parquet skip it. */
        elog(LOG, "Datalake foreign table LOG, file %s format is not parquet skip it.", info.fileName.c_str());
        return true;
    }

    checkSchemaCompatibility();
    buildRowGroupFilterCols();

    for (int i = 0; i < fileReader.getRowGroupNums(); i++)
    {
        if (rowGroupKept(i))
            tempRowGroupNums.push_back(i);
    }
    return true;
}

bool parquetRead::getRowGropFromBigFile(metaInfo info)
{
    if (curFileName != info.fileName)
    {
        rowGroupNums.clear();
		fileReader.closeParquetReader();
        /* open next file */
        if (!fileReader.createParquetReader(fileStream, info.fileName, options))
        {
            /* this file format not parquet skip it. */
            elog(LOG, "Datalake foreign table LOG, file %s format is not parquet skip it.", info.fileName.c_str());
            return true;
        }

        checkSchemaCompatibility();
        buildRowGroupFilterCols();
        curFileName = info.fileName;
        for (int i = 0; i < fileReader.getRowGroupNums(); i++)
        {
            /* min/max pruned row groups are dropped entirely (not deferred). */
            if (!rowGroupKept(i))
                continue;
            int64_t offset = fileReader.rowGroupOffset(i);
            if (info.rangeOffset <= offset && offset < info.rangeOffsetEnd)
            {
                tempRowGroupNums.push_back(i);
            }
            else
            {
                rowGroupNums.push_back(i);
            }
        }
    }
    else
    {
        /* is old file */
        int64_t count = rowGroupNums.size();
        for (int64_t i = 0; i < count; i++)
        {
            int64_t offset = fileReader.rowGroupOffset(rowGroupNums[i]);
            if (info.rangeOffset <= offset && offset < info.rangeOffsetEnd)
            {
                tempRowGroupNums.push_back(rowGroupNums[i]);
            }
        }
    }
    return true;
}


int64_t parquetRead::read(void *values, void *nulls)
{
	if (!exec)
	{
		return 0;
	}
nextPartition:

	if (readNextRow((Datum*)values, (bool*)nulls))
	{
		if (scanstate->options->hiveOption->hivePartitionKey != NULL)
		{
			AttrNumber numDefaults = this->nDefaults;
			int *defaultMap = this->defMap;
			ExprState **defaultExprs = this->defExprs;

			for (int i = 0; i < numDefaults; i++)
			{
				/* only eval const expr, so we don't need pg_try catch block here */
				Datum* value = (Datum*)values;
				bool* null = (bool*)nulls;
				value[defaultMap[i]] = datalakeExecEvalConst(defaultExprs[i], NULL, &null[defaultMap[i]], NULL);
			}
		}
		return 1;
	}

	if (QueryFinishPending || QueryCancelPending)
	{
		return 0;
	}

	if (!datalakeIsLastPartition(scanstate))
	{
		restart();
		goto nextPartition;
	}
	return 0;
}

bool parquetRead::getRow(Datum *values, bool *nulls)
{
	return convertToDatum(values, nulls);
}

bool parquetRead::convertToDatum(Datum *values, bool *nulls)
{
	int nColumnsToRead = ncolumns - nPartitionKey;
    int nColumnsInFile = fileReader.getFileColumns();
    for (int i = 0; i < nColumnsToRead; i++)
    {
        if (options.nPartitionKey <= 0 && !options.includes_columns[i])
        {
            nulls[i] = true;
            continue;
        }
        if (i >= nColumnsInFile)
        {
            nulls[i] = true;
            continue;
        }
        bool isNull = false;
        int state = 0;
        Datum value = fileReader.read(tupdesc->attrs[i].atttypid, i, isNull, state);
        if (state == -1)
        {
            /* read has not row, read next */
            return false;
        }
        values[i] = value;
        nulls[i] = isNull;
    }
    return true;
}

fileState parquetRead::getFileState()
{
	return fileReader.getState();
}

void parquetRead::destroyHandler()
{
    fileReader.closeParquetReader();
    datalakeDestroyFileSystem(fileStream);
    fileStream = NULL;
	releaseResources();
}

}
}