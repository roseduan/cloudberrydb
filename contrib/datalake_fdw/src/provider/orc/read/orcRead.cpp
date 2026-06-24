#include "orcRead.h"

#include <list>
#include <cassert>
#include <sstream>

extern "C" {
	#include "src/common/random_segment.h"
	#include "src/datalake_fragment.h"
}

namespace Datalake {
namespace Internal {

void orcRead::createHandler(void *sstate)
{
	initParameter(sstate);
	options.transactionTable = scanstate->options->hiveOption->transactional;
	datalakeExecSegment(selected_segments, segId, segnum, &exec, &dummy_segid, &dummy_segnums);
	if (!exec)
	{
		return;
	}
	blockSerial = dummy_segid;
	initializeColumnValue();
	createPolicy();
	initFileStream();
	deltaFile.readDeleteDeltaLists(fileStream,
		readPolicy.deleteDeltaLists, setStreamWhetherCache(options));
	readNextGroup();
}

bool orcRead::createPolicy()
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
	
	readPolicy.hiveTranscation = options.transactionTable;
	readPolicy.build(dummy_segid, dummy_segnums, BLOCK_POLICY_SIZE, lists);
	if (!nPartitionKey)
	{
		readPolicy.distBlock();
		blockSerial = readPolicy.start;
	}

	return exec;
}

void orcRead::restart()
{
	tupleIndex = 0;
	stripeIndex = 0;
	last = false;
	fileReader.closeORCReader();
	readPolicy.reset();
	initializeColumnValue();
	createPolicy();
	deltaFile.reset();
	deltaFile.readDeleteDeltaLists(fileStream,
		readPolicy.deleteDeltaLists, setStreamWhetherCache(options));
	readNextGroup();
}

int64_t orcRead::read(void *values, void *nulls)
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

fileState orcRead::getFileState()
{
	return fileReader.getState();
}

void orcRead::destroyHandler()
{
	tupleIndex = 0;
	fileReader.closeORCReader();
	datalakeDestroyFileSystem(fileStream);
	fileStream = NULL;
	releaseResources();
}

bool orcRead::readNextFile()
{
	if (blockSerial >= readPolicy.end)
	{
		return false;
	}
	stripeIndex = 0;
	fileReader.readInterface.tempStripes.clear();
	auto it = readPolicy.block.find(blockSerial);
	if (it != readPolicy.block.end())
	{
		metaInfo info = it->second;
		if (info.exists)
		{
			if (external_table_debug)
			{
				elog(LOG, "Datalake foreign table LOG, read block index %d, block offset %ld, end %ld", blockSerial, info.rangeOffset, info.rangeOffsetEnd);
			}
			if (info.fileLength <= info.blockSize)
			{
				return getStripeFromSmallFile(info);
			}
			else
			{
				return getStripeFromBigFile(info);
			}
		}
	}
	else
	{
		int64_t blockCount = readPolicy.block.size();
		elog(ERROR, "Datalake foreign table internal error. block index %d "
        "not found in orc block policy. block count %ld", blockSerial, blockCount);
	}
	return true;
}

/*
 * issue #297: build the column metadata used for ORC stripe min/max pruning.
 * The reader maps table column i to ORC column id i+1 positionally; decimal
 * scale/precision are unused on the ORC path (orc::Decimal::toString already
 * yields scaled text), and timestamps are intentionally not pruned (kept).
 */
void orcRead::buildOrcFilterCols()
{
	orcFilterCols.clear();
	int natts = tupdesc->natts;
	for (int i = 0; i < natts; i++)
	{
		RowGroupColMeta m;
		m.pgType = tupdesc->attrs[i].atttypid;
		m.colIdx = i;
		m.scale = 0;
		m.precision = 0;
		m.typeLength = 0;
		m.timeUnit = -1;
		orcFilterCols.push_back(m);
	}
}

/* true if stripe stripeIdx should be read (not provably excluded by quals). */
bool orcRead::stripeKept(int stripeIdx, const std::string &fileName)
{
	if (scanstate->quals == NIL)
		return true;
	/* Transactional (ACID) ORC nests data columns under a wrapper struct, so
	 * the positional column-id mapping does not hold; skip pruning then. */
	if (options.transactionTable)
		return true;
	/* No stripe statistics in the file footer -> cannot prune. */
	if (fileReader.readInterface.reader->getStripeStatisticsLength() == 0)
		return true;

	auto ss = fileReader.readInterface.reader->getStripeStatistics(stripeIdx);
	int64_t numRows = (int64_t) fileReader.readInterface.reader->getStripe(stripeIdx)->getNumberOfRows();
	bool keep = !stripeExcludedByQuals(scanstate->quals, ss.get(), numRows, orcFilterCols);
	if (!keep)
		elog(DEBUG1, "datalake orc stripe skip: file=%s stripe=%d skipped by min/max",
			 fileName.c_str(), stripeIdx);
	return keep;
}

bool orcRead::getStripeFromSmallFile(metaInfo info)
{
	fileReader.closeORCReader();
	if (!fileReader.createORCReader(fileStream, info.fileName, info.fileLength, options))
	{
		/* file format not orc skip it */
		elog(LOG, "Datalake foreign table LOG, file %s format is not orc skip it.", info.fileName.c_str());
		return true;
	}

	buildOrcFilterCols();
	for (uint64_t i = 0; i < fileReader.readInterface.reader->getNumberOfStripes(); i++)
	{
		if (!stripeKept((int) i, info.fileName))
			continue;
		fileReader.readInterface.tempStripes.push_back(fileReader.readInterface.reader->getStripe(i));
	}
	return true;
}

bool orcRead::getStripeFromBigFile(metaInfo info)
{
	if (curFileName != info.fileName)
	{
		fileReader.readInterface.stripes.clear();
		fileReader.closeORCReader();
		if (!fileReader.createORCReader(fileStream, info.fileName, info.fileLength, options))
		{
			/* file format not orc skip it */
			elog(LOG, "Datalake foreign table LOG, file %s format is not orc skip it.", info.fileName.c_str());
			return true;
		}

		curFileName = info.fileName;
		buildOrcFilterCols();
		for (uint64_t i = 0; i < fileReader.readInterface.reader->getNumberOfStripes(); i++)
		{
			/* min/max pruned stripes are dropped entirely (not deferred); they
			 * never enter `stripes`, so the same-file branch below only ever
			 * reuses already-kept stripes. */
			if (!stripeKept((int) i, info.fileName))
				continue;
			ORC_UNIQUE_PTR<orc::StripeInformation> result = fileReader.readInterface.reader->getStripe(i);
			int64_t stripeOffset = result->getOffset();
			if (info.rangeOffset <= stripeOffset && stripeOffset < info.rangeOffsetEnd)
			{
				fileReader.readInterface.tempStripes.push_back(fileReader.readInterface.reader->getStripe(i));
			}
			else
			{
				fileReader.readInterface.stripes.push_back(fileReader.readInterface.reader->getStripe(i));
			}
		}
	}
	else
	{
		int count = fileReader.readInterface.stripes.size();
		for (int i = 0; i < count; i++)
		{
			if (fileReader.readInterface.stripes[i] == NULL)
			{
				continue;
			}
			int64_t stripeOffset = fileReader.readInterface.stripes[i]->getOffset();
			if (info.rangeOffset <= stripeOffset && stripeOffset < info.rangeOffsetEnd)
			{
				fileReader.readInterface.tempStripes.push_back(std::move(fileReader.readInterface.stripes[i]));
			}
		}
	}
	return true;
}

bool orcRead::getRow(Datum *values, bool *nulls)
{
	while (fileReader.readInterface.batch != NULL)
	{
		if (tupleIndex < (int64_t)fileReader.readInterface.batch->numElements)
		{
			if (options.transactionTable && fileReader.compareToDeleteMap(deltaFile, tupleIndex))
			{
				++tupleIndex;
                continue;
			}
			else
			{
				int nColumnsToRead = ncolumns - nPartitionKey;
				fileReader.read(tupdesc, values, nulls, tupleIndex, nColumnsToRead);
				tupleIndex++;
				return true;
			}
		}
		tupleIndex = 0;
		return false;
	}
	tupleIndex = 0;
	return false;
}

bool orcRead::getNextGroup()
{
	if (fileReader.getState() == FILE_CLOSE)
	{
		return false;
	}

	while(!fileReader.readInterface.rowReader || !fileReader.readInterface.rowReader->next(*fileReader.readInterface.batch))
	{
		fileReader.readInterface.rowReader.reset();
		if (stripeIndex < (int)fileReader.readInterface.tempStripes.size())
		{
			// Read all columns in file when ncolumns > nColumnsInFile
			// Read the first ncolumns column when ncolumns <= nColumnsInFile
			int nColumnsToRead = std::min(fileReader.readInterface.getDataColumnsNum(), static_cast<int>(ncolumns));
			fileReader.readInterface.setRowReadOptions(attrs_used, nColumnsToRead);
			fileReader.readInterface.createRowReader(fileReader.readInterface.tempStripes[stripeIndex]->getOffset(),
				fileReader.readInterface.tempStripes[stripeIndex]->getLength());
			fileReader.readInterface.getTransactionTableType();

			/* Check schema compatibility after type is initialized.
			 * Only check once per file (first stripe). This mirrors
			 * parquetRead::checkSchemaCompatibility() placement. */
			if (stripeIndex == 0)
			{
				fileReader.checkFileSchemaCompatibility(tupdesc, nColumnsToRead);
			}

			stripeIndex++;
			continue;
		}
		else
		{
			return false;
		}
	}
	return true;
}



}
}