#include "rewrLogical.h"
#include <random>
#include <algorithm>

/*
 * GUC datalake.disable_cache_file (defined in datalake_fdw.c). Default OFF
 * (false) means caching is enabled. Referenced here so the GUC can globally
 * control gopher block caching for all datalake reads, including native
 * iceberg AM tables that have no per-table enablecache option.
 */
extern bool disableCacheFile;

namespace Datalake {
namespace Internal {

void readLogical::releaseResources()
{
	if (defMap)
	{
		pfree(defMap);
	}
	if (defExprs)
	{
		pfree(defExprs);
	}
	if (includes_columns)
	{
		pfree(includes_columns);
	}
}

void readLogical::initParameter(void *sstate)
{
	scanstate = (dataLakeFdwScanState*)sstate;
	selected_segments = scanstate->selected_segments;
	tupdesc = scanstate->rel->rd_att;
    ncolumns = tupdesc->natts;
	curFileName = "";
	segId = GpIdentity.segindex;
	segnum = getgpsegmentCount();
	dummy_segid = 0;
	dummy_segnums = 0;
	exec = false;
	last = false;
	options.buffer.allocDataBufferArray(ncolumns);
	options.enableCache = scanstate->options->gopher->enableCache || !disableCacheFile;
	if (PROTOCOL_IS_HDFS(scanstate->options->protocol))
	{
		options.ptype = PROTOTCOL_HDFS;
	}
	else
	{
		options.ptype = PROTOTCOL_OSS;
	}
	std::vector<ListContainer> lists;
	// fileStream = createFileStream();

	getAttrsUsed(scanstate->retrieved_attrs, attrs_used);
	setOptionAttrsUsed(attrs_used);

	defMap = (int *)palloc0(ncolumns * sizeof(int));
	defExprs = (ExprState **)palloc0(ncolumns * sizeof(ExprState *));

	includes_columns = (bool*)palloc0(sizeof(bool) * (ncolumns));
	for (int i = 0; i < ncolumns; i++)
	{
		includes_columns[i] = options.includes_columns[i];
	}

	return;
}

void readLogical::initFileStream()
{
	fileStream = createFileStream();
}

void readLogical::setOptionAttrsUsed(std::set<int> used)
{
	for (int i = 0; i < tupdesc->natts; i++)
	{
        /* Skip columns we don't intend to use in query */
        if (used.find(i) == used.end())
		{
			options.includes_columns.push_back(false);
			continue;
		}
		else
		{
			options.includes_columns.push_back(true);
		}
	}
}

void readLogical::initReadLogical()
{
	return;
}

bool readLogical::readNextFile()
{
	return true;
}

void readLogical::initializeColumnValue()
{
	MemoryContext oldContext = MemoryContextSwitchTo(scanstate->initcontext);
	List *constraints;
	List *attNums = scanstate->options->hiveOption->attNums;
	int maxPartitionIdx = 0;
	int minPartitionIdx = 0;
	ListCell *lc;

	if (!scanstate->options->hiveOption->hivePartitionKey)
	{
		constraints = scanstate->options->hiveOption->constraints;
	}
	else
	{
		datalakePartitionConstraint *pc = (datalakePartitionConstraint*) list_nth(scanstate->options->hiveOption->hivePartitionConstraints,
			scanstate->options->hiveOption->curPartition);
		constraints = pc->constraints;
	}

	nPartitionKey = list_length(attNums);

	// Partition key must be at the end of the table
	if (nPartitionKey > 0)
	{
		foreach(lc, attNums)
		{
			int att = lfirst_int(lc);
			if (att < 0)
			{
				elog(ERROR, "get attNums error value %d", att);
			}
			if (maxPartitionIdx == 0 || maxPartitionIdx < att)
			{
				maxPartitionIdx = att;
			}
			if (minPartitionIdx == 0 || minPartitionIdx > att)
			{
				minPartitionIdx = att;
			}
		}
		if (ncolumns - nPartitionKey >= minPartitionIdx || maxPartitionIdx < ncolumns)
		{
			elog(ERROR, "Datalake foreign table internal error. partition key index %d-%d "
				"are not at the end of the table %d-%d", minPartitionIdx, maxPartitionIdx, ncolumns - nPartitionKey + 1, ncolumns);
		}
	}

	nDefaults = datalakeInitializeDefaultMap(attNums,
						constraints,
						includes_columns,
						defMap,
						defExprs);
	nConstraint = nDefaults;

	options.nPartitionKey = nPartitionKey;
	MemoryContextSwitchTo(oldContext);
}

bool readLogical::readNextRow(Datum *values, bool *nulls)
{
	while((getFileState() == FILE_OPEN) && !last)
	{
		if (!getRow(values, nulls))
		{
			if (!readNextGroup())
			{
				/* read over */
				last = true;
				return false;
			}
		}
		else
		{
			setPartitionNulls(nulls);
			return true;
		}
	}
	return false;
}

fileState readLogical::getFileState()
{
	return FILE_UNDEF;
}

bool readLogical::getRow(Datum *values, bool *nulls)
{
	return true;
}

bool readLogical::readNextGroup()
{
	while(true)
	{
		if (getNextGroup())
		{
			return true;
		}

		if (!readNextFile())
		{
			last = true;
			return false;
		}
		if (nPartitionKey)
		{
			blockSerial += dummy_segnums;
		}
		else
		{
			blockSerial++; 
		}
		
	}
	return false;
}

bool readLogical::getNextGroup()
{
	return true;
}

void readLogical::closeFile()
{
	return;
}

bool readLogical::createPolicy()
{
	return false;
}

ossFileStream readLogical::createFileStream()
{
	gopherConfig *gopherConf = datalakeCreateGopherConfig((void*)(scanstate->options->gopher));
	ossFileStream fileStream = datalakeCreateFileSystem(gopherConf);
	datalakeFreeGopherConfig(gopherConf);
	return fileStream;
}

void readLogical::extraFragmentLists(std::vector<ListContainer> &lists, List *fragmentLists)
{
	ListCell *cell;
	foreach(cell, fragmentLists)
	{
		ListContainer obj;
		List *fragment = (List*)lfirst(cell);
		obj.keyName = strVal(list_nth(fragment, 0));
		obj.size = atoll(strVal(list_nth(fragment, 1)));
		lists.push_back(obj);
	}
	sort(lists.begin(), lists.end(), cmp);
}

void readLogical::getAttrsUsed(List* retrieved_attrs, std::set<int> &attrs_used)
{
	ListCell   *frag_c = NULL;
	foreach(frag_c, retrieved_attrs)
	{
		int att = lfirst_int(frag_c) - 1;
		if (att < 0)
		{
			elog(ERROR, "get retrieved_attrs error value %d", att);
		}
		attrs_used.insert(att);
	}
}

int setStreamFlag(readOption opt)
{
	int flag = O_RDONLY|O_RDTHR;
	if (SUPPORT_ENABLE_CACHE(opt))
	{
		flag = O_RDONLY;
	}
	return flag;
}

bool setStreamWhetherCache(readOption opt)
{
	bool enableCache = false;
	if (SUPPORT_ENABLE_CACHE(opt))
	{
		enableCache = true;
	}
	return enableCache;
}

void readLogical::setPartitionNulls(bool* nulls)
{
	if (scanstate->options->hiveOption->hivePartitionKey != NULL)
	{
		int partitionColStart = ncolumns - nPartitionKey;
		for (int i = partitionColStart; i < ncolumns; ++i)
		{
			nulls[i] = !includes_columns[i];
		}
	}
	return;
}

}
}