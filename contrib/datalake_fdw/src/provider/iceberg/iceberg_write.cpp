#include "iceberg_write.h"

extern "C" {
#include "access/table.h"
#include "utils/lsyscache.h"
#include <catalog/namespace.h>
#include <access/heapam.h>
#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "nodes/value.h"
#include "utils/rel.h"
#include "src/common/fileMetadata.h"
}

namespace Datalake {
namespace Internal {


void icebergWrite::createHandler(void *sstate)
{
	ss = (dataLakeFdwScanState*)sstate;
	fileStream = datalakeCreateFileSystem((void*)(ss->options->gopher));
	prefix = (char*)lfirst(list_head(ss->fragments));
	/*
	 * Capture the (persistent) context createHandler runs in.  The fanout
	 * writers created lazily in write() must be allocated here rather than in
	 * the per-row context, which is reset on every tuple.
	 */
	handlerContext = CurrentMemoryContext;
	initWriteOption();
	buildFilePrefix(ss->options);
	parsePartitionColumns();

	/*
	 * Unpartitioned tables keep the original single-stream writer.  Partitioned
	 * tables create one parquetFileWriter per partition lazily in write(), so no
	 * default writer is opened here.
	 */
	if (partCols.empty())
	{
		generateNewFileName();
		file_writer = std::make_unique<parquetFileWriter>();
		file_writer->init(sstate, option);
	}
}

/*
 * Resolve the PARTITION BY column names (comma-separated, declaration order)
 * against the relation tuple descriptor into attnos + type output functions.
 * Leaves partCols empty for unpartitioned tables.
 *
 * M0 supports identity partitioning on integer and character/text columns,
 * whose PostgreSQL output text matches the Iceberg identity partition value.
 * Other types (bool/date/timestamp/numeric/...) need the transform module's
 * byte-exact encoding and are rejected here until then.
 */
void icebergWrite::parsePartitionColumns()
{
	const char *pb = ss->options->partition_by;
	TupleDesc	desc;
	char	   *dup;
	char	   *saveptr = NULL;

	if (pb == NULL || pb[0] == '\0')
		return;

	desc = RelationGetDescr(ss->rel);
	dup = pstrdup(pb);

	for (char *tok = strtok_r(dup, ",", &saveptr);
		 tok != NULL;
		 tok = strtok_r(NULL, ",", &saveptr))
	{
		/* trim surrounding whitespace */
		while (*tok == ' ' || *tok == '\t')
			tok++;
		{
			char *end = tok + strlen(tok);
			while (end > tok && (end[-1] == ' ' || end[-1] == '\t'))
				*(--end) = '\0';
		}
		if (*tok == '\0')
			continue;

		IcebergPartitionColumn pc;
		bool		found = false;

		for (int i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(desc, i);

			if (att->attisdropped)
				continue;
			if (strcmp(NameStr(att->attname), tok) == 0)
			{
				bool		isvarlena;

				pc.attno = att->attnum;
				pc.typid = att->atttypid;
				pc.name = tok;
				getTypeOutputInfo(pc.typid, &pc.outputFunc, &isvarlena);
				found = true;
				break;
			}
		}

		if (!found)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("iceberg partition column \"%s\" not found in table \"%s\"",
							tok, RelationGetRelationName(ss->rel))));

		switch (pc.typid)
		{
			case INT2OID:
			case INT4OID:
			case INT8OID:
			case TEXTOID:
			case VARCHAROID:
			case BPCHAROID:
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("iceberg identity partitioning on column \"%s\" of type %u is not supported yet",
								pc.name.c_str(), pc.typid),
						 errhint("supported partition column types are integer and character/text")));
		}

		partCols.push_back(pc);
	}

	pfree(dup);
}

/*
 * Compute the partition tuple (identity values) for one input row and the
 * relative "col=val[/col2=val2]" directory path.  valuesOut carries the raw
 * type-output text sent to the agent; the path form escapes it for the object
 * key.  isNull marks a SQL NULL partition value.
 */
void icebergWrite::computePartition(TupleTableSlot *slot, std::string &pathOut,
									std::vector<IcebergPartitionValue> &valuesOut)
{
	std::stringstream path;

	for (size_t i = 0; i < partCols.size(); i++)
	{
		IcebergPartitionColumn &pc = partCols[i];
		Datum		datum = slot->tts_values[pc.attno - 1];
		bool		isnull = slot->tts_isnull[pc.attno - 1];
		IcebergPartitionValue pv;

		pv.isNull = isnull;
		if (!isnull)
		{
			char	   *out = OidOutputFunctionCall(pc.outputFunc, datum);

			pv.value = out;
			pfree(out);
		}
		valuesOut.push_back(pv);

		if (i > 0)
			path << "/";
		path << pc.name << "=";
		if (isnull)
		{
			path << "null";
		}
		else
		{
			/* Keep the object key filesystem-safe; correctness rides on the
			 * partition tuple sent to the agent, not on this directory name. */
			for (char c : pv.value)
			{
				if (c == '/' || c == '\\' || c == '=' || c == ' ' ||
					c == '%' || (unsigned char) c < 0x20)
				{
					char buf[4];
					snprintf(buf, sizeof(buf), "%%%02X", (unsigned char) c);
					path << buf;
				}
				else
					path << c;
			}
		}
	}

	pathOut = path.str();
}

void icebergWrite::buildFilePrefix(dataLakeOptions *opt)
{
	std::stringstream buf;
	if (PROTOCOL_IS_HDFS(opt->protocol))
	{
		buf << opt->gopher->gopherType << "://" << opt->gopher->hdfs_namenode_host << ":" << opt->gopher->hdfs_namenode_port;
	}
	else if (PROTOCOL_IS_OSS(opt->protocol))
	{
		buf << opt->gopher->gopherType << "://" << opt->gopher->bucket;
	}
	else
	{
		elog(ERROR, "Datalake foreign table Error, gopher type %s is not supported for iceberg.", opt->gopher->gopherType);
	}
	append_file_prefix = buf.str();
}

void icebergWrite::generateNewFileName()
{
	file_name = generateIcebergUuidFileName(prefix, "parquet");
}

void icebergWrite::appendFileMeta()
{
	MemoryContext oldContext = MemoryContextSwitchTo(CurrentMemoryContext->parent);
	FileFragment *meta = (FileFragment*)palloc0(sizeof(FileFragment));
	meta->filePath = pstrdup((append_file_prefix + file_name).c_str());
	/*
	 * Class 1 (#344): delete this staging data file if the txn aborts.  Use
	 * file_name -- the exact bucket-relative key createParquetWriter() wrote to
	 * on a gopherFS built from ss->options->gopher -- NOT meta->filePath, which
	 * carries the "<type>://<bucket>" URI prefix the gopher client does not want.
	 */
	iceberg_register_staging_pending_delete(ss->rel, file_name.c_str(),
											(void *) ss->options->gopher);
	meta->fileSize = file_writer->getWrittenBytes();
	meta->format = PARQUET;
	meta->recordCount = tuple_num;
	meta->content = DATA;
	meta->type = T_FileFragment;
	fileMetas = lappend(fileMetas, (void*)meta);
	MemoryContextSwitchTo(oldContext);
}

/*
 * Build the FileFragment for one closed per-partition data file, stamping its
 * partition tuple (partitionValues, spec order) so the commit pipeline and the
 * agent register the file under the right Iceberg partition.  Mirrors
 * appendFileMeta() for the unpartitioned case.
 */
void icebergWrite::appendPartitionFileMeta(IcebergPartitionFile &pf)
{
	MemoryContext oldContext = MemoryContextSwitchTo(CurrentMemoryContext->parent);
	FileFragment *meta = (FileFragment*)palloc0(sizeof(FileFragment));
	List	   *pv = NIL;

	meta->filePath = pstrdup((append_file_prefix + pf.fileName).c_str());
	/* Same abort-cleanup contract as appendFileMeta(): register the exact
	 * bucket-relative key written on the gopherFS, not the URI-prefixed path. */
	iceberg_register_staging_pending_delete(ss->rel, pf.fileName.c_str(),
											(void *) ss->options->gopher);
	meta->fileSize = pf.writer->getWrittenBytes();
	meta->format = PARQUET;
	meta->recordCount = pf.tupleNum;
	meta->content = DATA;
	meta->type = T_FileFragment;

	for (size_t i = 0; i < pf.values.size(); i++)
	{
		if (pf.values[i].isNull)
			pv = lappend(pv, NULL);
		else
			pv = lappend(pv, makeString(pstrdup(pf.values[i].value.c_str())));
	}
	meta->partitionValues = pv;

	fileMetas = lappend(fileMetas, (void*)meta);
	MemoryContextSwitchTo(oldContext);
}

void icebergWrite::initWriteOption()
{
	option.writeFileSize = ss->options->fileSizeLimit;
	option.compression = ss->options->compress;
	option.compressionLevel = ss->options->compressLevel;
}

int64_t icebergWrite::write(const void* buf, int64_t length)
{
	if (!partCols.empty())
	{
		/*
		 * Fanout path: route the row to the writer for its partition, creating
		 * that writer (and its data/<col>=<val>/ file) on first sight.  Each
		 * partition rolls files independently on the size limit.
		 */
		TupleTableSlot *slot = (TupleTableSlot *) buf;
		std::string		partPath;
		std::vector<IcebergPartitionValue> partValues;

		computePartition(slot, partPath, partValues);

		auto it = partWriters.find(partPath);
		if (it == partWriters.end())
		{
			/*
			 * Allocate the writer and its column batch buffers in the
			 * persistent handler context; init() palloc's buffers that must
			 * survive the per-row context reset between tuples.
			 */
			MemoryContext oldctx = MemoryContextSwitchTo(handlerContext);
			IcebergPartitionFile pf;

			pf.relativePath = partPath;
			pf.values = partValues;
			/* Dedicated storage stream: one active file per FileSystem handle. */
			pf.fileStream = datalakeCreateFileSystem((void *) (ss->options->gopher));
			pf.writer = std::make_unique<parquetFileWriter>();
			pf.writer->init((void *) ss, option);
			it = partWriters.emplace(partPath, std::move(pf)).first;
			MemoryContextSwitchTo(oldctx);
		}
		IcebergPartitionFile &pf = it->second;

		if (pf.writer->isOpen() && option.writeFileSize > 0 &&
			pf.writer->getWrittenBytes() + length > option.writeFileSize)
		{
			pf.writer->closeParquetWriter();
			appendPartitionFileMeta(pf);
			sliceIdx += 1;
		}

		if (!pf.writer->isOpen())
		{
			MemoryContext oldctx = MemoryContextSwitchTo(handlerContext);
			std::string writePrefix = prefix;

			if (!writePrefix.empty() && writePrefix.back() != '/')
				writePrefix += '/';
			writePrefix += partPath;

			pf.fileName = generateIcebergUuidFileName(writePrefix, "parquet");
			pf.writer->createParquetWriter(pf.fileStream, pf.fileName);
			pf.tupleNum = 0;
			MemoryContextSwitchTo(oldctx);
		}

		int64_t len = pf.writer->write(buf, length);
		pf.tupleNum++;
		return len;
	}

	if (file_writer->isOpen() && option.writeFileSize > 0 && file_writer->getWrittenBytes() + length > option.writeFileSize)
	{
		file_writer->closeParquetWriter();
		appendFileMeta();
		sliceIdx += 1;
	}

	if (!file_writer->isOpen())
	{
		generateNewFileName();
		file_writer->createParquetWriter(fileStream, file_name);
		tuple_num = 0;
	}
	int64_t len = file_writer->write(buf, length);
	tuple_num++;
	return len;
}

void icebergWrite::destroyHandler()
{
	if (!partCols.empty())
	{
		/* Flush and record every open per-partition data file. */
		for (auto &kv : partWriters)
		{
			IcebergPartitionFile &pf = kv.second;

			if (pf.writer->isOpen())
			{
				pf.writer->closeParquetWriter();
				appendPartitionFileMeta(pf);
			}
			if (pf.fileStream != NULL)
			{
				datalakeDestroyFileSystem(pf.fileStream);
				pf.fileStream = NULL;
			}
		}
	}
	else if (file_writer->isOpen())
	{
		file_writer->closeParquetWriter();
		appendFileMeta();
	}
	datalakeDestroyFileSystem(fileStream);
	fileStream = NULL;

	if (fileMetas != NIL)
	{
		if (ss->collect_qe_metadata)
		{
			/*
			 * Local collection path: transfer FileFragment ownership
			 * to ss->local_meta_list.  No serialize/deserialize round-trip.
			 * appendFileMeta() allocates in CurrentMemoryContext->parent,
			 * which outlives this function.
			 */
			ListCell *lc = NULL;
			int i;
			foreach_with_count (lc, fileMetas, i)
			{
				FileFragment *meta = (FileFragment *)lfirst(lc);
				ss->local_meta_list = lappend(ss->local_meta_list, meta);
			}
			/* Free list spine only; FileFragment ownership transferred */
			list_free(fileMetas);
		}
		else
		{
			/* Normal network send path */
			ListCell *lc = NULL;
			int i;
			foreach_with_count (lc, fileMetas, i)
			{
				FileFragment *meta = (FileFragment *)lfirst(lc);
				bytea *msg = FDW_serializeMeta(meta, ss->rel->rd_id);
				FDW_SendMeta(msg);
				pfree(msg);
				pfree(meta->filePath);
			}
			list_free_deep(fileMetas);
		}
	}
}
}
}
