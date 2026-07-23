#include "iceberg_write.h"

extern "C" {
#include "access/table.h"
#include "utils/lsyscache.h"
#include <catalog/namespace.h>
#include <access/heapam.h>
#include "src/common/fileMetadata.h"
}

namespace Datalake {
namespace Internal {


void icebergWrite::createHandler(void *sstate)
{
	ss = (dataLakeFdwScanState*)sstate;
	fileStream = datalakeCreateFileSystem((void*)(ss->options->gopher));
	prefix = (char*)lfirst(list_head(ss->fragments));
	initWriteOption();
	buildFilePrefix(ss->options);
	generateNewFileName();
	file_writer = std::make_unique<parquetFileWriter>();
	file_writer->init(sstate, option);
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

void icebergWrite::initWriteOption()
{
	option.writeFileSize = ss->options->fileSizeLimit;
	option.compression = ss->options->compress;
	option.compressionLevel = ss->options->compressLevel;
}

int64_t icebergWrite::write(const void* buf, int64_t length)
{
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
	if (file_writer->isOpen())
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
