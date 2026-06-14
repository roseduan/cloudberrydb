#include "iceberg_posdel_write.h"

extern "C" {
#include "access/table.h"
#include "utils/lsyscache.h"
#include "catalog/namespace.h"
#include "access/heapam.h"
#include "src/common/fileMetadata.h"
}

namespace Datalake {
namespace Internal {


void icebergPosDeleteWrite::createHandler(void *sstate)
{
	ss = (dataLakeFdwScanState*)sstate;
	gopherConfig *conf = datalakeCreateGopherConfig((void*)(ss->options->gopher));
	fileStream = datalakeCreateFileSystem(conf);
	datalakeFreeGopherConfig(conf);
	prefix = (char*)lfirst(list_head(ss->fragments));
	initWriteOption();
	buildFilePrefix(ss->options);
	generateNewFileName();
	file_writer = std::make_unique<parquetFileWriter>();
	file_writer->init(ss->modify_state->us_slot->tts_tupleDescriptor, MetadataSchemaBuilder::transformToParquetSchema(POS_DELETE_SCHEMA), option);
}

void icebergPosDeleteWrite::appendFileMeta()
{
	MemoryContext oldContext = MemoryContextSwitchTo(CurrentMemoryContext->parent);
	FileFragment *meta = (FileFragment*)palloc0(sizeof(FileFragment));
	meta->filePath = pstrdup((append_file_prefix + file_name).c_str());
	/* Class 1 (#344): delete this staging delete-file if the txn aborts. */
	iceberg_register_staging_pending_delete(ss->rel, file_name.c_str(),
											(void *) ss->options->gopher);
	meta->fileSize = file_writer->getWrittenBytes();
	meta->format = PARQUET;
	meta->recordCount = tuple_num;
	meta->content = POSITION_DELETES;
	meta->type = T_FileFragment;
	fileMetas = lappend(fileMetas, (void*)meta);
	MemoryContextSwitchTo(oldContext);
}
}
}
