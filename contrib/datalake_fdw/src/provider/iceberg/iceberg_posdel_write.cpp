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
	fileStream = datalakeCreateFileSystem((void*)(ss->options->gopher));
	prefix = (char*)lfirst(list_head(ss->fragments));
	initWriteOption();
	buildFilePrefix(ss->options);
	generateNewFileName();
	file_writer = std::make_unique<parquetFileWriter>();
	file_writer->init(ss->modify_state->us_slot->tts_tupleDescriptor, MetadataSchemaBuilder::transformToParquetSchema(POS_DELETE_SCHEMA), option);
}

int64_t icebergPosDeleteWrite::write(const void* buf, int64_t length)
{
	/*
	 * File-scoped position deletes: each delete parquet must reference exactly one
	 * data file, otherwise some standard readers (notably Trino, whose iceberg
	 * connector mis-applies a delete file that spans multiple data files) leave
	 * deleted rows visible.  Column 0 of the delete tuple (DELETE_FILE_PATH, a
	 * CSTRING) is the data file the deleted row lives in; roll over to a fresh
	 * parquet whenever it changes so the file_path column is single-valued.
	 *
	 * Iceberg 1.3.0 cannot yet set the manifest referenced_data_file, so this
	 * single-file_path layout is the file-scoping we rely on for now.
	 */
	TupleTableSlot *slot = (TupleTableSlot *) buf;
	std::string target;
	if (!slot->tts_isnull[0])
		target.assign(DatumGetCString(slot->tts_values[0]));

	bool targetChanged = file_writer->isOpen() && target != current_delete_target;
	bool sizeExceeded = file_writer->isOpen() && option.writeFileSize > 0 &&
						file_writer->getWrittenBytes() + length > option.writeFileSize;
	if (targetChanged || sizeExceeded)
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
	current_delete_target = target;
	int64_t len = file_writer->write(buf, length);
	tuple_num++;
	return len;
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
