#ifndef _UTILS_H_
#define _UTILS_H_

#include "gopher/gopher.h"

#include "src/provider/common/config.h"
#include "src/dlproxy/uriparser.h"
#include "utils/plancache.h"
#include "utils/guc.h"

struct List;
struct FileScanTask;
struct ExternalTableMetadata;

#define BLOCK_SIZE (1024 * 1024 * gopher_local_blocksize_mb)

typedef struct DatalakeReaderInitInfo
{
	int				taskId;
	MemoryContext	mcxt;
	List		   *datafileDesc;
	TupleDesc		tupDesc;
	bool		   *attrUsed;
	gopherFS		gopherFilesystem;
	FileScanTask   *fileScanTask;
	ExternalTableMetadata *tableOptions;
	void		   *buffer;
	uint32			fileId;			/* File ID for current task (Iceberg only) */
	void		   *deleteIndex;	/* IcebergDeleteIndex for pre-built delete bitmaps */
	List		   *filterQuals;	/* WHERE-clause quals for row-group pushdown (data file only) */
} DatalakeReaderInitInfo;

typedef struct DatalakeInternalRecord
{
	Datum    *values;
	bool     *nulls;
	int64     position;
	uint32    fileId;      /* For Iceberg TID encoding */
} DatalakeInternalRecord;

typedef struct InternalRecordWrapper {
	DatalakeInternalRecord  record;
	void           *recordDesc;
} InternalRecordWrapper;

typedef struct DatalakeLogRecordHashEntry {
	InternalRecordWrapper *recordKey;
	InternalRecordWrapper *recordValue;
} DatalakeLogRecordHashEntry;

typedef struct DatalakeInternalRecordDesc {
	void *hashTab;
	int   nColumns;
	int   nKeys;
	int  *keyIndexes;
	Oid  *attrType;
	bool *attrUsed;
} DatalakeInternalRecordDesc;

typedef struct Reader
{
	struct Reader *(*Create) (void *args);
	bool (*Next) (struct Reader *reader, DatalakeInternalRecord *record);
	void (*Close) (struct Reader *reader);
} Reader;

typedef struct DatalakeFieldDescription
{
	char  name[NAMEDATALEN];
	Oid   typeOid;
	int   typeMod;
} DatalakeFieldDescription;

typedef struct DatalakeKeyValue
{
	char *key;
	char *value;
} DatalakeKeyValue;

typedef struct MergeProvider
{
	void (*Close) (struct MergeProvider *merger);
	void (*CombineAndUpdate) (struct MergeProvider *merger, InternalRecordWrapper *record);
	void (*UpdateOnDelete) (struct MergeProvider *merger, InternalRecordWrapper *record);
	bool (*Next) (struct MergeProvider *merger, DatalakeInternalRecord *record);
	bool (*Contains) (struct MergeProvider *merger, DatalakeInternalRecord *record, DatalakeInternalRecord **newRecord, bool *isDeleted);

	DatalakeInternalRecordDesc *recordDesc;
	Oid preCombineFieldType;
	int preCombineFieldIndex;
} MergeProvider;

extern PGDLLIMPORT int PostPortNumber;

typedef struct DatalakeRowReader
{
	List					*fileScanTasks;
	Reader					*curReader;
	List					*datafileDesc;
	bool					*attrUsed;
	gopherFS				gopherFilesystem;
	MemoryContext			mcxt;
	int						curReaderIndex;
	int						curTaskIndex;	/* cursor into fileScanTasks; advanced
											 * instead of consuming the list, so the
											 * scan can be rewound (see
											 * datalakeRowReaderRewind) */
	Reader					*handler;
	char					format;
	ExternalTableMetadata	*tableOptions;
	List					*filterQuals;	/* WHERE-clause quals (Expr) for row-group min/max pushdown */
	MemoryContext			taskMcxt;
	MemoryContext			taskListMcxt;	/* holds the persistent copy of
											 * fileScanTasks; freed once at
											 * datalakeRowReaderClose so the task
											 * nodes survive a rewind (rescan) */
	MemoryContext			curMcxt;
	void 					*buffer;
	bool					fileIndexMapInitialized;	/* Track if Iceberg file index map has been initialized */
	bool					deleteIndexBuilt;			/* True after first icebergBuildDeleteIndex attempt */
	void					*deleteIndex;				/* IcebergDeleteIndex for position delete dedup */

	/*
	 * Fast-path cache: direct pointer to the innermost format reader
	 * (e.g. parquet_next + BaseFileReader*), bypassing 3 layers of
	 * function-pointer indirection per row.  Set by C code in
	 * row_reader.c after successful datalakeRowReaderNext().
	 */
	bool					(*deepNext)(void *reader, DatalakeInternalRecord *record);
	void					*deepReader;	/* e.g. BaseFileReader* as void* */
	uint32					deepFileId;		/* cached fileId for TID encoding */
} DatalakeRowReader;

typedef struct DatalakeRemoteFileHandle
{
	gopherFS        gopherFilesystem;
	DatalakeRowReader      *reader;
	ResourceOwner   owner;
	struct DatalakeRemoteFileHandle *next;
	struct DatalakeRemoteFileHandle *prev;
} DatalakeRemoteFileHandle;

typedef struct DatalakeProtocolContext
{
	List			*filterQuals;
	MemoryContext	rowContext;
	DatalakeInternalRecord	*record;
	DatalakeRemoteFileHandle *file;
} DatalakeProtocolContext;

#ifndef WORDS_BIGENDIAN
static inline uint32_t
datalakeReverse32(uint32_t value)
{
	value = (value >> 16) | (value << 16);
	return ((value & 0xff00ff00UL) >> 8) | ((value & 0x00ff00ffUL) << 8);
}

static inline uint64_t
datalakeReverse64(uint64_t value)
{
	value = (value >> 32) | (value << 32);
	value = ((value & 0xff00ff00ff00ff00ULL) >> 8) | ((value & 0x00ff00ff00ff00ffULL) << 8);
	return ((value & 0xffff0000ffff0000ULL) >> 16) | ((value & 0x0000ffff0000ffffULL) << 16);
}
#endif

bool datalakeCharSeqEquals(char *s1, int s1Len, char *s2, int s2Len);
int64 datalakeGetFileRecordCount(List *deletes);
int *datalakeCreateRecordKeyIndexes(List *recordKeys, List *columnDesc);
uint32 datalakeFieldHash(Datum datum, Oid type);
bool datalakeFieldCompare(Datum datum1, Datum datum2, Oid type);
InternalRecordWrapper *datalakeCreateInternalRecordWrapper(void *recordDesc, int nColumns);
void datalakeDestroyInternalRecordWrapper(InternalRecordWrapper *recordWrapper);
bool *datalakeCreateDeleteProjectionColumns(List *recordKeys, List *columnDesc);
uint32 datalakeRecordHash(const void *key, Size keysize);
int datalakeRecordMatch(const void *key1, const void *key2, Size keysize);
InternalRecordWrapper *datalakeDeepCopyRecord(InternalRecordWrapper *recordWrapper);
int datalakeExtractScalFromTypeMod(int32 typmod);
Datum datalakeCreateDatumByText(Oid attType, const char *value);
void datalakeFreeKeyValueList(List *kvs);
bool hudiGreaterThan(Oid type, Datum datum1, Datum datum2);
void datalakeInitRecord(InternalRecordWrapper *record, void *recordDesc, int nColumns);
void datalakeCleanupRecord(InternalRecordWrapper *record);
DatalakeInternalRecordDesc *createInternalRecordDesc(MemoryContext mcxt, List *columnDesc, List *recordKeyFields,
						 char *preCombineField, int *preCombineFieldIndex, Oid *preCombineFieldType);
void destroyInternalRecordDesc(DatalakeInternalRecordDesc *recordDesc);

#endif /* _UTILS_H_ */
