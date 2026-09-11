#ifndef DATALAKE_PROVIDERWRAPPER_H
#define DATALAKE_PROVIDERWRAPPER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "src/datalake_type.h"

extern bool external_table_debug;
extern bool external_table_new_text;

/*
 * Default size, in kB, of the blocks the new text reader cuts a large file
 * into (32 MB).  It lives here, not in readPolicy.h next to the reader, for
 * one reason: datalake_fdw.c is C and cannot include that C++ header, yet it
 * needs this value for the GUC's boot value.  Both the boot value and the
 * variable's initializer must come from one definition or they drift apart --
 * which is exactly what the three hand-copied `32 * 1024` literals this
 * replaces would have done.
 */
#define TEXT_BLOCK_SIZE_DEFAULT_KB (32 * 1024)

extern int external_table_text_block_size;
extern bool external_table_ignore_hidden_file;
extern bool enable_set_hdfs_user;
extern bool enable_list_in_master;
extern bool enable_get_block_location;

struct ProviderInternalWrapper;

typedef struct ProviderInternalWrapper *providerWrapper;

providerWrapper initProvider(DLTblFmt type, DLCmdType cmd, bool vectorization);

void createHandler(providerWrapper provider, void* sstate);

int64_t readFromProvider(providerWrapper provider, void *values, void *nulls);

int64_t readFromProviderWithTid(providerWrapper provider, void *values, void *nulls, void *tid);

void setPartitionValue(providerWrapper provider, void* values, void* nulls);

int64_t readRecordBatch(providerWrapper provider, void** recordBatch);

int64_t readBufferFromProvider(providerWrapper provider, void* buffer, int64_t length);

int64_t writeToProvider(providerWrapper provider, const void* buf, int64_t length);

/* Set the partition tuple (a PG List* passed as void*) of the data file that
 * the next writeToProvider() position-delete record targets. */
void providerSetDeletePartition(providerWrapper provider, void* partitionValues);

const char* getReadProviderFileName(providerWrapper provider);

void destroyHandler(providerWrapper provider);

void reScanProvider(providerWrapper provider);

void destroyProvider(providerWrapper provider);

void *getProviderRowReader(providerWrapper provider);

#ifdef __cplusplus
}
#endif

#endif
