#ifndef DATALAKE_PROVIDERWRAPPER_H
#define DATALAKE_PROVIDERWRAPPER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "src/datalake_type.h"

extern bool external_table_debug;
extern bool external_table_new_text;
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

const char* getReadProviderFileName(providerWrapper provider);

void destroyHandler(providerWrapper provider);

void reScanProvider(providerWrapper provider);

void destroyProvider(providerWrapper provider);

void *getProviderRowReader(providerWrapper provider);

#ifdef __cplusplus
}
#endif

#endif
