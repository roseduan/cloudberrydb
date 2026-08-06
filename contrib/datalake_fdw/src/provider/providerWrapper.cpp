#include "providerWrapper.h"
#include "provider.h"
#include <exception>

extern "C" {
#include "utils/elog.h"
#include "src/provider/common/utils.h"

bool external_table_debug = false;
bool external_table_new_text = false;
bool external_table_ignore_hidden_file = false;
bool enable_set_hdfs_user = true;

}

struct ProviderInternalWrapper {
public:
	ProviderInternalWrapper(DLTblFmt type, DLCmdType cmd, bool vectorization) {
		context = getProvider(type, cmd, vectorization);
	}

	~ProviderInternalWrapper() {

	}

	Provider *getContext() {
		return context.get();
	}

private:
	std::shared_ptr<Provider> context;
};

#ifdef __cplusplus
extern "C" {
#endif

providerWrapper initProvider(DLTblFmt type, DLCmdType cmd, bool vectorization) {
	ProviderInternalWrapper *prov;
	try
	{
		prov = new ProviderInternalWrapper(type, cmd, vectorization);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "Datalake foreign table init provider failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "Datalake foreign table init provider failed.");
	}
	return prov;
}


void createHandler(providerWrapper provider, void* sstate) {
	try
	{
		provider->getContext()->createHandler(sstate);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "Datalake foreign table create handle failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "Datalake foreign table create handle failed.");
	}
	return;
}

int64_t readFromProvider(providerWrapper provider, void *values, void *nulls) {
	int64_t res = 0;
	try
	{
		res = provider->getContext()->read(values, nulls);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "Datalake foreign table read from oss failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "Datalake foreign table read from oss failed.");
	}
	return res;
}

int64_t readFromProviderWithTid(providerWrapper provider, void *values, void *nulls, void *tid) {
	int64_t res = 0;
	try
	{
		res = provider->getContext()->read(values, nulls, tid);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "Datalake foreign table read from oss with tid failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "Datalake foreign table read from oss with tid failed.");
	}
	return res;
}

/*
 * Get the DatalakeRowReader pointer for fast-path scan bypass.
 * Returns NULL if the provider doesn't have one (non-Iceberg/Hudi).
 */
void *getProviderRowReader(providerWrapper provider) {
	try
	{
		Provider *ctx = provider->getContext();
		DatalakeProtocolContext *pctx = (DatalakeProtocolContext *) ctx->getProtocolContext();
		if (pctx && pctx->file && pctx->file->reader)
			return (void *) pctx->file->reader;
	}
	catch (...)
	{
	}
	return NULL;
}

void setPartitionValue(providerWrapper provider, void *values, void *nulls) {
	try
	{
		provider->getContext()->setPartitionValue(values, nulls);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "Datalake foreign table read setPartitionValue failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "Datalake foreign table read setPartitionValue failed.");
	}
}

int64_t readRecordBatch(providerWrapper provider, void** recordBatch)
{
	int64_t res = 0;
	try
	{
		res = provider->getContext()->read(recordBatch);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "Datalake foreign table readRecordBatch from oss failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "Datalake foreign table readRecordBatch from oss failed.");
	}
	return res;
}

int64_t readBufferFromProvider(providerWrapper provider, void* buffer, int64_t length)
{
	int64_t res = 0;
	try
	{
		res = provider->getContext()->readWithBuffer(buffer, length);
	}
	catch(const std::exception& e)
	{
		elog(ERROR, "Datalake foreign table read buffer from oss failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "Datalake foreign table read buffer from oss failed.");
	}
	return res;
}

int64_t writeToProvider(providerWrapper provider, const void* buf, int64_t length) {
	int64_t writenLen = 0;
	try
	{
		writenLen = provider->getContext()->write(buf, length);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "Datalake foreign table write to oss failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "Datalake foreign table write to oss failed.");
	}
	return writenLen;
}

void providerSetDeletePartition(providerWrapper provider, void* partitionValues) {
	if (provider == NULL)
		return;
	try
	{
		provider->getContext()->setDeletePartition(partitionValues);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "Datalake set delete partition failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "Datalake set delete partition failed.");
	}
}

void destroyHandler(providerWrapper provider) {
	try
	{
		provider->getContext()->destroyHandler();
	}
	catch(std::exception &e)
	{
		elog(ERROR, "Datalake foreign table destroy handle failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "Datalake foreign table destroy handle failed.");
	}
	return;
}

void reScanProvider(providerWrapper provider) {
	try
	{
		provider->getContext()->reScan();
	}
	catch(std::exception &e)
	{
		elog(ERROR, "Datalake foreign table rescan failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "Datalake foreign table rescan failed.");
	}
	return;
}

void destroyProvider(providerWrapper provider) {
	try
	{
		if (provider)
		{
			delete provider;
			provider = NULL;
		}
	}
	catch (std::exception &e)
	{
		elog(ERROR, "Datalake foreign table destroy provider handle failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "Datalake foreign table destroy provider handle failed.");
	}
	return;
}

const char* getReadProviderFileName(providerWrapper provider)
{
	const char* res = 0;
	try
	{
		res = provider->getContext()->getReadFileName();
	}
	catch(const std::exception& e)
	{
		elog(ERROR, "Datalake foreign table get read filename failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "Datalake foreign table get read filename failed.");
	}
	return res;
}


#ifdef __cplusplus
}
#endif
