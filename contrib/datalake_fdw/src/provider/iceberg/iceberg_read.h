#ifndef DATALAKE_ICEBERGREAD_H
#define DATALAKE_ICEBERGREAD_H

#include "src/common/rewrLogical.h"
#include "src/common/readPolicy.h"
#include "src/provider/provider.h"
#include "src/dlproxy/datalake.h"

extern "C" {
	#include "src/provider/common/utils.h"
	#include "src/provider/common/row_reader.h"
	#include "iceberg_task_reader.h"
}

namespace Datalake {
namespace Internal {

class icebergRead : public Provider, public readLogical
{
public:
	virtual void createHandler(void *sstate);

	virtual int64_t read(void *values, void *nulls);

	virtual int64_t read(void *values, void *nulls, void *tid);

	virtual void destroyHandler();

	virtual void reScan();

	virtual void *getProtocolContext() { return (void *) protocolContext; }

private:
	DatalakeProtocolContext *protocolContext;
	DatalakeInternalRecord record_ = {};	/* reused across rows, avoids per-row palloc */
};

}
}

#endif
