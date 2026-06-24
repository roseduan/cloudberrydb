#ifndef DATALAKE_ORCREAD_H
#define DATALAKE_ORCREAD_H

#include "src/provider/provider.h"
#include "orcFileReader.h"
#include "orc_stripe_filter.h"
#include <vector>


namespace Datalake {
namespace Internal {

class orcRead : public Provider, public readLogical
{
public:
	virtual void createHandler(void *sstate);

	virtual int64_t read(void *values, void *nulls);

	virtual void destroyHandler();

protected:
	virtual bool createPolicy();

	virtual fileState getFileState();

	virtual bool readNextFile();

	bool getStripeFromSmallFile(metaInfo info);

	bool getStripeFromBigFile(metaInfo info);

	/* issue #297: ORC stripe min/max pushdown.  Built positionally (table
	 * column i -> ORC column id i+1) when a file is opened. */
	void buildOrcFilterCols();
	bool stripeKept(int stripeIdx, const std::string &fileName);

	virtual bool getRow(Datum *values, bool *nulls);

	virtual bool getNextGroup();

protected:

	void restart();

	orcReadPolicy readPolicy;
	orcFileReader fileReader;
	int64_t tupleIndex;
	int stripeIndex;
	orcReadDeltaFile deltaFile;
	std::vector<RowGroupColMeta> orcFilterCols;
};

}
}
#endif
