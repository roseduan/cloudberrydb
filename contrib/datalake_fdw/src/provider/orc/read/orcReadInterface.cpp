#include "orcReadInterface.h"
#include "orcReadDeltaFile.h"
#include <list>
#include <cassert>
#include <cstdint>
#include <sstream>

extern "C" {
#include "utils/elog.h"
}


namespace Datalake {
namespace Internal {


bool orcReadInterface::createORCReader(ossFileStream fileStream, std::string filename, int64_t length, bool enableCache, char *allocBuffer)
{
    auto istream = ORC_UNIQUE_PTR<orc::InputStream>(new OssInputStream(fileStream, filename, length, enableCache, allocBuffer));
    stream = (OssInputStream *)istream.get();
    if (!stream->checkORCFile())
    {
        /* this file is not orc file skip this file. */
        return false;
    }
    reader = orc::createReader(std::move(istream), options);

    /*
     * Cold-path backfill. Placed AFTER createReader for two reasons:
     * 1) only once the PostScript has been parsed do we know the tail's
     *    total length T;
     * 2) by this point the last block holding the PostScript / Footer /
     *    Metadata(StripeStatistics) has already been materialized by
     *    Gopher, so this tail read is a local pread and generates no
     *    remote traffic.
     *
     * isMetaOk() being true means the cache is already usable, so we
     * skip a wasted backfill read; FileMetaRangeCache::push() also
     * guards itself with hit()/tombstone() internally, as a second line
     * of defense.
     *
     * All four lengths come from liborc's public interface
     * (orc/Reader.hh:509-533); getStripeStatisticsLength() corresponds
     * to metadata_length in the PostScript.
     */
    if (!stream->isMetaOk())
    {
        uint64_t postscriptLen = reader->getFilePostscriptLength();
        uint64_t footerLen = reader->getFileFooterLength();
        uint64_t metadataLen = reader->getStripeStatisticsLength();
        uint64_t fileLen = reader->getFileLength();
        uint64_t tailLen = 1 + postscriptLen + footerLen + metadataLen;

        /*
         * This guard is about the CAST, not about the configured size
         * limit -- datalake.file_meta_max_size is enforced inside
         * FileMetaRangeCache::push(), where every format goes through it.
         *
         * tailLen is a uint64_t, but fillAndPushTail's len parameter is
         * only an int32_t. Without an explicit guard, a tail exceeding
         * INT32_MAX would get truncated into a negative number, and then
         * silently dropped by FileMetaRangeCache::push()'s len <= 0
         * check -- so this file would NEVER GET CACHED, WITH NO LOG
         * ANYWHERE TO EXPLAIN WHY. A real ORC footer + metadata
         * basically never gets anywhere near 2GB, but silent failure is
         * exactly the hardest failure mode of this feature to debug, so
         * it is worth leaving a clue.
         */
        if (tailLen > (uint64_t) INT32_MAX)
        {
            elog(DEBUG1,
                 "datalake: ORC tail %lu bytes cannot be expressed as an "
                 "int32 blob length, skipping backfill",
                 (unsigned long) tailLen);
        }
        else if (tailLen > 0 && tailLen <= fileLen)
        {
            stream->fillAndPushTail((int64_t) (fileLen - tailLen), (int32_t) tailLen);
        }
    }

    return true;
}

void orcReadInterface::setRowReadOptions(std::set<int> attr_used, int column)
{
    if (!transactionTable)
    {
        std::list<uint64_t> include;
        for (int i = 0; i < column; ++i)
        {
            if (attr_used.find(i) != attr_used.end())
            {
                include.push_back(i);
            }
        }
        rowOptions.include(include);
    }
}

void orcReadInterface::createRowReader(uint64_t stripeOffset, uint64_t stripteLength)
{
    rowOptions.range(stripeOffset, stripteLength);
    rowReader = reader->createRowReader(rowOptions);
    batch = rowReader->createRowBatch(ORC_BATCH_SIZE);
}

void orcReadInterface::setTransactionTable(bool transaction)
{
    transactionTable = transaction;
}

int orcReadInterface::getDataColumnsNum()
{
    if (transactionTable)
    {
        return reader->getType().getSubtype(5)->getSubtypeCount();
    }
    return reader->getType().getSubtypeCount();
}

void orcReadInterface::getStripes()
{
    for (uint64_t i = 0; i < reader->getNumberOfStripes(); ++i)
    {
        stripes.push_back(reader->getStripe(i));
    }
}

void orcReadInterface::getTransactionTableType()
{
    if (transactionTable)
    {
        const orc::Type *orcType = &reader->getType();
        type = orcType->getSubtype(5);
    }
    else
    {
        type = &reader->getType();
    }
}

orc::StructVectorBatch *orcReadInterface::getORCStructVectorBatch()
{
    if (transactionTable)
    {
        orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(batch.get());
        orcVector = dynamic_cast<orc::StructVectorBatch *>(structBatch->fields[5]);
    }
    else
    {
        orcVector = dynamic_cast<orc::StructVectorBatch *>(batch.get());
    }
    return orcVector;
}

void orcReadInterface::deleteORCReader()
{
    orc::RowReader *row_read = rowReader.release();
    if (row_read)
        delete row_read;

    orc::Reader *read = reader.release();
    if (read)
        delete read;
}


bool orcReadInterface::getNextStripes()
{
    for (; stripesIndex < (int)stripes.size(); stripesIndex++)
    {
        createRowReader(stripes[stripesIndex]->getOffset(), stripes[stripesIndex]->getLength());
        stripesIndex += 1;
        return true;
    }
    return false;
}

bool orcReadInterface::getNextBatch()
{
    while (!rowReader || !rowReader->next(*batch))
    {
        if (!getNextStripes())
        {
            batch.reset();
            return false;
        }
    }
    return true;

}

bool orcReadInterface::readRow(void *data)
{
    while (batch)
    {
		int count = batch->numElements;
        if (rowIndex < count)
        {
            parseTranscationRow(data);
            return true;
        }
        rowIndex = 0;
        getNextBatch();
    }
    return false;
}

void orcReadInterface::parseTranscationRow(void *data)
{
    orcVector = dynamic_cast<orc::StructVectorBatch *>(batch.get());
    transcationStruct *ptr = (transcationStruct*)data;
    orc::ColumnVectorBatch *mVector = orcVector->fields[0];
    orc::LongVectorBatch *vec = dynamic_cast<orc::LongVectorBatch *>(mVector);
    ptr->operation = vec->data[rowIndex];

    mVector = orcVector->fields[1];
    vec = dynamic_cast<orc::LongVectorBatch *>(mVector);
    ptr->originalTransaction = vec->data[rowIndex];

    mVector = orcVector->fields[2];
    vec = dynamic_cast<orc::LongVectorBatch *>(mVector);
    ptr->bucketId = vec->data[rowIndex];

    mVector = orcVector->fields[3];
    vec = dynamic_cast<orc::LongVectorBatch *>(mVector);
    ptr->rowId = vec->data[rowIndex];

    mVector = orcVector->fields[4];
    vec = dynamic_cast<orc::LongVectorBatch *>(mVector);
    ptr->currentTransaction = vec->data[rowIndex];

    rowIndex += 1;
}

}
}






