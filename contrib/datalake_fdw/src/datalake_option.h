#ifndef DATALAKE_OPTION_H
#define DATALAKE_OPTION_H

#include "datalake_def.h"

dataLakeOptions *datalakeGetOptions(Oid foreigntableid);

List* datalakeGetCopyOptions(Oid foreigntableid);

List* datalakeGetCustomOptions(Oid foreigntableid);

void datalakeGetCopyLogerrorOptions(Oid foreigntableid, int *rejectlimit,
			   bool *islimitinrows, char *logerrors);

void datalakeGetUriFromOptions(Oid foreigntableid, char** uri);

void datalakeFreeDatalakeOptions(dataLakeOptions *options);

void datalakeCheckValidRecordBatchOpt(dataLakeOptions *options);

char* datalakeGetCompressionName(CompressType compress);

CompressType datalakeGetCompression(const char* compress);

void DatalakeGetGopherSocketPath(char *dest);

void DatalakeGetGopherPlasmaSocketPath(char *dest);

void DatalakeGetGopherMetaPath(char *dest);

char* getOptionFromList(List *options, const char *option);

#endif							/* DATALAKE_OPTION_H */
