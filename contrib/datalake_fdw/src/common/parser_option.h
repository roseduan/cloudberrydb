#ifndef PARSER_OPTION_H
#define PARSER_OPTION_H

#include "postgres.h"
#include "nodes/pg_list.h"


char* getStringOption(List *options, const char *optionName);
bool getBoolOption(List *options, const char *optionName, bool defaultValue);
bool getBoolOptionEx(List *options, const char *optionName, bool defaultValue,
					 bool *isset);
int getIntOption(List *options, const char *optionName, int defaultValue);

#endif