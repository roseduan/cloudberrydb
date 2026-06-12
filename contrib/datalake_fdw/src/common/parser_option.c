#include "parser_option.h"
#include "postgres.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "commands/defrem.h"

char* getStringOption(List *options, const char *optionName)
{
    ListCell *lc;

    foreach(lc, options)
    {
        DefElem *def = (DefElem *) lfirst(lc);
        if (pg_strcasecmp(def->defname, optionName) == 0)
        {
            return defGetString(def);
        }
    }

    return NULL;
}

bool getBoolOption(List *options, const char *optionName, bool defaultValue)
{
    bool isset;

    return getBoolOptionEx(options, optionName, defaultValue, &isset);
}

/*
 * Like getBoolOption, but also reports whether the user actually wrote the
 * option. Callers that forward options to the agent need the distinction so
 * an unset bool can fall back to the conf file while an explicit false
 * overrides it (per-key merge, SQL OPTIONS over site config).
 */
bool getBoolOptionEx(List *options, const char *optionName, bool defaultValue,
                     bool *isset)
{
    char *value = getStringOption(options, optionName);

    *isset = false;

    if (!value)
        return defaultValue;

    if (pg_strcasecmp(value, "true") == 0)
    {
        *isset = true;
        return true;
    }
    else if (pg_strcasecmp(value, "false") == 0)
    {
        *isset = true;
        return false;
    }
    else
        return defaultValue;
}

int getIntOption(List *options, const char *optionName, int defaultValue)
{
    char *value = getStringOption(options, optionName);

    if (!value)
        return defaultValue;

    return atoi(value);
}
