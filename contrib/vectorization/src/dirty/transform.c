#include<regex.h>
#include <string.h>
#include"transform.h"
#include "nodes/parsenodes.h"
#include "optimizer/cost.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "cdb/cdbdisp_query.h"

bool* prev_optimizer = NULL;
bool* prev_enable_mergejoin = NULL;
char* prev_vec_stddev = NULL;
char* prev_partition_top_k = NULL;

static void forbiden_merge_join(const char **query_string, const char* merge_join_partten[]);
static void enable_partition_top_k(const char **query_string,
								   const char *optimizer_enable_motion_broadcast_partten);
void forbiden_stddev_vec(const char **query_string, const char *stddev_non_vec_partten[]);

typedef struct GucItem
{
	const char *guc;
	const char *value;
	const char *prev;
} GucItem;

void
try_transform_sql(const char **query_string)
{
	regex_t	 regex;
	int	     r;
	regmatch_t     matches[100];
	int            nmatch; 
	extern const char* transform_result[];
        extern const int transform_nmatches[];
        extern const int transform_matches[][TRANSFORM_LEN];
        extern const char* transform_pattern[];
        char*   new_sql_string;
        char*   next;
        int sql_bytes;
        regmatch_t matche;
        char* result;

	forbiden_merge_join(query_string, merge_join_partten);
	enable_partition_top_k(query_string, partition_top_k_partten);
	forbiden_stddev_vec(query_string, stddev_partten);

	for (int i = 0; i < TRANSFORM_NUM; i++) {
                nmatch = transform_nmatches[i] + 1;
                sql_bytes = 0;
		r = regcomp(&regex, transform_pattern[i], 0);
		if (r) {
			elog(WARNING, "Could not compile regex\n");
			return;
		}
		r = regexec(&regex, *query_string, nmatch, matches, 0);
                regfree(&regex);
		if (REG_NOERROR == r) {
                        result = pstrdup(transform_result[i]);
                        new_sql_string = (char*)palloc(strlen(transform_result[i]) + 100);
                        memset(new_sql_string, 0, strlen(transform_result[i]) + 100);
                        next = strtok(result, "####");
                        for (int j = 0; transform_matches[i][j] != -1; j++) {
                                matche = matches[transform_matches[i][j] + 1];
                                memcpy(new_sql_string + strlen(new_sql_string), (*query_string) + matche.rm_so, matche.rm_eo - matche.rm_so);
                                sql_bytes += matche.rm_eo - matche.rm_so;
                                new_sql_string[sql_bytes] = '\0';
                                sql_bytes += strlen(next);
                                strcat(new_sql_string, next);
                                next = strtok(NULL, "####");
                                
                        }
                        *query_string = new_sql_string;
                        break;
		}
                
	}
}

void
forbiden_merge_join(const char **query_string, const char* merge_join_partten[])
{
	regex_t	 regex;
	int	     r;
	for (int i = 0; merge_join_partten[i] != NULL; i++) {
		r = regcomp(&regex, merge_join_partten[i], 0);
		if (r) {
			elog(WARNING, "Could not compile regex\n");
			return;
		}
		r = regexec(&regex, *query_string, 0, NULL, 0);
		regfree(&regex);
		if (REG_NOERROR == r) {
			prev_optimizer = &optimizer;	
			prev_enable_mergejoin = &enable_mergejoin;
			optimizer = false;
			enable_mergejoin = false;
			break;
		}
	}
}

void
forbiden_stddev_vec(const char **query_string, const char* stddev_non_vec_partten[])
{
	regex_t	 regex;
	int	     r;
	for (int i = 0; stddev_non_vec_partten[i] != NULL; i++) {
		r = regcomp(&regex, stddev_non_vec_partten[i], 0);
		if (r) {
			elog(WARNING, "Could not compile regex\n");
			return;
		}
		r = regexec(&regex, *query_string, 0, NULL, 0);
		regfree(&regex);
		if (REG_NOERROR == r) {
			prev_vec_stddev = (char *)GetConfigOption("vector.enable_vectorization", true, false);
			List *guc_list = NIL;
			A_Const aconst = {.type = T_A_Const, .val = {.sval = {.type = T_String, .sval = pstrdup("off")}}};
			guc_list = lappend(guc_list, (void *)&aconst);
			SetPGVariable("vector.enable_vectorization", guc_list, false);
			break;
		}
	}
}

void
enable_partition_top_k(const char **query_string, const char* partition_top_k_partten)
{
	regex_t	 regex;
	int	     r;
	r = regcomp(&regex, partition_top_k_partten, 0);
	if (r) {
		elog(WARNING, "Could not compile regex\n");
		return;
	}
	r = regexec(&regex, *query_string, 0, NULL, 0);
	regfree(&regex);
	if (REG_NOERROR == r) {
		prev_partition_top_k = (char*)GetConfigOption("vector.partition_top_k", true, false);
		List *guc_list = NIL;
		A_Const aconst = {.type = T_A_Const, .val = {.sval = {.type = T_String, .sval = pstrdup("100")}}};
		guc_list = lappend(guc_list, (void*)&aconst);
		SetPGVariable("vector.partition_top_k", guc_list, false);
	}
}

void
restore_gucs(void)
{
	if (prev_optimizer) 
	{
		optimizer = *prev_optimizer;
		prev_optimizer = NULL;
	}
	if (prev_enable_mergejoin) 
	{
		enable_mergejoin = *prev_enable_mergejoin;
		prev_enable_mergejoin = NULL;
	}
	if (prev_partition_top_k) 
	{
		List *guc_list = NIL;
		A_Const aconst = {.type = T_A_Const, .val = {.sval = {.type = T_String, .sval = pstrdup(prev_partition_top_k)}}};
		guc_list = lappend(guc_list, (void*)&aconst);
		SetPGVariable("vector.partition_top_k", guc_list, false);
		prev_partition_top_k = NULL;
	}
	if (prev_vec_stddev)
	{
		List *guc_list = NIL;
		A_Const aconst = {.type = T_A_Const, .val = {.sval = {.type = T_String, .sval = pstrdup(prev_vec_stddev)}}};
		guc_list = lappend(guc_list, (void *)&aconst);
		SetPGVariable("vector.enable_vectorization", guc_list, false);
		prev_vec_stddev = NULL;
	}
}
