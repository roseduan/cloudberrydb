/*-------------------------------------------------------------------------
 *
 * arrow_sort_options_vec.h
 *	  Helpers for creating GArrowSortOptions with external-sort GUC settings.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *		contrib/vectorization/src/include/utils/arrow_sort_options_vec.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ARROW_SORT_OPTIONS_VEC_H
#define ARROW_SORT_OPTIONS_VEC_H

#include "arrow-glib/compute.h"

#include "utils/guc_vec.h"
#include "vecexecutor/vec_spill_stats.h"

/*
 * plan_node_id names the spill files of the sort when
 * vector.sort_external_temp_file_base is unset: they then go to pgsql_tmp
 * like the other vectorized spill files, instead of Arrow's default of
 * $TMPDIR or the current directory (the data directory).
 *
 * The options only matter to the Sort node itself (Arrow's "orderby" node),
 * which spills sorted runs once its input passes
 * vector.sort_spill_memory_mb.
 */
static inline void
vec_apply_external_sort_options(GArrowSortOptions *sort_options,
								const char *spill_file_base)
{
	if (sort_options == NULL)
		return;

	garrow_sort_options_set_spill_memory_budget(sort_options,
								(gint64) sort_spill_memory_mb * 1024 * 1024);
	if (sort_spill_memory_mb <= 0)
		return;

	garrow_sort_options_set_external_sort_num_threads(sort_options,
										 sort_external_num_threads);
	if (sort_external_temp_file_base != NULL &&
		sort_external_temp_file_base[0] != '\0')
	{
		garrow_sort_options_set_external_sort_temp_file_base(
			sort_options,
			sort_external_temp_file_base);
	}
	else
		garrow_sort_options_set_external_sort_temp_file_base(sort_options,
															 spill_file_base);
}

static inline GArrowSortOptions *
vec_sort_options_new(GList *sort_keys,
				 guint64 limit,
				 gint64 take_thread_num,
				 gboolean two_phase_take,
				 int plan_node_id)
{
	GArrowSortOptions *sort_options;
	char		spill_file_base[MAXPGPATH];

	/* Before creating the options: this can ERROR, which would leak them. */
	VecSpillFileBase(spill_file_base, sizeof(spill_file_base), plan_node_id);
	sort_options = garrow_sort_options_new(sort_keys, limit,
							 take_thread_num, two_phase_take);
	vec_apply_external_sort_options(sort_options, spill_file_base);
	return sort_options;
}

#endif   /* ARROW_SORT_OPTIONS_VEC_H */