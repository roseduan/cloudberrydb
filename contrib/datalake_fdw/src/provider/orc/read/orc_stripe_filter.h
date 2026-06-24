/*-------------------------------------------------------------------------
 *
 * orc_stripe_filter.h
 *    ORC stripe min/max ("zone map") predicate pruning.  Wraps an ORC
 *    StripeStatistics in the shared IZoneStats interface so the generic
 *    zone-map evaluator (provider/common/rowgroup_filter) can skip stripes
 *    that provably contain no matching rows.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/provider/orc/read/orc_stripe_filter.h
 *-------------------------------------------------------------------------
 */
#ifndef ORC_STRIPE_FILTER_H
#define ORC_STRIPE_FILTER_H

#include <vector>
#include <cstdint>
#include "src/provider/common/rowgroup_filter.h"

namespace orc { class StripeStatistics; }

/*
 * Returns true iff `quals` prove the stripe described by `stripeStats`
 * (with `numRows` rows) contains no matching rows.  `cols` is indexed by
 * (Var attno - 1); each cols[i].colIdx is the 0-based table column position
 * (mapped to ORC column id colIdx+1, since ORC column 0 is the struct root).
 */
bool stripeExcludedByQuals(List *quals,
						   const orc::StripeStatistics *stripeStats,
						   int64_t numRows,
						   const std::vector<RowGroupColMeta> &cols);

#endif							/* ORC_STRIPE_FILTER_H */
