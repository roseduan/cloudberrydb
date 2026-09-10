/*-------------------------------------------------------------------------
 *
 * iceberg_site_scalar.h
 *    YAML 1.1 scalar resolution, reproduced from SnakeYAML.
 *
 * Deliberately free of PostgreSQL dependencies: this is the layer where a
 * disagreement with the agent turns into a silently wrong endpoint or a
 * corrupted credential, so it has to be exercisable by a plain test binary
 * with no cluster and no build of the extension behind it.  Callers own all
 * the memory; nothing here allocates.
 *
 * The patterns come from org/yaml/snakeyaml/resolver/Resolver.class in
 * snakeyaml 2.2, the version contrib/datalake_agent/pom.xml pins.  They are
 * reproduced and explained in .router/plans/410/W1-equivalence-spec.md §8.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/iceberg_site_scalar.h
 *-------------------------------------------------------------------------
 */
#ifndef ICEBERG_SITE_SCALAR_H
#define ICEBERG_SITE_SCALAR_H

#include <stdbool.h>
#include <stddef.h>

typedef enum IcebergSiteScalarTag
{
	ICEBERG_SITE_SCALAR_NULL,	/* empty, ~, null, Null, NULL */
	ICEBERG_SITE_SCALAR_BOOL,	/* normalised to "true" / "false" */
	ICEBERG_SITE_SCALAR_INT,	/* normalised to decimal */
	ICEBERG_SITE_SCALAR_STR,	/* the input text, unchanged */
	ICEBERG_SITE_SCALAR_UNSUPPORTED		/* float, timestamp, sexagesimal */
} IcebergSiteScalarTag;

/*
 * Enough room for any normalised rendering of an input of the given length.
 * The rendering is never longer than the input plus a sign and terminator,
 * except that a short input can render as a longer word or number: "no"
 * becomes "false", and "0x7FFFFFFFFFFFFFFF" becomes 19 digits.  The constant
 * covers both with room to spare.
 */
#define ICEBERG_SITE_SCALAR_BUFSZ(inputlen)	((inputlen) + 24)

/*
 * Resolve one *plain* scalar -- a quoted one is always a string and must not
 * be passed here.  out receives the normalised text for BOOL and INT and is
 * left alone otherwise; outlen must be at least
 * ICEBERG_SITE_SCALAR_BUFSZ(strlen(text)).  For UNSUPPORTED, *unsupportedKind
 * receives a static description naming what was recognised.
 */
extern IcebergSiteScalarTag icebergSiteResolveScalar(const char *text,
													 char *out,
													 size_t outlen,
													 const char **unsupportedKind);

/*
 * Percent-decode, reproducing FilePathUtils.unescapeString().  out must hold
 * at least strlen(path) + 1 bytes.  Returns the decoded length.
 */
extern size_t icebergSiteUnescape(const char *path, char *out);

/*
 * Split a location into its first path component and the rest, reproducing
 * Utilities.parserBucketAndPrefix() -- including its inability to see past a
 * URI scheme.  bucket and prefix must each hold strlen(location) + 1 bytes.
 * Returns false for the input the Java code rejects.
 */
extern bool icebergSiteSplitBucketAndPrefix(const char *location,
											char *bucket,
											char *prefix);

#endif							/* ICEBERG_SITE_SCALAR_H */
