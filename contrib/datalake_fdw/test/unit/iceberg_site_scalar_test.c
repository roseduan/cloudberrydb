/*-------------------------------------------------------------------------
 *
 * iceberg_site_scalar_test.c
 *    Equivalence tests for the YAML 1.1 scalar layer.
 *
 * Every case here is an assertion about what the *agent* would produce for
 * the same input, taken from .router/plans/410/W1-equivalence-spec.md §8.
 * The point is not that the C code is self-consistent but that it agrees
 * with SnakeYAML 2.2 plus Java's toString(); when this file and the agent
 * disagree, the agent wins and this file is the bug report.
 *
 * Builds and runs without PostgreSQL:
 *
 *     make -C contrib/datalake_fdw/test/unit check
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/test/unit/iceberg_site_scalar_test.c
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "iceberg_site_scalar.h"

static int	failures = 0;
static int	checks = 0;

static const char *
tagName(IcebergSiteScalarTag tag)
{
	switch (tag)
	{
		case ICEBERG_SITE_SCALAR_NULL:
			return "null";
		case ICEBERG_SITE_SCALAR_BOOL:
			return "bool";
		case ICEBERG_SITE_SCALAR_INT:
			return "int";
		case ICEBERG_SITE_SCALAR_STR:
			return "str";
		case ICEBERG_SITE_SCALAR_UNSUPPORTED:
			return "unsupported";
	}
	return "?";
}

/*
 * expected is what asString() would hand the loader: NULL for a null
 * resolution, the input itself for a string, the rendering otherwise.
 */
static void
checkScalar(const char *input, IcebergSiteScalarTag wantTag,
			const char *expected)
{
	size_t		bufsz = ICEBERG_SITE_SCALAR_BUFSZ(strlen(input));
	char	   *buf = malloc(bufsz);
	const char *kind;
	IcebergSiteScalarTag tag;
	const char *got;

	checks++;

	/*
	 * Exactly the size a caller is told to allocate, so a rendering that
	 * outgrows its input -- "no" becoming "false" -- fails here instead of
	 * silently truncating in production.
	 */
	memset(buf, 0, bufsz);
	tag = icebergSiteResolveScalar(input, buf, bufsz, &kind);

	if (tag != wantTag)
	{
		printf("FAIL %-28s tag %s, wanted %s\n",
			   input, tagName(tag), tagName(wantTag));
		failures++;
		free(buf);
		return;
	}

	if (tag == ICEBERG_SITE_SCALAR_NULL || tag == ICEBERG_SITE_SCALAR_UNSUPPORTED)
	{
		free(buf);
		return;
	}

	got = (tag == ICEBERG_SITE_SCALAR_STR) ? input : buf;
	if (strcmp(got, expected) != 0)
	{
		printf("FAIL %-28s got \"%s\", wanted \"%s\"\n", input, got, expected);
		failures++;
	}
	free(buf);
}

/* The two-step parseBoolean() the loader applies to a boolean key. */
static void
checkBooleanKey(const char *input, bool expected)
{
	size_t		bufsz = ICEBERG_SITE_SCALAR_BUFSZ(strlen(input));
	char	   *buf = malloc(bufsz);
	const char *kind;
	IcebergSiteScalarTag tag;
	const char *text;
	bool		got;

	checks++;
	memset(buf, 0, bufsz);
	tag = icebergSiteResolveScalar(input, buf, bufsz, &kind);
	text = (tag == ICEBERG_SITE_SCALAR_STR) ? input : buf;

	if (tag == ICEBERG_SITE_SCALAR_NULL)
	{
		printf("FAIL boolean %-20s resolved to null\n", input);
		failures++;
		free(buf);
		return;
	}

	got = (strcasecmp(text, "true") == 0);
	if (got != expected)
	{
		printf("FAIL boolean %-20s got %s, wanted %s\n",
			   input, got ? "true" : "false", expected ? "true" : "false");
		failures++;
	}
	free(buf);
}

static void
checkUnescape(const char *input, const char *expected)
{
	char		buf[512];

	checks++;
	icebergSiteUnescape(input, buf);
	if (strcmp(buf, expected) != 0)
	{
		printf("FAIL unescape %-20s got \"%s\", wanted \"%s\"\n",
			   input, buf, expected);
		failures++;
	}
}

static void
checkSplit(const char *location, bool wantOk, const char *wantBucket,
		   const char *wantPrefix)
{
	char		bucket[512];
	char		prefix[512];
	bool		ok;

	checks++;
	ok = icebergSiteSplitBucketAndPrefix(location, bucket, prefix);

	if (ok != wantOk)
	{
		printf("FAIL split %-24s ok=%d, wanted %d\n", location, ok, wantOk);
		failures++;
		return;
	}
	if (!ok)
		return;

	if (strcmp(bucket, wantBucket) != 0 || strcmp(prefix, wantPrefix) != 0)
	{
		printf("FAIL split %-24s got (\"%s\", \"%s\"), wanted (\"%s\", \"%s\")\n",
			   location, bucket, prefix, wantBucket, wantPrefix);
		failures++;
	}
}

int
main(void)
{
	/* NULL: an absent key and an empty value are the same thing */
	checkScalar("", ICEBERG_SITE_SCALAR_NULL, NULL);
	checkScalar("~", ICEBERG_SITE_SCALAR_NULL, NULL);
	checkScalar("null", ICEBERG_SITE_SCALAR_NULL, NULL);
	checkScalar("Null", ICEBERG_SITE_SCALAR_NULL, NULL);
	checkScalar("NULL", ICEBERG_SITE_SCALAR_NULL, NULL);
	checkScalar(" ", ICEBERG_SITE_SCALAR_NULL, NULL);
	checkScalar("nulL", ICEBERG_SITE_SCALAR_STR, "nulL");

	/* BOOL: exactly three casings per spelling reach the resolver */
	checkScalar("true", ICEBERG_SITE_SCALAR_BOOL, "true");
	checkScalar("True", ICEBERG_SITE_SCALAR_BOOL, "true");
	checkScalar("TRUE", ICEBERG_SITE_SCALAR_BOOL, "true");
	checkScalar("yes", ICEBERG_SITE_SCALAR_BOOL, "true");
	checkScalar("Yes", ICEBERG_SITE_SCALAR_BOOL, "true");
	checkScalar("YES", ICEBERG_SITE_SCALAR_BOOL, "true");
	checkScalar("on", ICEBERG_SITE_SCALAR_BOOL, "true");
	checkScalar("On", ICEBERG_SITE_SCALAR_BOOL, "true");
	checkScalar("ON", ICEBERG_SITE_SCALAR_BOOL, "true");
	checkScalar("false", ICEBERG_SITE_SCALAR_BOOL, "false");
	checkScalar("False", ICEBERG_SITE_SCALAR_BOOL, "false");
	checkScalar("FALSE", ICEBERG_SITE_SCALAR_BOOL, "false");
	checkScalar("no", ICEBERG_SITE_SCALAR_BOOL, "false");
	checkScalar("No", ICEBERG_SITE_SCALAR_BOOL, "false");
	checkScalar("NO", ICEBERG_SITE_SCALAR_BOOL, "false");
	checkScalar("off", ICEBERG_SITE_SCALAR_BOOL, "false");
	checkScalar("Off", ICEBERG_SITE_SCALAR_BOOL, "false");
	checkScalar("OFF", ICEBERG_SITE_SCALAR_BOOL, "false");

	/* other casings stay strings, and then parseBoolean has its say */
	checkScalar("tRue", ICEBERG_SITE_SCALAR_STR, "tRue");
	checkScalar("yEs", ICEBERG_SITE_SCALAR_STR, "yEs");
	checkScalar("oN", ICEBERG_SITE_SCALAR_STR, "oN");

	/*
	 * The whole truth table for a boolean key.  The asymmetry is real: a
	 * mixed-case "true" is true because parseBoolean ignores case, while a
	 * mixed-case "yes" is false because it never became a Boolean.
	 */
	checkBooleanKey("true", true);
	checkBooleanKey("TRUE", true);
	checkBooleanKey("tRue", true);
	checkBooleanKey("yes", true);
	checkBooleanKey("YES", true);
	checkBooleanKey("on", true);
	checkBooleanKey("yEs", false);
	checkBooleanKey("oN", false);
	checkBooleanKey("false", false);
	checkBooleanKey("no", false);
	checkBooleanKey("off", false);
	checkBooleanKey("1", false);
	checkBooleanKey("0", false);

	/* INT: decimal is a rewrite, the other radices are conversions */
	checkScalar("0", ICEBERG_SITE_SCALAR_INT, "0");
	checkScalar("-0", ICEBERG_SITE_SCALAR_INT, "0");
	checkScalar("8020", ICEBERG_SITE_SCALAR_INT, "8020");
	checkScalar("+8020", ICEBERG_SITE_SCALAR_INT, "8020");
	checkScalar("-8020", ICEBERG_SITE_SCALAR_INT, "-8020");
	checkScalar("1_000", ICEBERG_SITE_SCALAR_INT, "1000");
	checkScalar("-1_000", ICEBERG_SITE_SCALAR_INT, "-1000");
	checkScalar("0x1F", ICEBERG_SITE_SCALAR_INT, "31");
	checkScalar("0b101", ICEBERG_SITE_SCALAR_INT, "5");
	checkScalar("0_7", ICEBERG_SITE_SCALAR_INT, "7");

	/* the one that silently rewrites a credential */
	checkScalar("01234567", ICEBERG_SITE_SCALAR_INT, "342391");

	/* decimal needs no arithmetic, so arbitrary width is exact */
	checkScalar("99999999999999999999999999",
				ICEBERG_SITE_SCALAR_INT, "99999999999999999999999999");

	/* near misses that stay strings */
	checkScalar("08", ICEBERG_SITE_SCALAR_STR, "08");
	checkScalar("08020", ICEBERG_SITE_SCALAR_STR, "08020");
	checkScalar("0B1", ICEBERG_SITE_SCALAR_STR, "0B1");
	checkScalar("0b", ICEBERG_SITE_SCALAR_STR, "0b");
	checkScalar("0x", ICEBERG_SITE_SCALAR_STR, "0x");
	checkScalar("12:xyz", ICEBERG_SITE_SCALAR_STR, "12:xyz");

	/* values a volume section actually holds */
	checkScalar("s3", ICEBERG_SITE_SCALAR_STR, "s3");
	checkScalar("http://minio:9000", ICEBERG_SITE_SCALAR_STR, "http://minio:9000");
	checkScalar("192.168.1.10:8020", ICEBERG_SITE_SCALAR_STR, "192.168.1.10:8020");
	checkScalar("nn1.example.com", ICEBERG_SITE_SCALAR_STR, "nn1.example.com");
	checkScalar("us-east-1", ICEBERG_SITE_SCALAR_STR, "us-east-1");

	/* rejected rather than reproduced: floats */
	checkScalar("1.5", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar("1.50", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar("1e3", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar("1E3", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar("-1.5e-3", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar(".5", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar(".inf", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar("-.INF", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar(".nan", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar(".NaN", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);

	/*
	 * Floats that read as strings if you work from the older PyYAML-style
	 * pattern, which has no exponent-without-a-point branch and no sign on
	 * the leading-point one.  The FLOAT pattern in snakeyaml 2.2 -- the jar
	 * the agent pins -- resolves all five, so this must too: were any of them
	 * to come back as a STR, the parser would be handing gopher "1e3" where
	 * the agent hands it "1000.0".  ._5 is the leading-underscore spelling
	 * the leading-point branch admits.
	 */
	checkScalar("+1e3", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar("+.5", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar("-.5", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar(".5e5", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar("._5", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);

	/* and the near misses that really are strings */
	checkScalar("1e", ICEBERG_SITE_SCALAR_STR, "1e");
	checkScalar("1e+", ICEBERG_SITE_SCALAR_STR, "1e+");
	checkScalar(".e5", ICEBERG_SITE_SCALAR_STR, ".e5");

	/* rejected rather than reproduced: timestamps */
	checkScalar("2020-01-01", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar("2020-1-1 10:20:30", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar("2020-01-01T00:00:00Z", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar("2020-01-01t00:00:00.5+08:00", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar("2020-1-1", ICEBERG_SITE_SCALAR_STR, "2020-1-1");

	/* rejected rather than reproduced: sexagesimal and oversized */
	checkScalar("1:30", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar("1:30.5", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);
	checkScalar("0xFFFFFFFFFFFFFFFFF", ICEBERG_SITE_SCALAR_UNSUPPORTED, NULL);

	/* unescapeString: two hex digits, a sign allowed, no whitespace */
	checkUnescape("plain", "plain");
	checkUnescape("a%41b", "aAb");
	checkUnescape("%41", "A");
	checkUnescape("%4", "%4");
	checkUnescape("a%2", "a%2");
	checkUnescape("100%", "100%");
	checkUnescape("%zz", "%zz");
	checkUnescape("%-1x", "%-1x");
	checkUnescape("% 1x", "% 1x");
	checkUnescape("%2f", "/");
	checkUnescape("%2F", "/");

	/* parserBucketAndPrefix, latent scheme bug included */
	checkSplit("mybucket/path", true, "mybucket", "path");
	checkSplit("/mybucket/path/", true, "mybucket", "path");
	checkSplit("mybucket", true, "mybucket", "");
	checkSplit("mybucket/a/b/c", true, "mybucket", "a/b/c");
	checkSplit("s3a://mybucket/path", true, "s3a:", "/mybucket/path");
	checkSplit("///", false, NULL, NULL);
	checkSplit("", false, NULL, NULL);

	printf("%d checks, %d failures\n", checks, failures);
	return failures == 0 ? 0 : 1;
}
