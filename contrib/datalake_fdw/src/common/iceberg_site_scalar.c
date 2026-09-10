/*-------------------------------------------------------------------------
 *
 * iceberg_site_scalar.c
 *    YAML 1.1 scalar resolution, reproduced from SnakeYAML.
 *
 * libyaml hands back the raw text of a scalar; SnakeYAML hands the agent a
 * Boolean, an Integer or a Date, and the agent then calls toString() on it.
 * Reading the raw text would therefore make the two sides disagree on
 * "path_style_access: yes" (true, not the string "yes"), on
 * "secret_access_key: 01234567" (the octal 342391, not the digits typed),
 * and on an empty value (an absent key, not an empty string).  This file
 * closes that gap.
 *
 * Three resolutions are reported as unsupported rather than reproduced:
 * floats, timestamps and sexagesimal numbers.  Their Java renderings depend
 * on Double.toString(), on the JVM's default timezone, and on a 32-bit
 * accumulator inside SnakeYAML -- none of them things to freeze into a wire
 * format, and none of them a meaningful value for an endpoint, a bucket or
 * a credential.  Quoting such a value makes both sides agree exactly.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/iceberg_site_scalar.c
 *-------------------------------------------------------------------------
 */
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "iceberg_site_scalar.h"

#define lengthof_local(a)	(sizeof(a) / sizeof((a)[0]))

static bool
matchesAny(const char *s, const char *const *list, size_t nlist)
{
	size_t		i;

	for (i = 0; i < nlist; i++)
	{
		if (strcmp(s, list[i]) == 0)
			return true;
	}

	return false;
}

/*
 * The resolver's NULL pattern, plus the separate empty-scalar rule.  The
 * lone space is in the pattern as written; a plain scalar can never be one,
 * but reproducing the pattern faithfully costs nothing.
 */
static bool
isNullScalar(const char *s)
{
	static const char *const forms[] = {"~", "null", "Null", "NULL", " "};

	return (s[0] == '\0' || matchesAny(s, forms, lengthof_local(forms)));
}

/*
 * The BOOL pattern lists exactly three casings of each spelling.  Anything
 * else stays a string, which Boolean.parseBoolean() then accepts only when
 * it reads "true" in any casing -- so "tRue" is true while "yEs" is false.
 * Callers get that second step for free by comparing the rendering here
 * against "true" case-insensitively.
 */
static bool
boolScalar(const char *s, bool *value)
{
	static const char *const trueForms[] = {
		"true", "True", "TRUE", "yes", "Yes", "YES", "on", "On", "ON"
	};
	static const char *const falseForms[] = {
		"false", "False", "FALSE", "no", "No", "NO", "off", "Off", "OFF"
	};

	if (matchesAny(s, trueForms, lengthof_local(trueForms)))
	{
		*value = true;
		return true;
	}
	if (matchesAny(s, falseForms, lengthof_local(falseForms)))
	{
		*value = false;
		return true;
	}

	return false;
}

/*
 * Accumulate the digits of one radix, skipping the underscores YAML allows
 * anywhere among them.  Returns false when this is not a well-formed
 * literal of that radix; sets *overflow when it is well-formed but larger
 * than we are willing to render.
 */
static bool
scanRadix(const char *s, int radix, uint64_t *value, bool *overflow)
{
	bool		sawDigit = false;

	*value = 0;
	*overflow = false;

	for (; *s != '\0'; s++)
	{
		int			digit;

		if (*s == '_')
			continue;

		if (*s >= '0' && *s <= '9')
			digit = *s - '0';
		else if (*s >= 'a' && *s <= 'f')
			digit = *s - 'a' + 10;
		else if (*s >= 'A' && *s <= 'F')
			digit = *s - 'A' + 10;
		else
			return false;

		if (digit >= radix)
			return false;

		sawDigit = true;

		if (!*overflow)
		{
			if (*value > (UINT64_C(0x7FFFFFFFFFFFFFFF) - digit) / radix)
				*overflow = true;
			else
				*value = *value * radix + digit;
		}
	}

	return sawDigit;
}

/*
 * The INT pattern, minus its sexagesimal branch, which isSexagesimalScalar()
 * claims first.  Decimal needs no arithmetic: the pattern forbids leading
 * zeros, so the rendering is the literal with underscores and a leading '+'
 * removed.  The other radices are converted, and anything too large to hold
 * is reported rather than silently truncated -- BigInteger would print it in
 * full and we would not.
 */
static bool
intScalar(const char *s, char *out, size_t outlen, bool *overflow)
{
	bool		negative = false;
	const char *body = s;
	uint64_t	value;

	*overflow = false;

	if (*body == '+' || *body == '-')
	{
		negative = (*body == '-');
		body++;
	}

	if (body[0] == '0' && body[1] == 'b')
	{
		if (!scanRadix(body + 2, 2, &value, overflow))
			return false;
	}
	else if (body[0] == '0' && body[1] == 'x')
	{
		if (!scanRadix(body + 2, 16, &value, overflow))
			return false;
	}
	else if (body[0] == '0' && body[1] != '\0')
	{
		/* a leading zero followed by anything can only be octal */
		if (!scanRadix(body + 1, 8, &value, overflow))
			return false;
	}
	else
	{
		size_t		n = 0;
		const char *p;

		if (body[0] == '0')
		{
			/* the pattern admits a bare zero and nothing else */
			if (body[1] != '\0')
				return false;
			snprintf(out, outlen, "0");
			return true;
		}
		if (body[0] < '1' || body[0] > '9')
			return false;

		if (negative)
			out[n++] = '-';

		for (p = body; *p != '\0'; p++)
		{
			if (*p == '_')
				continue;
			if (*p < '0' || *p > '9')
				return false;
			if (n + 1 >= outlen)
				return false;
			out[n++] = *p;
		}
		out[n] = '\0';
		return true;
	}

	if (*overflow)
		return true;

	/* BigInteger has no negative zero */
	if (value == 0)
		snprintf(out, outlen, "0");
	else
		snprintf(out, outlen, "%s%llu", negative ? "-" : "",
				 (unsigned long long) value);

	return true;
}

/* Match between nmin and nmax digits, advancing p when it succeeds. */
static bool
scanDigits(const char **p, int nmin, int nmax)
{
	const char *s = *p;
	int			n = 0;

	while (n < nmax && s[n] >= '0' && s[n] <= '9')
		n++;

	if (n < nmin)
		return false;

	*p = s + n;
	return true;
}

/* The FLOAT pattern's optional (or, after 'e', mandatory) exponent. */
static bool
scanExponent(const char **p, bool required)
{
	const char *s = *p;

	if (*s != 'e' && *s != 'E')
		return !required;

	s++;
	if (*s == '+' || *s == '-')
		s++;
	if (!scanDigits(&s, 1, INT_MAX))
		return false;

	*p = s;
	return true;
}

static void
scanDigitsOrUnderscores(const char **p)
{
	const char *s = *p;

	while ((*s >= '0' && *s <= '9') || *s == '_')
		s++;

	*p = s;
}

/*
 * True when the resolver would call this a float.  The pattern, read out of
 * the constant pool of Resolver.class in the snakeyaml-2.2.jar the agent
 * pins, is six branches:
 *
 *     [-+]?[0-9][0-9_]*\.[0-9_]*([eE][-+]?[0-9]+)?    decimal
 *   | [-+]?[0-9][0-9_]*[eE][-+]?[0-9]+                exponent, no point
 *   | [-+]?\.[0-9_]+([eE][-+]?[0-9]+)?                leading point
 *   | [-+]?[0-9][0-9_]*(:[0-5]?[0-9])+\.[0-9_]*       sexagesimal
 *   | [-+]?\.(inf|Inf|INF)
 *   | \.(nan|NaN|NAN)
 *
 * The middle two are worth spelling out here, because the older pattern --
 * the one PyYAML still carries, and the one a reader is most likely to
 * remember -- has neither: it wants a point in the decimal branch and takes
 * no sign in the leading-point one.  Under the pattern actually in the jar,
 * "1e3", "+.5" and ".5e5" are floats, and Double.toString() renders them
 * "1000.0", "0.5" and "50000.0".  Reading them as strings here would
 * therefore not be a narrowing, it would be the silent mismatch this whole
 * file exists to prevent -- the agent writing to one endpoint, the segments
 * to another.
 */
static bool
isFloatScalar(const char *s)
{
	const char *p = s;

	if (strcmp(s, ".nan") == 0 || strcmp(s, ".NaN") == 0 ||
		strcmp(s, ".NAN") == 0)
		return true;

	if (*p == '+' || *p == '-')
		p++;

	if (*p == '.')
	{
		p++;
		if (strcmp(p, "inf") == 0 || strcmp(p, "Inf") == 0 ||
			strcmp(p, "INF") == 0)
			return true;

		/*
		 * "\.[0-9_]+" and not "\.[0-9][0-9_]*", so the underscore may come
		 * first: "._5" is a float, printed by Java as "0.5".
		 */
		if ((*p < '0' || *p > '9') && *p != '_')
			return false;
		scanDigitsOrUnderscores(&p);
		return (scanExponent(&p, false) && *p == '\0');
	}

	if (*p < '0' || *p > '9')
		return false;
	scanDigitsOrUnderscores(&p);

	if (*p == '.')
	{
		p++;
		scanDigitsOrUnderscores(&p);
		return (scanExponent(&p, false) && *p == '\0');
	}

	if (*p == 'e' || *p == 'E')
		return (scanExponent(&p, true) && *p == '\0');

	/*
	 * The one remaining branch is the sexagesimal float, which
	 * isSexagesimalScalar() has already claimed.
	 */
	return false;
}

/*
 * True for the base-60 spellings the INT and FLOAT patterns both carry,
 * "1:30" and "1:30.5".  Reported on its own so the error can name it.
 */
static bool
isSexagesimalScalar(const char *s)
{
	const char *p = s;
	bool		sawGroup = false;

	if (*p == '+' || *p == '-')
		p++;
	if (*p < '1' || *p > '9')
		return false;

	scanDigitsOrUnderscores(&p);

	while (*p == ':')
	{
		p++;
		if (!scanDigits(&p, 1, 2))
			return false;
		sawGroup = true;
	}

	if (!sawGroup)
		return false;

	if (*p == '.')
	{
		p++;
		scanDigitsOrUnderscores(&p);
	}

	return (*p == '\0');
}

/* True when the resolver would call this a timestamp. */
static bool
isTimestampScalar(const char *s)
{
	const char *p = s;

	if (!scanDigits(&p, 4, 4) || *p != '-')
		return false;
	p++;
	if (!scanDigits(&p, 1, 2) || *p != '-')
		return false;
	p++;
	if (!scanDigits(&p, 1, 2))
		return false;

	/* the date-only branch spells month and day with two digits each */
	if (*p == '\0')
		return (strlen(s) == 10);

	if (*p == 'T' || *p == 't')
		p++;
	else if (*p == ' ' || *p == '\t')
	{
		while (*p == ' ' || *p == '\t')
			p++;
	}
	else
		return false;

	if (!scanDigits(&p, 1, 2) || *p != ':')
		return false;
	p++;
	if (!scanDigits(&p, 2, 2) || *p != ':')
		return false;
	p++;
	if (!scanDigits(&p, 2, 2))
		return false;

	if (*p == '.')
	{
		p++;
		scanDigits(&p, 0, INT_MAX);
	}

	if (*p == '\0')
		return true;

	while (*p == ' ' || *p == '\t')
		p++;

	if (*p == 'Z')
		return (*(p + 1) == '\0');

	if (*p == '+' || *p == '-')
	{
		p++;
		if (!scanDigits(&p, 1, 2))
			return false;
		if (*p == ':')
		{
			p++;
			if (!scanDigits(&p, 2, 2))
				return false;
		}
		return (*p == '\0');
	}

	return false;
}

IcebergSiteScalarTag
icebergSiteResolveScalar(const char *text, char *out, size_t outlen,
						 const char **unsupportedKind)
{
	bool		bval;
	bool		overflow;

	*unsupportedKind = NULL;

	if (isNullScalar(text))
		return ICEBERG_SITE_SCALAR_NULL;

	if (boolScalar(text, &bval))
	{
		snprintf(out, outlen, "%s", bval ? "true" : "false");
		return ICEBERG_SITE_SCALAR_BOOL;
	}

	if (isSexagesimalScalar(text))
	{
		*unsupportedKind = "sexagesimal number";
		return ICEBERG_SITE_SCALAR_UNSUPPORTED;
	}

	if (intScalar(text, out, outlen, &overflow))
	{
		if (overflow)
		{
			*unsupportedKind = "integer too large to represent";
			return ICEBERG_SITE_SCALAR_UNSUPPORTED;
		}
		return ICEBERG_SITE_SCALAR_INT;
	}

	if (isFloatScalar(text))
	{
		*unsupportedKind = "floating-point number";
		return ICEBERG_SITE_SCALAR_UNSUPPORTED;
	}

	if (isTimestampScalar(text))
	{
		*unsupportedKind = "timestamp";
		return ICEBERG_SITE_SCALAR_UNSUPPORTED;
	}

	return ICEBERG_SITE_SCALAR_STR;
}

size_t
icebergSiteUnescape(const char *path, char *out)
{
	size_t		len = strlen(path);
	size_t		i;
	size_t		n = 0;

	for (i = 0; i < len; i++)
	{
		if (path[i] == '%' && i + 2 < len)
		{
			char		hex[3];
			char	   *endptr;
			long		code;

			hex[0] = path[i + 1];
			hex[1] = path[i + 2];
			hex[2] = '\0';

			/*
			 * Integer.parseInt() takes a sign, so "%+F" decodes while "%-1"
			 * yields a negative code and is left alone.  Unlike strtol() it
			 * does not skip leading whitespace, so "% 1" must not decode.
			 */
			errno = 0;
			code = strtol(hex, &endptr, 16);
			if (errno == 0 && endptr != hex && *endptr == '\0' && code >= 0 &&
				hex[0] != ' ' && hex[0] != '\t')
			{
				out[n++] = (char) code;
				i += 2;
				continue;
			}
		}

		out[n++] = path[i];
	}

	out[n] = '\0';
	return n;
}

bool
icebergSiteSplitBucketAndPrefix(const char *location, char *bucket,
								char *prefix)
{
	size_t		len = strlen(location);
	size_t		start = 0;
	const char *slash;

	while (start < len && location[start] == '/')
		start++;
	while (len > start && location[len - 1] == '/')
		len--;

	if (len == start)
		return false;			/* Java: pathParts[0].isEmpty() */

	slash = memchr(location + start, '/', len - start);
	if (slash == NULL)
	{
		memcpy(bucket, location + start, len - start);
		bucket[len - start] = '\0';
		prefix[0] = '\0';
		return true;
	}

	memcpy(bucket, location + start, slash - (location + start));
	bucket[slash - (location + start)] = '\0';
	if (bucket[0] == '\0')
		return false;

	memcpy(prefix, slash + 1, (location + len) - (slash + 1));
	prefix[(location + len) - (slash + 1)] = '\0';
	return true;
}
