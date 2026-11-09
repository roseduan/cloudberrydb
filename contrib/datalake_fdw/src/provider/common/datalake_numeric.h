#pragma once

#include <algorithm>
#include <type_traits>
#include <parquet/internal/arrow/util/endian.h>

extern "C"
{
#include "fmgr.h"
#include "utils/fmgrprotos.h"
#include "utils/numeric.h"
#include "varatt.h"
}

/*
 * Declared numeric(p,s) versus the precision and scale the data file carries.
 *
 * A Parquet DECIMAL stores its own precision and scale, and decoding with
 * those alone ignores the numeric(p,s) the foreign table declares: values
 * come back neither rounded to the declared scale nor rejected when they
 * need more digits than the declared precision allows (#487).  A declaration
 * on a foreign table is a statement about the data, not a placeholder, so the
 * readers convert to it exactly as PostgreSQL would for a local column.
 *
 * numeric.c keeps its typmod accessors to itself (static inline, not in
 * numeric.h), hence the copies here.  Note the scale is an 11-bit SIGNED
 * field -- this kernel accepts a negative numeric scale -- so unpacking it
 * needs numeric.c's sign-extending bit hack, not a plain mask.
 */
#define DL_NUMERIC_TYPMOD_PRECISION(tm) \
	((int) ((((uint32) ((tm) - VARHDRSZ)) >> 16) & 0xffff))
#define DL_NUMERIC_TYPMOD_SCALE(tm) \
	((int) (((((uint32) ((tm) - VARHDRSZ)) & 0x7ff) ^ 1024) - 1024))

/*
 * Does reading a DECIMAL(filePrecision, fileScale) as numeric(p,s) change any
 * value?
 *
 * No for a bare numeric (typmod below VARHDRSZ is numeric.c's "no typmod":
 * the file's own scale is then the right answer), and no when the declared
 * scale already matches and the declared precision admits every value the
 * file schema does -- the conversion is the identity there, so the fast
 * decode paths stay intact.
 *
 * Also no when the file column is not a DECIMAL at all: libparquet reports no
 * meaningful precision/scale for those, so there is nothing to convert from
 * and feeding those placeholder numbers to a conversion would be worse than
 * the pre-existing silence.  (That silence -- a plain INT64 read through a
 * narrow numeric(p,s) -- is a separate gap, not this one.)
 */
static inline bool
numericTypmodApplies(int32 typmod, int filePrecision, int fileScale)
{
	if (typmod < (int32) VARHDRSZ)
		return false;
	if (filePrecision <= 0 || fileScale < 0)
		return false;

	return !(DL_NUMERIC_TYPMOD_SCALE(typmod) == fileScale &&
			 DL_NUMERIC_TYPMOD_PRECISION(typmod) >= filePrecision);
}

/*
 * Can the declared typmod be reached by rescaling an unscaled int128 in
 * place, rather than by building a NUMERIC and handing it to the kernel?
 *
 * Only for a non-negative declared scale within a precision an int128 holds
 * with room to spare.  Every value that survives then satisfies
 * |v| < 10^precision <= 10^35, which keeps both the rescale itself and the
 * engine's 120-bit numeric128 encoding (see encodeNumeric128) in range.
 * Wider or negative-scale declarations are rare and go the allocating way.
 */
#define DL_NUMERIC_INPLACE_MAX_PRECISION 35

static inline bool
numericTypmodRescalesInPlace(int32 typmod)
{
	int			prec = DL_NUMERIC_TYPMOD_PRECISION(typmod);
	int			scale = DL_NUMERIC_TYPMOD_SCALE(typmod);

	return (scale >= 0 && scale <= DL_NUMERIC_INPLACE_MAX_PRECISION &&
			prec >= 1 && prec <= DL_NUMERIC_INPLACE_MAX_PRECISION);
}

/* 10^n for 0 <= n <= 38, which is all an int128 holds. */
static inline __int128
numericPowerOfTen(int n)
{
	__int128	r = 1;

	for (int i = 0; i < n; i++)
		r *= 10;
	return r;
}

/*
 * Restate an unscaled decimal read at fileScale in the scale the declared
 * numeric(p,s) asks for: rounds half away from zero, as numeric_round() does,
 * and raises "numeric field overflow" when the declared precision cannot hold
 * the result -- the same test apply_typmod() makes, |value| < 10^precision
 * once the value is expressed at the declared scale.
 *
 * Only for typmods numericTypmodRescalesInPlace() accepts, so every bound
 * below stays inside an int128.
 */
static inline __int128
numericRescaleUnscaled(__int128 v, int fileScale, int32 typmod)
{
	int			declScale = DL_NUMERIC_TYPMOD_SCALE(typmod);
	int			declPrec = DL_NUMERIC_TYPMOD_PRECISION(typmod);
	__int128	limit = numericPowerOfTen(declPrec);

	if (declScale < fileScale)
	{
		int			drop = fileScale - declScale;
		__int128	div;
		__int128	q;
		__int128	r;

		if (drop > 38)
			return 0;			/* every digit dropped; |v| < 10^38 rounds to 0 */

		div = numericPowerOfTen(drop);
		q = v / div;
		r = v % div;
		if (r < 0)
			r = -r;
		if (r >= div - r)		/* |r| * 2 >= div, without overflowing */
			q += (v < 0) ? -1 : 1;
		v = q;
	}
	else if (declScale > fileScale)
	{
		int			pad = declScale - fileScale;
		__int128	mul;
		__int128	room;

		/*
		 * Bound the value before multiplying, not after: anything that would
		 * not fit the declared precision afterwards cannot fit now either.
		 */
		if (pad > declPrec)
			goto overflow;
		mul = numericPowerOfTen(pad);
		room = limit / mul;
		if (v >= room || v <= -room)
			goto overflow;
		v *= mul;
	}

	if (v >= limit || v <= -limit)
		goto overflow;
	return v;

overflow:
	ereport(ERROR,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("numeric field overflow"),
			 errdetail("A field with precision %d, scale %d must round to an "
					   "absolute value less than %s%d.",
					   declPrec, declScale,
					   declPrec > declScale ? "10^" : "",
					   declPrec > declScale ? declPrec - declScale : 1)));
	return 0;					/* unreachable */
}

/*
 * Apply the declared typmod to an already decoded NUMERIC: rounds to the
 * declared scale and throws "numeric field overflow" when the value does not
 * fit the declared precision.  Returns a freshly palloc'd numeric, so callers
 * serving values out of a reusable buffer must return this datum rather than
 * their buffer.
 */
static inline Datum
numericApplyTypmod(Datum value, int32 typmod)
{
	return DirectFunctionCall2(numeric, value, Int32GetDatum(typmod));
}

/*
 * Hand back a NUMERIC decoded into buf, converting it to the declared
 * numeric(p,s) first when that conversion is not the identity.  The
 * conversion allocates, so the result then no longer aliases buf.
 */
static inline Datum
numericDecodedResult(void *buf, bool applyTypmod, int32 typmod)
{
	Datum		d = NumericGetDatum((Numeric) buf);

	return applyTypmod ? numericApplyTypmod(d, typmod) : d;
}

template<typename T>
struct IntDigitsTraits;

template<> struct IntDigitsTraits<int32_t> {
	static constexpr int digits = 10 / DEC_DIGITS + 1; // int32_digits
};

template<> struct IntDigitsTraits<int64_t> {
	static constexpr int digits = 19 / DEC_DIGITS + 1; // int64_digits
};

template<> struct IntDigitsTraits<__int128> {
	static constexpr int digits = 40 / DEC_DIGITS + 1; // int128_digits
};

int128 FLBA_to_int128(const uint8 *bytes, int length);

/*
 * Inlined to eliminate PLT overhead (~1.4% of FDW CPU).
 * Previously in datalake_numeric.cpp, called from int_to_numeric_with_scale
 * template in this header — crossing the .cpp boundary caused a PLT jump.
 */
#define NUMERIC_CAN_BE_SHORT_INLINE(scale,weight) \
	((scale) <= NUMERIC_SHORT_DSCALE_MAX && \
	(weight) <= NUMERIC_SHORT_WEIGHT_MAX && \
	(weight) >= NUMERIC_SHORT_WEIGHT_MIN)

static inline int
fill_numeric_result(NumericVar *var, Numeric result)
{
	NumericDigit *digits = var->digits;
	int			weight = var->weight;
	int			sign = var->sign;
	uint32_t	n;
	Size		len;
	n = var->ndigits;

	while (n > 0 && *digits == 0) { digits++; weight--; n--; }
	while (n > 0 && digits[n - 1] == 0) n--;

	if (n == 0) { weight = 0; sign = NUMERIC_POS; }

	if (NUMERIC_CAN_BE_SHORT_INLINE(var->dscale, weight))
	{
		len = NUMERIC_HDRSZ_SHORT + n * sizeof(NumericDigit);
		SET_VARSIZE(result, len);
		result->choice.n_short.n_header =
			(sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK)
			 : NUMERIC_SHORT)
			| (var->dscale << NUMERIC_SHORT_DSCALE_SHIFT)
			| (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0)
			| (weight & NUMERIC_SHORT_WEIGHT_MASK);
	}
	else
	{
		len = NUMERIC_HDRSZ + n * sizeof(NumericDigit);
		SET_VARSIZE(result, len);
		result->choice.n_long.n_sign_dscale =
			sign | (var->dscale & NUMERIC_DSCALE_MASK);
		result->choice.n_long.n_weight = weight;
	}

	if (n > 0)
		memcpy(NUMERIC_DIGITS(result), digits, n * sizeof(NumericDigit));

	/* Guard against int16 header field overflow */
	if (NUMERIC_WEIGHT(result) != weight ||
		NUMERIC_DSCALE(result) != var->dscale)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value overflows numeric format")));
	return len;
}

/*
 * Convert Parquet FLBA (big-endian) to int64.
 * For DECIMAL columns with precision <= 18, the value always fits in int64
 * even when stored in a wider FLBA (e.g., 16 bytes).  This avoids the
 * expensive FLBA_to_int128 + __int128 division path.
 */
static inline int64_t FLBA_to_int64(const uint8_t *bytes, int length)
{
	const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;
	int64_t result = is_negative ? INT64_C(-1) : INT64_C(0);

	/* Copy up to 8 bytes from the tail of the big-endian FLBA */
	int copy_len = length <= 8 ? length : 8;
	memcpy(reinterpret_cast<uint8_t *>(&result) + 8 - copy_len,
		   bytes + length - copy_len, copy_len);

	return static_cast<int64_t>(__builtin_bswap64(static_cast<uint64_t>(result)));
}

constexpr std::array<__int128_t, 39> makePowerTable() {
	std::array<__int128_t, 39> table{};
	__int128_t value = 1; // 10^0
	table[0] = value;
	for (size_t i = 1; i < 39; ++i) {
		value *= 10;
		table[i] = value;
	}
	return table;
}

inline int getPowerOf10(__int128_t n) {
	static constexpr std::array<__int128_t, 39> POWER_TABLE = makePowerTable();
	if (n <= 0) return -1;

	/*
	 * Use CLZ (count leading zeros) to estimate the decimal digit count,
	 * replacing the binary search (std::upper_bound) over 39 __int128 entries.
	 *
	 * The single fixup below (return estimate or estimate + 1) is only valid
	 * if the estimate is never more than one short of the true digit count,
	 * so the approximation of log10(2) must be tight enough that
	 *     floor(bits * approx) >= digits(n) - 1
	 * holds for every bit length up to 128.
	 *
	 * 77/256 = 0.30078125 is NOT tight enough: against log10(2) = 0.3010300
	 * it loses 0.00025 per bit, which reaches a full digit by bits = 103.
	 * At bits = 103, 113 and 123 (i.e. around 10^31, 10^34 and 10^37) the
	 * estimate fell two short and this returned digits(n) - 1, which made
	 * int_to_numeric_with_scale() compute a weight one NBASE digit too low
	 * and decode e.g. DECIMAL(38,37) 1.0 as 0.0001.
	 *
	 * 1233/4096 = 0.30102539 is within 5e-7 of log10(2), so the shortfall
	 * stays below 7e-5 of a digit across the whole 128-bit range.  Verified
	 * exhaustively against every bit-length boundary and every power of ten,
	 * plus 20M random values.
	 */
	int bits;
	uint64_t hi = (uint64_t)((unsigned __int128)n >> 64);
	if (hi != 0)
		bits = 128 - __builtin_clzll(hi);
	else
		bits = 64 - __builtin_clzll((uint64_t)n);

	int estimate = (bits * 1233) >> 12;
	if (estimate >= 39) return -1;
	if (n < POWER_TABLE[estimate]) return estimate;
	if (estimate + 1 >= 39) return -1;
	return estimate + 1;
}

/*
 * Fast __int128 division+modulo by NBASE (10000), avoiding the expensive
 * __divti3 software routine.  Decomposes into three 64-bit divisions by
 * the constant 10000, each of which the compiler turns into a multiply-shift
 * sequence with no actual division instruction.
 */
static inline unsigned __int128
fast_divmod_NBASE(unsigned __int128 val, NumericDigit *remainder)
{
	uint64_t hi = (uint64_t)(val >> 64);
	uint64_t lo = (uint64_t)val;

	uint64_t q1 = hi / NBASE;
	uint64_t r1 = hi % NBASE;

	uint64_t lo_hi = lo >> 32;
	uint64_t lo_lo = lo & 0xFFFFFFFFULL;

	uint64_t mid = r1 * 0x100000000ULL + lo_hi;
	uint64_t q2 = mid / NBASE;
	uint64_t r2 = mid % NBASE;

	uint64_t bot = r2 * 0x100000000ULL + lo_lo;
	uint64_t q3 = bot / NBASE;

	*remainder = (NumericDigit)(bot - q3 * NBASE);
	return ((unsigned __int128)q1 << 64) | ((uint64_t)q2 << 32) | q3;
}

template<typename T>
int int_to_numeric_with_scale(T val, int scale, Numeric dest)
{
	static constexpr int16 scale_factors[] = {
		1, 1, 10, 100, 1000, 10000
	};

	/*
	 * Fast path for the overwhelmingly common case: DECIMAL(p<=9, s<=4)
	 * money-style columns (TPC-DS/TPC-H use DECIMAL(7,2) throughout).  An
	 * unscaled |val| < 10^8 with scale <= DEC_DIGITS produces at most two
	 * integer base-10000 digits plus one fraction digit, so we can build
	 * the digits and the short varlena header straight into dest, skipping
	 * getPowerOf10(), the generic padding loop and the NumericVar round
	 * trip.  The output is byte-identical to the generic path below
	 * (same leading/trailing zero stripping, same short-header layout).
	 * weight here is always in [-1, 1], so the short header always fits.
	 */
	if (scale >= 0 && scale <= DEC_DIGITS)
	{
		bool	is_neg = val < 0;
		/*
		 * Compute the magnitude in an unsigned domain wide enough for every T
		 * (int32/int64/__int128).  Negating a signed value directly would be
		 * undefined behaviour when val == T::min(); a wrapped result would
		 * still look negative, pass the range guard below, and then truncate
		 * to a wrong NUMERIC instead of falling through to the generic path.
		 */
		unsigned __int128 aval = is_neg ? (unsigned __int128) 0 - (unsigned __int128) val
										: (unsigned __int128) val;

		if (aval < (unsigned __int128) 100000000)
		{
			static constexpr uint32_t pow10[5] = {1, 10, 100, 1000, 10000};
			uint32_t	v = (uint32_t) aval;
			uint32_t	p = pow10[scale];
			uint32_t	rest = v / p;
			uint32_t	frac = v - rest * p;
			NumericDigit d[3];
			int			n = 0;
			int			weight;

			if (rest >= 10000)
			{
				d[0] = (NumericDigit) (rest / 10000);
				d[1] = (NumericDigit) (rest % 10000);
				n = 2;
				weight = 1;
				if (frac == 0 && d[1] == 0)
					n = 1;			/* strip trailing zero digit */
			}
			else if (rest > 0)
			{
				d[0] = (NumericDigit) rest;
				n = 1;
				weight = 0;
			}
			else
				weight = -1;		/* no integer digits */

			if (frac != 0)
				d[n++] = (NumericDigit) (frac * pow10[DEC_DIGITS - scale]);

			if (n == 0)
			{
				weight = 0;			/* canonical zero: no digits, positive */
				is_neg = false;
			}

			Size len = NUMERIC_HDRSZ_SHORT + n * sizeof(NumericDigit);

			SET_VARSIZE(dest, len);
			dest->choice.n_short.n_header =
				(is_neg ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK)
						: NUMERIC_SHORT)
				| (scale << NUMERIC_SHORT_DSCALE_SHIFT)
				| (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0)
				| (weight & NUMERIC_SHORT_WEIGHT_MASK);

			NumericDigit *out = NUMERIC_DIGITS(dest);
			for (int i = 0; i < n; i++)
				out[i] = d[i];

			return (int) len;
		}
	}

	/*
	 * Use a stack-allocated digit buffer instead of palloc/pfree via
	 * alloc_numeric_var/free_numeric_var.  This eliminates 4 PLT calls
	 * (init_numeric_var, alloc_numeric_var, free_numeric_var, and the
	 * underlying palloc/pfree) per DECIMAL value per row.
	 *
	 * +1 for the spare leading digit slot that alloc_numeric_var reserves.
	 */
	static constexpr int max_digits = IntDigitsTraits<T>::digits;
	NumericDigit digit_buf[max_digits + 1];
	NumericVar numeric;

	digit_buf[0] = 0;
	numeric.ndigits = max_digits;
	numeric.weight = 0;
	numeric.sign = NUMERIC_POS;
	numeric.dscale = 0;
	numeric.buf = digit_buf;
	numeric.digits = digit_buf + 1;

	bool is_negative = val < 0;
	numeric.sign = is_negative ? NUMERIC_NEG : NUMERIC_POS;
	val = is_negative ? -val : val;
	numeric.dscale = scale;

	T temp = val;
	int nweight = 0;

	nweight = getPowerOf10(temp);

	NumericDigit *ptr = numeric.digits + numeric.ndigits;

	int dweight = nweight - scale - 1;
	int weight = dweight < 0 ? (dweight + 1) / DEC_DIGITS - 1 : dweight / DEC_DIGITS;
	int offset = (weight + 1) * DEC_DIGITS - (dweight + 1);
	int scale_padding = (offset + nweight) % DEC_DIGITS;
	bool padding_done = scale_padding == 0;
	int ndigits = 0;
	while (val) {
		ptr--;
		ndigits++;

		if (!padding_done)
		{
			int16 pow_padding = 10 * scale_factors[scale_padding];
			int16 pow_padding_remain = 10 * scale_factors[DEC_DIGITS - scale_padding];

			temp = val / pow_padding;
			*ptr = (val - (temp * pow_padding)) * pow_padding_remain;
			val = temp;
			padding_done = true;
		}
		else
		{
			if constexpr (std::is_same_v<T, __int128>)
			{
				NumericDigit rem;
				val = (T)fast_divmod_NBASE((unsigned __int128)val, &rem);
				*ptr = rem;
			}
			else
			{
				temp = val / NBASE;
				*ptr = val - (temp * NBASE);
				val = temp;
			}
		}
	}

	numeric.digits = ptr;
	numeric.ndigits = ndigits;
	numeric.weight = weight;
	return fill_numeric_result(&numeric, dest);
	/* No free needed — digit_buf is on the stack */
}

void numeric_to_FLBA(Numeric num, char *res);