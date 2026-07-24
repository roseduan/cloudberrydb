#pragma once

#include <algorithm>
#include <type_traits>
#include <parquet/internal/arrow/util/endian.h>

extern "C"
{
#include "utils/numeric.h"
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
	 * bits * log10(2) approximated as bits * 77 / 256 always underestimates,
	 * so the true answer is either 'estimate' or 'estimate + 1'.
	 */
	int bits;
	uint64_t hi = (uint64_t)((unsigned __int128)n >> 64);
	if (hi != 0)
		bits = 128 - __builtin_clzll(hi);
	else
		bits = 64 - __builtin_clzll((uint64_t)n);

	int estimate = (bits * 77) >> 8;
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