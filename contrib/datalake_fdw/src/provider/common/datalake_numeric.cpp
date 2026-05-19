#include "datalake_numeric.h"

#define NUMERIC_SHORT_HEADER_SIZE 2

int128 FLBA_to_int128(const uint8 *bytes, int length)
{
	static constexpr int32_t kMinDecimalBytes = 1;
	static constexpr int32_t kMaxDecimalBytes = 16;

	if (length < kMinDecimalBytes || length > kMaxDecimalBytes)
	{
		elog(ERROR, "overflow");
	}

	const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;
	int128 result = 0;
	if (is_negative)
	{
		memset(&result, 0xff, sizeof(result));
	}
	memcpy(reinterpret_cast<uint8_t*>(&result) + kMaxDecimalBytes - length, bytes, length);
	int128 high_bits = PARQUET_ARROW_BYTE_SWAP64((result >> 64) & 0xffffffffffffffffL);
	int128 low_bits = PARQUET_ARROW_BYTE_SWAP64(result & 0xffffffffffffffffL);
	result = high_bits + (low_bits << 64);
	return result;
}

void numeric_to_FLBA(Numeric num, char *res)
{
	int scale, weight, ndigits, num_len;
	NumericDigit *digits;
	bool neg;
	int i = 0;
	int128 frac_val = 0, val = 0;
	uint64_t *n_low = (uint64_t*) res;
	uint64_t *n_high = (uint64_t*) (res + sizeof(uint64_t));
	static constexpr std::array<__int128_t, 39> POWER_TABLE = makePowerTable();


    num_len = VARSIZE_ANY_EXHDR((struct varlena *)DatumGetPointer(num));
	scale = NUMERIC_DSCALE(num);
	weight = NUMERIC_WEIGHT(num);
	digits = NUMERIC_DIGITS(num);
	ndigits = (num_len - NUMERIC_SHORT_HEADER_SIZE) / sizeof(NumericDigit);
	neg = (NUMERIC_SIGN(num) == NUMERIC_NEG);

	// integer part
	if (weight >= 0 && ndigits > 0)
	{
		val = digits[0];
		for (i = 1; i <= weight; i++)
		{
			val *= NBASE;
			if (i < ndigits)
			{
				val += digits[i];
			}
		}
		val *= POWER_TABLE.at(scale);
	}
	int frac_offset = weight >= 0 ? 0 : (abs(weight) - 1) * 4;

	/*
	 * Fractional part: accumulate full NBASE-aligned digits first, then a
	 * sub-NBASE tail when scale is not a multiple of DEC_DIGITS.
	 *
	 * The previous one-pass form "multiply by NBASE, then divide back when we
	 * overshoot scale" silently corrupted numeric(p,s) values whose scale is
	 * in {37, 38} (issue #325): after nine full iterations frac_val is
	 * already ~10^36, multiplying by NBASE=10000 before dividing back blows
	 * the intermediate up to ~10^40, past signed __int128 max (~1.7e38).  The
	 * subsequent divide could not unwind the wrap-around.  Splitting the tail
	 * into a partial multiply keeps every intermediate within int128 range
	 * for any scale supported by PG numeric.
	 */
	static constexpr int small_pow10[DEC_DIGITS + 1] = {1, 10, 100, 1000, NBASE};
	while (frac_offset + DEC_DIGITS <= scale)
	{
		frac_val = frac_val * NBASE;
		if (i < ndigits)
		{
			frac_val += digits[i];
		}
		frac_offset += DEC_DIGITS;
		i++;
	}
	if (frac_offset < scale)
	{
		int partial = scale - frac_offset;
		frac_val = frac_val * small_pow10[partial];
		if (i < ndigits)
		{
			frac_val += digits[i] / small_pow10[DEC_DIGITS - partial];
		}
		i++;
	}
	val += frac_val;
	val = neg ? -val : val;

	// Trans to big endian
	*n_high = PARQUET_ARROW_BYTE_SWAP64((unsigned long)val);
	*n_low = PARQUET_ARROW_BYTE_SWAP64((unsigned long)(val >> 64));
}
