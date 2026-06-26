/*-------------------------------------------------------------------------
 *
 * sm4_ctr.c
 *    SM4 block cipher in CTR (Counter) mode.
 *
 * CTR mode is position-addressable: to process bytes starting at file offset
 * N, compute block_counter = N / 16, build the IV with that counter, then
 * call sm4_ctr_cipher().  Each 16-byte block is independent of all others.
 *
 * This is the cipher used for PAX micro-partition files when the tablespace
 * encryption method is SM4, because PAX performs random-offset reads and
 * therefore requires position-addressable decryption.  SM4-OFB cannot do
 * this (its keystream is chained), but SM4-CTR can.
 *
 * Two implementations are compiled depending on the OpenSSL version:
 *
 *   EVP path (USE_OPENSSL && !OPENSSL_NO_SM4):
 *     Delegates to EVP_sm4_ctr() (available in OpenSSL >= 1.1.1 when SM4 is
 *     enabled).  The SM4_KEY.rk[] array holds the raw 16-byte key so that the
 *     EVP context can be constructed inside sm4_ctr_cipher() without keeping
 *     a long-lived context.
 *
 *   Software path (all other cases):
 *     Pure C implementation using ossl_sm4_encrypt() from sm4_ofb.c.
 *     SM4_KEY.rk[] holds the expanded 32-word key schedule.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    src/backend/crypto/sm4_ctr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <string.h>

#include "crypto/sm4_ctr.h"

#if defined(USE_OPENSSL) && !defined(OPENSSL_NO_SM4)
#include <openssl/err.h>
#include <openssl/evp.h>
#endif

/*
 * sm4_ctr_setkey — initialise the SM4_KEY for later use by sm4_ctr_cipher.
 *
 * EVP path:      copies the raw 16-byte key into ks->rk[0..3] (treated as a
 *                16-byte byte array).  No key expansion is performed here.
 * Software path: expands the key schedule via ossl_sm4_set_key().
 */
void
sm4_ctr_setkey(SM4_KEY *ks, const uint8_t *key)
{
#if defined(USE_OPENSSL) && !defined(OPENSSL_NO_SM4)
	/* Store raw key bytes for EVP_EncryptInit_ex in sm4_ctr_cipher */
	memcpy(ks->rk, key, SM4_BLOCK_SIZE);
#else
	ossl_sm4_set_key(key, ks);
#endif
}

/*
 * sm4_ctr_cipher — SM4-CTR encrypt/decrypt (in-place capable).
 *
 * EVP path:
 *   Constructs a per-call EVP_CIPHER_CTX using the raw key stored in ks->rk
 *   and the supplied iv, then runs EVP_EncryptUpdate over the full buffer.
 *   EVP SM4-CTR handles partial final blocks internally.
 *
 * Software path:
 *   Manual CTR loop: counter starts at iv and is incremented as a 128-bit
 *   big-endian integer.  This matches the counter layout produced by
 *   build_pax_iv() and PaxBuildIvFixed() in pax_storage.
 *
 * out == in is allowed (in-place operation) in both paths.
 */
void
sm4_ctr_cipher(const SM4_KEY *ks,
			   unsigned char *out,
			   const unsigned char *in,
			   size_t len,
			   const uint8_t iv[SM4_BLOCK_SIZE])
{
#if defined(USE_OPENSSL) && !defined(OPENSSL_NO_SM4)
	EVP_CIPHER_CTX *ctx;
	int				outl = 0;

	ctx = EVP_CIPHER_CTX_new();
	if (!ctx)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("sm4_ctr_cipher: EVP_CIPHER_CTX_new failed")));

	if (EVP_EncryptInit_ex(ctx, EVP_sm4_ctr(), NULL,
						   (const unsigned char *) ks->rk, iv) != 1)
	{
		EVP_CIPHER_CTX_free(ctx);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("sm4_ctr_cipher: EVP_EncryptInit_ex failed: %s",
						ERR_error_string(ERR_get_error(), NULL))));
	}

	if (EVP_EncryptUpdate(ctx, out, &outl, in, (int) len) != 1)
	{
		EVP_CIPHER_CTX_free(ctx);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("sm4_ctr_cipher: EVP_EncryptUpdate failed: %s",
						ERR_error_string(ERR_get_error(), NULL))));
	}

	EVP_CIPHER_CTX_free(ctx);
#else
	/* Pure-software SM4-CTR: no OpenSSL SM4 or OpenSSL without SM4 support */
	uint8_t		ctr[SM4_BLOCK_SIZE];
	uint8_t		keystream[SM4_BLOCK_SIZE];
	int			j;

	memcpy(ctr, iv, SM4_BLOCK_SIZE);

	/* Full blocks */
	while (len >= SM4_BLOCK_SIZE)
	{
		ossl_sm4_encrypt(ctr, keystream, ks);

		for (j = 0; j < SM4_BLOCK_SIZE; j++)
			out[j] = in[j] ^ keystream[j];

		/* Increment 128-bit big-endian counter */
		for (j = SM4_BLOCK_SIZE - 1; j >= 0; j--)
			if (++ctr[j] != 0)
				break;

		out += SM4_BLOCK_SIZE;
		in  += SM4_BLOCK_SIZE;
		len -= SM4_BLOCK_SIZE;
	}

	/* Partial final block */
	if (len > 0)
	{
		ossl_sm4_encrypt(ctr, keystream, ks);
		for (j = 0; j < (int) len; j++)
			out[j] = in[j] ^ keystream[j];
	}
#endif
}
