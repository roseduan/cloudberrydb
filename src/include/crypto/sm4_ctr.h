/*-------------------------------------------------------------------------
 *
 * sm4_ctr.h
 *    SM4 block cipher in CTR (Counter) mode.
 *
 * CTR mode generates an independent keystream block for each 16-byte aligned
 * counter value: keystream[i] = SM4_encrypt(key, counter + i).  This makes
 * the cipher position-addressable: any byte offset can be decrypted without
 * processing prior bytes, which is required for PAX micro-partition files.
 *
 * The counter is treated as a 128-bit big-endian integer, matching the IV
 * layout produced by build_pax_iv() / PaxBuildIvFixed().
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    src/include/crypto/sm4_ctr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SM4_CTR_H
#define SM4_CTR_H

#include "crypto/sm4_ofb.h"     /* SM4_KEY, SM4_BLOCK_SIZE, ossl_sm4_encrypt */

/*
 * sm4_ctr_setkey — expand `key` (16 bytes for SM4-128) into key schedule `ks`.
 */
void sm4_ctr_setkey(SM4_KEY *ks, const uint8_t *key);

/*
 * sm4_ctr_cipher — encrypt or decrypt `len` bytes in-place (out == in allowed).
 *
 * `iv` holds the initial 128-bit counter value for the first 16-byte block.
 * The counter is incremented as a 128-bit big-endian integer for each block.
 * Encrypt and decrypt are identical (XOR with keystream).
 */
void sm4_ctr_cipher(const SM4_KEY *ks,
                    unsigned char *out,
                    const unsigned char *in,
                    size_t len,
                    const uint8_t iv[SM4_BLOCK_SIZE]);

#endif   /* SM4_CTR_H */
