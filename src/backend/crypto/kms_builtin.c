/*-------------------------------------------------------------------------
 *
 * kms_builtin.c
 *	  Built-in KMS provider for tablespace-level TDE.
 *
 * The built-in provider derives a KEK from a passphrase using
 * PBKDF2-SHA256 (OpenSSL PKCS5_PBKDF2_HMAC) and then wraps / unwraps
 * the DEK using AES-256-KWP (RFC 5649) via the existing
 * pg_cipher_keywrap() / pg_cipher_keyunwrap() functions.
 *
 * This provider is STRICTLY for development, CI, and regression tests.
 * The passphrase lives in postgresql.conf in plaintext.
 * NEVER use in production environments.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/crypto/kms_builtin.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef USE_OPENSSL
#include <openssl/evp.h>
#endif

#include "access/xlog.h"
#include "common/cipher.h"
#include "common/kmgr_utils.h"
#include "crypto/tblspc_kmgr.h"
#include "kms_client.h"

/* PBKDF2 parameters */
#define BUILTIN_PBKDF2_ITER			310000	/* OWASP 2023 recommendation for SHA-256 */
#define BUILTIN_PBKDF2_SALT_PREFIX	"HashData-TDE-builtin-v1"
#define BUILTIN_KEK_LEN				PG_AES256_KEY_LEN

/*
 * Derive the KEK from the passphrase GUC using PBKDF2-SHA256.
 * Writes BUILTIN_KEK_LEN bytes into kek_out.
 * Returns true on success.
 */
static bool
builtin_derive_kek(uint8 *kek_out)
{
#ifdef USE_OPENSSL
	const char *passphrase = tde_kms_builtin_passphrase;
	int			passlen;
	uint64		sysid;
	uint8		salt[sizeof(BUILTIN_PBKDF2_SALT_PREFIX) - 1 + sizeof(uint64)];
	int			saltlen;

	if (!passphrase || passphrase[0] == '\0')
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("tde_kms_builtin_passphrase must not be empty")));
		return false;
	}

	passlen = strlen(passphrase);

	/*
	 * Build a per-cluster salt: fixed prefix || cluster system identifier.
	 * Folding in the unique system identifier (stable for the cluster's
	 * lifetime, set at initdb) means two clusters that share the same
	 * passphrase no longer derive an identical KEK.  KEK derivation is local to
	 * each node, and wrap/unwrap of a node's own .wkey run on that node, so the
	 * salt is reproducible across restarts.
	 */
	sysid = GetSystemIdentifier();
	saltlen = sizeof(BUILTIN_PBKDF2_SALT_PREFIX) - 1;
	memcpy(salt, BUILTIN_PBKDF2_SALT_PREFIX, saltlen);
	memcpy(salt + saltlen, &sysid, sizeof(sysid));
	saltlen += sizeof(sysid);

	if (PKCS5_PBKDF2_HMAC(passphrase, passlen,
						  (const unsigned char *) salt,
						  saltlen,
						  BUILTIN_PBKDF2_ITER,
						  EVP_sha256(),
						  BUILTIN_KEK_LEN,
						  kek_out) != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("PBKDF2-SHA256 key derivation failed")));
		return false;
	}
	return true;
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("tde_kms_provider = 'builtin' requires OpenSSL support")));
	return false;
#endif
}

/*
 * builtin_wrap_dek
 *
 * Derives KEK from passphrase, then wraps in_dek using AES-256-KWP.
 * kms_key_id is ignored (the builtin provider uses a single derived KEK).
 */
static bool
builtin_wrap_dek(const uint8 *in_dek,
				 int in_dek_len,
				 const char *kms_key_id,
				 uint8 *out_wrapped,
				 int *out_wrapped_len)
{
#ifdef USE_OPENSSL
	uint8		   kek[BUILTIN_KEK_LEN];
	PgCipherCtx	   *ctx;
	CryptoKey		dek_key;
	bool			ok;

	memset(kek, 0, sizeof(kek));

	if (!builtin_derive_kek(kek))
		return false;

	ctx = pg_cipher_ctx_create(PG_CIPHER_AES_KWP, kek, BUILTIN_KEK_LEN, true);
	if (!ctx)
	{
		explicit_bzero(kek, sizeof(kek));
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not create AES-KWP cipher context for DEK wrapping")));
		return false;
	}

	/* pg_cipher_keywrap expects a CryptoKey */
	if (in_dek_len > (int) sizeof(dek_key.key))
	{
		pg_cipher_ctx_free(ctx);
		explicit_bzero(kek, sizeof(kek));
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("DEK length %d exceeds maximum supported key length", in_dek_len)));
		return false;
	}
	dek_key.klen = in_dek_len;
	memcpy(dek_key.key, in_dek, in_dek_len);

	ok = pg_cipher_keywrap(ctx,
						   dek_key.key, dek_key.klen,
						   out_wrapped, out_wrapped_len);

	pg_cipher_ctx_free(ctx);
	explicit_bzero(kek, sizeof(kek));
	explicit_bzero(&dek_key, sizeof(dek_key));

	if (!ok)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("AES-KWP DEK wrapping failed")));

	return ok;
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("tde_kms_provider = 'builtin' requires OpenSSL support")));
	return false;
#endif
}

/*
 * builtin_unwrap_dek
 *
 * Derives KEK from passphrase, then unwraps in_wrapped using AES-256-KWP.
 */
static bool
builtin_unwrap_dek(const uint8 *in_wrapped,
				   int in_len,
				   const char *kms_key_id,
				   uint8 *out_dek,
				   int *out_dek_len)
{
#ifdef USE_OPENSSL
	uint8		   kek[BUILTIN_KEK_LEN];
	PgCipherCtx	   *ctx;
	CryptoKey		dek_key;
	bool			ok;

	memset(kek, 0, sizeof(kek));

	if (!builtin_derive_kek(kek))
		return false;

	ctx = pg_cipher_ctx_create(PG_CIPHER_AES_KWP, kek, BUILTIN_KEK_LEN, false);
	if (!ctx)
	{
		explicit_bzero(kek, sizeof(kek));
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not create AES-KWP cipher context for DEK unwrapping")));
		return false;
	}

	ok = pg_cipher_keyunwrap(ctx, in_wrapped, in_len,
							 dek_key.key, &dek_key.klen);

	pg_cipher_ctx_free(ctx);
	explicit_bzero(kek, sizeof(kek));

	if (!ok)
	{
		explicit_bzero(&dek_key, sizeof(dek_key));
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PASSWORD),
				 errmsg("AES-KWP DEK unwrapping failed -- wrong passphrase?")));
		return false;
	}

	if (dek_key.klen > KMGR_MAX_KEY_LEN_BYTES)
	{
		explicit_bzero(&dek_key, sizeof(dek_key));
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unwrapped DEK length %d exceeds maximum", dek_key.klen)));
		return false;
	}

	*out_dek_len = dek_key.klen;
	memcpy(out_dek, dek_key.key, dek_key.klen);
	explicit_bzero(&dek_key, sizeof(dek_key));

	return true;
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("tde_kms_provider = 'builtin' requires OpenSSL support")));
	return false;
#endif
}

/* KmsOps vtable for the built-in provider */
static const KmsOps builtin_ops = {
	.wrap_dek	= builtin_wrap_dek,
	.unwrap_dek = builtin_unwrap_dek,
};

const KmsOps *
KmsBuiltinOps(void)
{
	return &builtin_ops;
}
