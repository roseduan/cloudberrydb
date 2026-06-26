/*-------------------------------------------------------------------------
 *
 * kms_local_cmd.c
 *	  Local-command KMS provider for tablespace-level TDE.
 *
 * The local_cmd provider runs a shell command (tde_kms_command) that
 * returns a 256-bit KEK as 64 lowercase hex characters on stdout.  The
 * DEK is then wrapped / unwrapped locally using AES-256-KWP, exactly
 * like the builtin provider — only the KEK source differs.
 *
 * Command template substitutions:
 *   %k  →  kms_key_id (empty string if not specified)
 *   %%  →  literal %
 *
 * Example:
 *   tde_kms_command = '/etc/postgresql/get_kek.sh %k'
 *
 * The script must write exactly 64 lowercase hex characters followed by
 * an optional newline to stdout and exit with status 0 on success.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *	  src/backend/crypto/kms_local_cmd.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef USE_OPENSSL
#include <openssl/evp.h>
#endif

#include "common/cipher.h"
#include "common/kmgr_utils.h"
#include "common/string.h"
#include "crypto/tblspc_kmgr.h"
#include "kms_client.h"
#include "lib/stringinfo.h"
#include "storage/fd.h"

/* KEK is always AES-256 (32 bytes = 64 hex chars) */
#define LOCAL_CMD_KEK_LEN		PG_AES256_KEY_LEN
#define LOCAL_CMD_KEK_HEX_LEN	(LOCAL_CMD_KEK_LEN * 2)		/* 64 */
/* Buffer for the hex output plus newline + NUL */
#define LOCAL_CMD_BUF_SIZE		(LOCAL_CMD_KEK_HEX_LEN + 4)

/*
 * Validate that kms_key_id contains only characters safe for shell
 * interpolation: alphanumerics, hyphens, underscores, and dots.
 * Rejects anything that could be a metacharacter when embedded in a
 * /bin/sh -c string via OpenPipeStream.
 */
static void
validate_kms_key_id(const char *kms_key_id)
{
	const char *p;

	if (!kms_key_id || kms_key_id[0] == '\0')
		return;

	for (p = kms_key_id; *p; p++)
	{
		if (!(*p >= 'a' && *p <= 'z') &&
			!(*p >= 'A' && *p <= 'Z') &&
			!(*p >= '0' && *p <= '9') &&
			*p != '-' && *p != '_' && *p != '.')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unsafe character in TDE key ID at position %d",
							(int) (p - kms_key_id) + 1),
					 errhint("Key IDs must contain only alphanumeric characters, hyphens, underscores, and dots.")));
	}
}

/*
 * Build the command string by substituting %k → kms_key_id.
 * Returns a palloc'd string; caller must pfree().
 */
static char *
build_local_cmd(const char *kms_key_id)
{
	StringInfoData cmd;
	const char *sp;

	validate_kms_key_id(kms_key_id);

	initStringInfo(&cmd);

	for (sp = tde_kms_command; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 'k':
					sp++;
					appendStringInfoString(&cmd,
										   (kms_key_id && kms_key_id[0]) ? kms_key_id : "");
					break;
				case '%':
					sp++;
					appendStringInfoChar(&cmd, '%');
					break;
				default:
					appendStringInfoChar(&cmd, *sp);
					break;
			}
		}
		else
			appendStringInfoChar(&cmd, *sp);
	}

	return cmd.data;
}

/*
 * Run the tde_kms_command and read a 32-byte KEK as 64 hex chars from stdout.
 * Writes the KEK into kek_out (must be LOCAL_CMD_KEK_LEN bytes).
 * Returns true on success, false on failure.
 */
static bool
local_cmd_run(const char *kms_key_id, uint8 *kek_out)
{
	char   *cmd;
	FILE   *fh;
	char	buf[LOCAL_CMD_BUF_SIZE];
	int		len;
	int		i;

	if (!tde_kms_command || tde_kms_command[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("tde_kms_command must be set when tde_kms_provider = 'local_cmd'")));

	cmd = build_local_cmd(kms_key_id);

	fh = OpenPipeStream(cmd, "r");
	if (fh == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not execute tde_kms_command \"%s\": %m", cmd)));

	buf[0] = '\0';
	if (!fgets(buf, sizeof(buf), fh) && ferror(fh))
	{
		ClosePipeStream(fh);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from tde_kms_command \"%s\": %m", cmd)));
	}

	if (ClosePipeStream(fh) != 0)
	{
		pfree(cmd);
		ereport(WARNING,
				(errmsg("tde_kms_command exited with non-zero status")));
		return false;
	}

	/* Strip trailing whitespace */
	len = pg_strip_crlf(buf);

	if (len != LOCAL_CMD_KEK_HEX_LEN)
	{
		pfree(cmd);
		ereport(WARNING,
				(errmsg("tde_kms_command returned %d characters; expected %d hex digits",
						len, LOCAL_CMD_KEK_HEX_LEN)));
		return false;
	}

	/* Decode hex → binary */
	for (i = 0; i < LOCAL_CMD_KEK_LEN; i++)
	{
		unsigned int byte;

		if (sscanf(buf + i * 2, "%02x", &byte) != 1)
		{
			pfree(cmd);
			ereport(WARNING,
					(errmsg("tde_kms_command returned non-hex character at position %d",
							i * 2)));
			return false;
		}
		kek_out[i] = (uint8) byte;
	}

	pfree(cmd);
	return true;
}

/*
 * local_cmd_wrap_dek
 *
 * Obtains KEK from tde_kms_command, then wraps in_dek using AES-256-KWP.
 */
static bool
local_cmd_wrap_dek(const uint8 *in_dek,
				   int in_dek_len,
				   const char *kms_key_id,
				   uint8 *out_wrapped,
				   int *out_wrapped_len)
{
#ifdef USE_OPENSSL
	uint8			kek[LOCAL_CMD_KEK_LEN];
	PgCipherCtx	   *ctx;
	CryptoKey		dek_key;
	bool			ok;

	memset(kek, 0, sizeof(kek));

	if (!local_cmd_run(kms_key_id, kek))
		return false;

	ctx = pg_cipher_ctx_create(PG_CIPHER_AES_KWP, kek, LOCAL_CMD_KEK_LEN, true);
	explicit_bzero(kek, sizeof(kek));
	if (!ctx)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not create AES-KWP context for local_cmd DEK wrap")));

	if (in_dek_len > (int) sizeof(dek_key.key))
	{
		pg_cipher_ctx_free(ctx);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("DEK length %d exceeds maximum", in_dek_len)));
	}
	dek_key.klen = in_dek_len;
	memcpy(dek_key.key, in_dek, in_dek_len);

	ok = pg_cipher_keywrap(ctx,
						   dek_key.key, dek_key.klen,
						   out_wrapped, out_wrapped_len);

	pg_cipher_ctx_free(ctx);
	explicit_bzero(&dek_key, sizeof(dek_key));

	if (!ok)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("AES-KWP DEK wrapping failed in local_cmd provider")));

	return ok;
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("tde_kms_provider = 'local_cmd' requires OpenSSL support")));
	return false;
#endif
}

/*
 * local_cmd_unwrap_dek
 *
 * Obtains KEK from tde_kms_command, then unwraps in_wrapped using AES-256-KWP.
 */
static bool
local_cmd_unwrap_dek(const uint8 *in_wrapped,
					 int in_len,
					 const char *kms_key_id,
					 uint8 *out_dek,
					 int *out_dek_len)
{
#ifdef USE_OPENSSL
	uint8			kek[LOCAL_CMD_KEK_LEN];
	PgCipherCtx	   *ctx;
	CryptoKey		dek_key;
	bool			ok;

	memset(kek, 0, sizeof(kek));

	if (!local_cmd_run(kms_key_id, kek))
		return false;

	ctx = pg_cipher_ctx_create(PG_CIPHER_AES_KWP, kek, LOCAL_CMD_KEK_LEN, false);
	explicit_bzero(kek, sizeof(kek));
	if (!ctx)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not create AES-KWP context for local_cmd DEK unwrap")));

	ok = pg_cipher_keyunwrap(ctx, in_wrapped, in_len,
							 dek_key.key, &dek_key.klen);

	pg_cipher_ctx_free(ctx);

	if (!ok)
	{
		explicit_bzero(&dek_key, sizeof(dek_key));
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PASSWORD),
				 errmsg("AES-KWP DEK unwrapping failed in local_cmd provider "
						"-- wrong KEK returned by tde_kms_command?")));
		return false;
	}

	if (dek_key.klen > KMGR_MAX_KEY_LEN_BYTES)
	{
		explicit_bzero(&dek_key, sizeof(dek_key));
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unwrapped DEK length %d exceeds maximum", dek_key.klen)));
	}

	*out_dek_len = dek_key.klen;
	memcpy(out_dek, dek_key.key, dek_key.klen);
	explicit_bzero(&dek_key, sizeof(dek_key));

	return true;
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("tde_kms_provider = 'local_cmd' requires OpenSSL support")));
	return false;
#endif
}

/* KmsOps vtable for the local_cmd provider */
static const KmsOps local_cmd_ops = {
	.wrap_dek	= local_cmd_wrap_dek,
	.unwrap_dek = local_cmd_unwrap_dek,
};

const KmsOps *
KmsLocalCmdOps(void)
{
	return &local_cmd_ops;
}
