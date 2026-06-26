/*-------------------------------------------------------------------------
 *
 * kms_client.h
 *	  Internal KMS provider abstraction for tablespace-level TDE.
 *
 * This header is private to src/backend/crypto/ and must NOT be included
 * by other subsystems.  External callers use the API in tblspc_kmgr.h.
 *
 * A KMS provider is responsible for:
 *   wrap_dek   – take a plaintext DEK and return a wrapped (encrypted) blob
 *   unwrap_dek – take a wrapped blob and return the plaintext DEK
 *
 * All providers use the same blob representation; the wrapping algorithm
 * is internal to each provider:
 *   builtin   – PBKDF2-SHA256(passphrase) → KEK, then AES-KWP (RFC 5649)
 *   cosmian   – KMIP 2.0 server-side Encrypt/Decrypt via HTTPS (KEK stays in KMS)
 *   kmip      – native KMIP 2.1 TTLV over mTLS TCP, server-side Encrypt/Decrypt
 *   local_cmd – shell command returns KEK, then local AES-KWP
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *	  src/backend/crypto/kms_client.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KMS_CLIENT_H
#define KMS_CLIENT_H

#include "postgres.h"
#include "crypto/tblspc_kmgr.h"

/*
 * KmsOps – vtable for a KMS provider.
 *
 * Both functions:
 *   - return true on success, false on (non-fatal) failure
 *   - use ereport(ERROR, ...) for fatal / programming errors
 *   - are called inside a PG_TRY block by the key manager
 *
 * wrap_dek:
 *   in_dek / in_dek_len   – plaintext DEK
 *   kms_key_id            – KMS key ID (provider-specific; may be empty)
 *   out_wrapped           – caller-supplied buffer of TBLSPC_MAX_WRAPPED_DEK_LEN
 *   out_wrapped_len       – actual bytes written to out_wrapped
 *
 * unwrap_dek:
 *   in_wrapped / in_len   – the wrapped blob from wrap_dek
 *   kms_key_id            – same KMS key ID used during wrap
 *   out_dek               – caller-supplied buffer of KMGR_MAX_KEY_LEN_BYTES
 *   out_dek_len           – actual bytes written to out_dek
 */
typedef struct KmsOps
{
	bool	(*wrap_dek)(const uint8 *in_dek,
						int in_dek_len,
						const char *kms_key_id,
						uint8 *out_wrapped,
						int *out_wrapped_len);

	bool	(*unwrap_dek)(const uint8 *in_wrapped,
						  int in_len,
						  const char *kms_key_id,
						  uint8 *out_dek,
						  int *out_dek_len);
} KmsOps;

/*
 * Return the KmsOps vtable for the given TBLSPC_KMS_* provider code.
 * Returns NULL for TBLSPC_KMS_BUILTIN if OpenSSL is not available.
 * Calls ereport(ERROR) for an unrecognised code.
 */
extern const KmsOps *KmsGetOps(uint8 provider_code);

/* Provider initialisers (called by KmsGetOps) */
extern const KmsOps *KmsBuiltinOps(void);
extern const KmsOps *KmsLocalCmdOps(void);
extern const KmsOps *KmsCosmianOps(void);
extern const KmsOps *KmsKmipOps(void);

#endif							/* KMS_CLIENT_H */
