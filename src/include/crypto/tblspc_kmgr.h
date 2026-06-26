/*-------------------------------------------------------------------------
 *
 * tblspc_kmgr.h
 *	  Tablespace-level transparent data encryption (TDE) key management.
 *
 * Each encrypted tablespace has its own DEK (Data Encryption Key), stored
 * as a KMS-wrapped file under $PGDATA/pg_cryptokeys/tablespaces/<oid>.wkey.
 * At runtime, unwrapped DEKs live in a fixed-size shared-memory array keyed
 * by tablespace OID.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/include/crypto/tblspc_kmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TBLSPC_KMGR_H
#define TBLSPC_KMGR_H

#include "common/cipher.h"
#include "common/kmgr_utils.h"
#include "storage/lwlock.h"
#include "storage/relfilenode.h"

/* ----------------------------------------------------------------
 * Directory / path helpers
 * ---------------------------------------------------------------- */

/* Subdirectory that holds tablespace .wkey files */
#define TBLSPC_KMGR_DIR		"pg_cryptokeys/tablespaces"

#define TblspcWkeyPath(path, spcOid) \
	snprintf((path), MAXPGPATH, "%s/%u.wkey", TBLSPC_KMGR_DIR, (spcOid))

#define TblspcWkeyTmpPath(path, spcOid) \
	snprintf((path), MAXPGPATH, "%s/%u.wkey.tmp", TBLSPC_KMGR_DIR, (spcOid))

/* ----------------------------------------------------------------
 * .wkey on-disk format
 *
 * All multi-byte integers are stored in little-endian byte order.
 *
 *   Offset  Size  Field
 *   0       4     magic  (TBLSPC_WKEY_MAGIC, LE)
 *   4       2     version
 *   6       1     enc_method  (TBLSPC_ENC_*)
 *   7       1     provider    (TBLSPC_KMS_*)
 *   8       2     kms_key_id_len
 *   10      2     wrapped_dek_len
 *   12      4     crc32c  (covers bytes 0..11 + kms_key_id + wrapped_dek;
 *                          bytes 12..15 treated as 0x00 during computation)
 *   16      N     kms_key_id  (UTF-8, not NUL-terminated)
 *   16+N    M     wrapped_dek
 * ---------------------------------------------------------------- */

#define TBLSPC_WKEY_MAGIC		UINT32_C(0x48445445)	/* "HDTE" */
#define TBLSPC_WKEY_VERSION		UINT16_C(1)
#define TBLSPC_WKEY_HDR_SIZE	16

#define TBLSPC_MAX_KMS_KEY_ID_LEN	256
/* AES-KWP wraps 32-byte DEK → 40 bytes; leave room for other providers */
#define TBLSPC_MAX_WRAPPED_DEK_LEN	128

/* Encryption method codes */
#define TBLSPC_ENC_NONE		0
#define TBLSPC_ENC_AES128	1
#define TBLSPC_ENC_AES192	2
#define TBLSPC_ENC_AES256	3
#define TBLSPC_ENC_SM4		4

/* KMS provider codes */
#define TBLSPC_KMS_BUILTIN		1
#define TBLSPC_KMS_COSMIAN		2
#define TBLSPC_KMS_KMIP			3
#define TBLSPC_KMS_LOCAL_CMD	4

/* ----------------------------------------------------------------
 * Shared memory structures
 * ---------------------------------------------------------------- */

/*
 * One slot per loaded tablespace DEK.  The array is allocated in shared
 * memory so all backends share the same plaintext keys without each
 * having to contact the KMS independently.
 *
 * Cipher contexts (PgCipherCtx) are NOT stored here because they are
 * heap-allocated by OpenSSL and cannot live in shared memory.  Each
 * backend creates its own context lazily via TblspcGetOrCreateLocalCtx().
 */
typedef struct SpcEncEntry
{
	Oid		spc_oid;					/* InvalidOid == empty slot */
	uint8	present;					/* 1 if DEK has been loaded */
	uint8	enc_method;				/* TBLSPC_ENC_* */
	uint8	_pad[2];
	int		dek_len;					/* key length in bytes */
	uint8	dek[KMGR_MAX_KEY_LEN_BYTES]; /* plaintext DEK (range is mlock'd) */
} SpcEncEntry;

typedef struct TblspcKeyShmemData
{
	LWLock		lock;					/* protects all fields below */
	int			max_entries;			/* = tde_max_tablespace_keys */
	int			nentries;				/* currently occupied slots */
	SpcEncEntry entries[FLEXIBLE_ARRAY_MEMBER];
} TblspcKeyShmemData;

extern TblspcKeyShmemData *TblspcKeyShmem;

/* ----------------------------------------------------------------
 * GUC variables (defined in tblspc_kmgr.c, registered in guc.c)
 * ---------------------------------------------------------------- */
extern char *tde_kms_provider;
extern char *tde_kms_builtin_passphrase;
extern char *tde_kms_command;
extern char *tde_kms_host;
extern int	 tde_kms_port;
extern char *tde_kms_ca_cert;
extern char *tde_kms_client_cert;
extern char *tde_kms_client_key;
extern char *tde_kms_username;
extern char *tde_kms_password;
extern int	 tde_kms_connect_timeout;
extern int	 tde_kms_operation_timeout;
extern char *tde_kms_default_key_id;
extern int	 tde_max_tablespace_keys;

/* ----------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------- */

/* Shared-memory sizing and initialisation (called by postmaster) */
extern Size TblspcKmgrShmemSize(void);
extern void TblspcKmgrShmemInit(void);

/* Startup: validate GUCs, create directory, load all existing .wkey files */
extern void TblspcKmgrStartup(void);

/*
 * Return true if spcOid has a loaded DEK in shared memory.
 * This is the fast-path check used by storage engine code.
 */
extern bool TblspcEncryptionEnabled(Oid spcOid);

/* Return the shmem entry for spcOid, or NULL if not present */
extern SpcEncEntry *TblspcGetEncEntry(Oid spcOid);

/*
 * CREATE TABLESPACE handler: generate DEK, wrap via KMS, write .wkey,
 * load DEK into shmem.  enc_method_str is e.g. "AES256".
 * kms_key_id may be NULL to use tde_kms_default_key_id.
 * Returns the TBLSPC_ENC_* code used.
 */
extern uint8 TblspcKmgrCreateKey(Oid spcOid,
								 const char *enc_method_str,
								 const char *kms_key_id);

/* DROP TABLESPACE handler: evict from shmem, remove .wkey file */
extern void TblspcKmgrDropKey(Oid spcOid);

/*
 * Rescan pg_cryptokeys/tablespaces/ and load any .wkey files not yet in
 * shmem.  Returns the count of newly loaded DEKs.  Used by tde_sync_keys().
 */
extern int	TblspcKmgrSyncKeys(void);

/*
 * Load DEK for a single tablespace from its .wkey into shmem.
 * Called at startup (for each known tablespace) and lazily on first access.
 * Returns true on success, false if KMS is unreachable (tablespace stays
 * locked).
 */
extern bool TblspcKmgrLoadKey(Oid spcOid);

/* Helper: string → TBLSPC_ENC_* (returns TBLSPC_ENC_NONE for unknown) */
extern uint8 TblspcEncMethodFromStr(const char *method_str);

/* Helper: TBLSPC_ENC_* → key length in bytes */
extern int	TblspcEncMethodKeyLen(uint8 enc_method);

/* Helper: active KMS provider code (TBLSPC_KMS_*) from tde_kms_provider GUC */
extern uint8 TblspcCurrentProvider(void);

#endif							/* TBLSPC_KMGR_H */
