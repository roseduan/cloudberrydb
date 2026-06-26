/*-------------------------------------------------------------------------
 *
 * tblspc_kmgr.c
 *	  Tablespace-level TDE key manager.
 *
 * Responsibilities:
 *   - GUC variable registration (via guc.c; GUC *definitions* are here)
 *   - Shared memory allocation (TblspcKeyShmemSize / TblspcKeyShmemInit)
 *   - .wkey file read / write with CRC32C integrity check
 *   - DEK generation, wrapping, and loading via pluggable KMS provider
 *   - Fast-path TblspcEncryptionEnabled() check used by storage engines
 *
 * Called from:
 *   postmaster startup  → TblspcKmgrShmemInit() then TblspcKmgrStartup()
 *   tablespace.c (DDL)  → TblspcKmgrCreateKey() / TblspcKmgrDropKey()
 *   storage engines     → TblspcEncryptionEnabled() / TblspcGetEncEntry()
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/crypto/tblspc_kmgr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "miscadmin.h"
#include "pgstat.h"
#include "port.h"

#include "common/cipher.h"
#include "common/kmgr_utils.h"
#include "crypto/tblspc_kmgr.h"
#include "kms_client.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"

/* ----------------------------------------------------------------
 * GUC variable definitions
 * ---------------------------------------------------------------- */
char *tde_kms_provider			= NULL;
char *tde_kms_builtin_passphrase = NULL;
char *tde_kms_command			= NULL;
char *tde_kms_host				= NULL;
int	  tde_kms_port				= 5696;
char *tde_kms_ca_cert			= NULL;
char *tde_kms_client_cert		= NULL;
char *tde_kms_client_key		= NULL;
char *tde_kms_username			= NULL;
char *tde_kms_password			= NULL;
int	  tde_kms_connect_timeout	= 10;
int	  tde_kms_operation_timeout = 30;
char *tde_kms_default_key_id	= NULL;
int	  tde_max_tablespace_keys	= 128;

/* Shared memory pointer */
TblspcKeyShmemData *TblspcKeyShmem = NULL;

static int	TblspcLockTranche = -1;

/* ----------------------------------------------------------------
 * Internal helpers
 * ---------------------------------------------------------------- */

/*
 * Little-endian store/load helpers for on-disk .wkey format.
 * These are explicit to guarantee platform-independent byte order.
 */
static inline void
store_le16(uint8 *buf, uint16 val)
{
	buf[0] = (uint8) (val);
	buf[1] = (uint8) (val >> 8);
}

static inline void
store_le32(uint8 *buf, uint32 val)
{
	buf[0] = (uint8) (val);
	buf[1] = (uint8) (val >> 8);
	buf[2] = (uint8) (val >> 16);
	buf[3] = (uint8) (val >> 24);
}

static inline uint16
load_le16(const uint8 *buf)
{
	return (uint16) buf[0] | ((uint16) buf[1] << 8);
}

static inline uint32
load_le32(const uint8 *buf)
{
	return (uint32) buf[0]
		 | ((uint32) buf[1] << 8)
		 | ((uint32) buf[2] << 16)
		 | ((uint32) buf[3] << 24);
}

/*
 * Compute CRC32C over the .wkey content.
 * The header bytes 12..15 (the crc32c field itself) are treated as zero.
 *
 * hdr:        raw 16-byte header buffer
 * kms_key_id: pointer to kms_key_id bytes (may be NULL if len == 0)
 * key_id_len: length of kms_key_id
 * wrapped:    wrapped DEK bytes
 * wrapped_len: length of wrapped DEK
 */
static uint32
wkey_compute_crc(const uint8 *hdr,
				 const uint8 *kms_key_id, uint16 key_id_len,
				 const uint8 *wrapped, uint16 wrapped_len)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	/* Header bytes 0..11 (before the crc field) */
	COMP_CRC32C(crc, hdr, 12);
	/* Four zero bytes representing the crc field at offset 12..15 */
	{
		uint8 zeros[4] = {0, 0, 0, 0};
		COMP_CRC32C(crc, zeros, 4);
	}
	/* Variable-length data */
	if (key_id_len > 0 && kms_key_id)
		COMP_CRC32C(crc, kms_key_id, key_id_len);
	if (wrapped_len > 0 && wrapped)
		COMP_CRC32C(crc, wrapped, wrapped_len);
	FIN_CRC32C(crc);
	return (uint32) crc;
}

/*
 * wkey_write_raw
 *
 * Write len bytes from buf to fd, retrying on short writes.
 * Calls ereport(ERROR) on failure.
 */
static void
wkey_write_raw(int fd, const void *buf, int len, const char *path)
{
	int		written = 0;

	while (written < len)
	{
		int		n = write(fd, (const char *) buf + written, len - written);

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", path)));
		}
		written += n;
	}
}

/*
 * Write a .wkey file atomically (tmp path → rename).
 *
 * spcOid:       tablespace OID
 * enc_method:   TBLSPC_ENC_* code
 * provider:     TBLSPC_KMS_* code
 * kms_key_id:   KMS key identifier string (may be empty string, not NULL)
 * wrapped_dek:  wrapped DEK bytes
 * wrapped_len:  length of wrapped DEK
 */
static void
wkey_write(Oid spcOid,
		   uint8 enc_method,
		   uint8 provider,
		   const char *kms_key_id,
		   const uint8 *wrapped_dek,
		   int wrapped_len)
{
	char		path[MAXPGPATH];
	char		tmp_path[MAXPGPATH];
	int			fd;
	uint8		hdr[TBLSPC_WKEY_HDR_SIZE];
	uint16		key_id_len;
	uint16		dek_len16;
	uint32		crc;

	Assert(kms_key_id != NULL);
	key_id_len = (uint16) strlen(kms_key_id);
	Assert(key_id_len <= TBLSPC_MAX_KMS_KEY_ID_LEN);
	Assert(wrapped_len > 0 && wrapped_len <= TBLSPC_MAX_WRAPPED_DEK_LEN);

	dek_len16 = (uint16) wrapped_len;

	/* Build header */
	store_le32(hdr + 0,  TBLSPC_WKEY_MAGIC);
	store_le16(hdr + 4,  TBLSPC_WKEY_VERSION);
	hdr[6] = enc_method;
	hdr[7] = provider;
	store_le16(hdr + 8,  key_id_len);
	store_le16(hdr + 10, dek_len16);
	/* bytes 12..15 hold CRC; set to 0 during computation */
	store_le32(hdr + 12, 0);

	crc = wkey_compute_crc(hdr,
						   (const uint8 *) kms_key_id, key_id_len,
						   wrapped_dek, dek_len16);
	store_le32(hdr + 12, crc);

	/* Write to tmp path, then atomic rename */
	TblspcWkeyTmpPath(tmp_path, spcOid);
	TblspcWkeyPath(path, spcOid);

	fd = BasicOpenFilePerm(tmp_path,
						   O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY,
						   0600);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create tablespace key file \"%s\": %m",
						tmp_path)));

	pgstat_report_wait_start(WAIT_EVENT_KEY_FILE_WRITE);
	wkey_write_raw(fd, hdr, TBLSPC_WKEY_HDR_SIZE, tmp_path);
	if (key_id_len > 0)
		wkey_write_raw(fd, kms_key_id, (int) key_id_len, tmp_path);
	wkey_write_raw(fd, wrapped_dek, wrapped_len, tmp_path);
	pgstat_report_wait_end();

	pgstat_report_wait_start(WAIT_EVENT_KEY_FILE_SYNC);
	if (pg_fsync(fd) != 0)
	{
		close(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync \"%s\": %m", tmp_path)));
	}
	pgstat_report_wait_end();

	if (close(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close \"%s\": %m", tmp_path)));

	/* Atomic rename */
	if (rename(tmp_path, path) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename \"%s\" to \"%s\": %m",
						tmp_path, path)));

	ereport(LOG,
			(errmsg("TDE: wrote .wkey for tablespace oid=%u (method=%u, provider=%u)",
					spcOid, enc_method, provider)));
}

/*
 * Read and validate a .wkey file.
 *
 * On success: fills enc_method_out, kms_key_id_out (palloc'd), wrapped_out
 * (palloc'd), wrapped_len_out and returns true.
 *
 * On CRC failure or bad magic: emits WARNING and returns false.
 * On I/O error: ereport(ERROR).
 */
static bool
wkey_read(Oid spcOid,
		  uint8 *enc_method_out,
		  uint8 *provider_out,
		  char **kms_key_id_out,
		  uint8 **wrapped_out,
		  int *wrapped_len_out)
{
	char		path[MAXPGPATH];
	File		fd;
	uint8		hdr[TBLSPC_WKEY_HDR_SIZE];
	uint32		magic;
	uint16		version;
	uint16		key_id_len;
	uint16		wrapped_len;
	uint32		stored_crc;
	uint32		computed_crc;
	char	   *key_id_buf;
	uint8	   *wrapped_buf;

	TblspcWkeyPath(path, spcOid);

	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno == ENOENT)
			return false;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open tablespace key file \"%s\": %m", path)));
	}

	/* Read header */
	pgstat_report_wait_start(WAIT_EVENT_KEY_FILE_READ);
	if (read(fd, hdr, TBLSPC_WKEY_HDR_SIZE) != TBLSPC_WKEY_HDR_SIZE)
	{
		pgstat_report_wait_end();
		close(fd);
		ereport(WARNING,
				(errmsg("tablespace key file \"%s\" is truncated", path)));
		return false;
	}
	pgstat_report_wait_end();

	magic   = load_le32(hdr + 0);
	version = load_le16(hdr + 4);

	if (magic != TBLSPC_WKEY_MAGIC)
	{
		close(fd);
		ereport(WARNING,
				(errmsg("tablespace key file \"%s\" has wrong magic 0x%08X",
						path, magic)));
		return false;
	}

	if (version != TBLSPC_WKEY_VERSION)
	{
		close(fd);
		ereport(WARNING,
				(errmsg("tablespace key file \"%s\" has unsupported version %u",
						path, version)));
		return false;
	}

	*enc_method_out = hdr[6];
	*provider_out   = hdr[7];
	key_id_len		= load_le16(hdr + 8);
	wrapped_len		= load_le16(hdr + 10);
	stored_crc		= load_le32(hdr + 12);

	if (key_id_len > TBLSPC_MAX_KMS_KEY_ID_LEN ||
		wrapped_len == 0 ||
		wrapped_len > TBLSPC_MAX_WRAPPED_DEK_LEN)
	{
		close(fd);
		ereport(WARNING,
				(errmsg("tablespace key file \"%s\" has invalid field lengths "
						"(key_id_len=%u, wrapped_len=%u)",
						path, key_id_len, wrapped_len)));
		return false;
	}

	key_id_buf = (char *) palloc(key_id_len + 1);
	wrapped_buf = (uint8 *) palloc(wrapped_len);

	pgstat_report_wait_start(WAIT_EVENT_KEY_FILE_READ);
	if (key_id_len > 0 &&
		read(fd, key_id_buf, key_id_len) != (ssize_t) key_id_len)
	{
		pgstat_report_wait_end();
		close(fd);
		pfree(key_id_buf);
		pfree(wrapped_buf);
		ereport(WARNING,
				(errmsg("tablespace key file \"%s\" is truncated in kms_key_id",
						path)));
		return false;
	}
	key_id_buf[key_id_len] = '\0';

	if (read(fd, (char *) wrapped_buf, wrapped_len) != (ssize_t) wrapped_len)
	{
		pgstat_report_wait_end();
		close(fd);
		pfree(key_id_buf);
		pfree(wrapped_buf);
		ereport(WARNING,
				(errmsg("tablespace key file \"%s\" is truncated in wrapped DEK",
						path)));
		return false;
	}
	pgstat_report_wait_end();
	close(fd);

	/* Verify CRC */
	computed_crc = wkey_compute_crc(hdr,
									(const uint8 *) key_id_buf, key_id_len,
									wrapped_buf, wrapped_len);
	if (computed_crc != stored_crc)
	{
		pfree(key_id_buf);
		pfree(wrapped_buf);
		ereport(WARNING,
				(errmsg("CRC mismatch in tablespace key file \"%s\" "
						"(stored 0x%08X, computed 0x%08X) -- file may be corrupted",
						path, stored_crc, computed_crc),
				 errhint("Restore the file from backup or run tde_sync_keys().")));
		return false;
	}

	*kms_key_id_out  = key_id_buf;
	*wrapped_out	 = wrapped_buf;
	*wrapped_len_out = (int) wrapped_len;
	return true;
}

/* ----------------------------------------------------------------
 * Shared memory management
 * ---------------------------------------------------------------- */

Size
TblspcKmgrShmemSize(void)
{
	return add_size(MAXALIGN(offsetof(TblspcKeyShmemData, entries)),
					mul_size(tde_max_tablespace_keys, sizeof(SpcEncEntry)));
}

void
TblspcKmgrShmemInit(void)
{
	bool		found;
	Size		sz = TblspcKmgrShmemSize();

	TblspcKeyShmem = (TblspcKeyShmemData *)
		ShmemInitStruct("TblspcKeyShmem", sz, &found);

	if (!found)
	{
		int		i;

		TblspcLockTranche = LWLockNewTrancheId();
		LWLockRegisterTranche(TblspcLockTranche, "tblspc_tde");
		LWLockInitialize(&TblspcKeyShmem->lock, TblspcLockTranche);

		TblspcKeyShmem->max_entries = tde_max_tablespace_keys;
		TblspcKeyShmem->nentries = 0;
		for (i = 0; i < tde_max_tablespace_keys; i++)
		{
			TblspcKeyShmem->entries[i].spc_oid = InvalidOid;
			TblspcKeyShmem->entries[i].present = 0;
		}
	}
}

/* ----------------------------------------------------------------
 * Shmem entry lookup / insertion
 * ---------------------------------------------------------------- */

/*
 * Find the shmem slot for spcOid, or return NULL if not present.
 * Acquires LW_SHARED to guard against concurrent DROP or key rotation.
 * Callers that dereference the returned pointer must continue to hold
 * the lock (or copy the entry) before releasing it.  For the hot-path
 * encryption check (TblspcEncryptionEnabled), a short shared hold is
 * acceptable; for DEK access the caller must hold across the use.
 */
SpcEncEntry *
TblspcGetEncEntry(Oid spcOid)
{
	int		i;
	int		max;
	SpcEncEntry *result = NULL;

	if (!TblspcKeyShmem || spcOid == InvalidOid)
		return NULL;

	/*
	 * Fast path: when no DEKs are loaded (the common case on a cluster with no
	 * encrypted tablespaces), skip the LWLock entirely.  This function sits on
	 * the hot path of every buffer read/write via TblspcEncryptionEnabled(),
	 * and unconditionally taking a global LW_SHARED lock there caused
	 * measurable contention under highly concurrent workloads.
	 *
	 * Reading nentries without the lock is safe here: it only transitions from
	 * 0 to non-zero when a key is loaded, and a backend cannot legitimately be
	 * accessing buffers of a tablespace whose key is not yet visible to it, so
	 * a stale 0 read can never hide a key this backend actually needs.
	 */
	if (TblspcKeyShmem->nentries == 0)
		return NULL;

	LWLockAcquire(&TblspcKeyShmem->lock, LW_SHARED);

	max = TblspcKeyShmem->max_entries;
	for (i = 0; i < max; i++)
	{
		SpcEncEntry *e = &TblspcKeyShmem->entries[i];

		if (e->spc_oid == spcOid && e->present)
		{
			result = e;
			break;
		}
	}

	LWLockRelease(&TblspcKeyShmem->lock);
	return result;
}

bool
TblspcEncryptionEnabled(Oid spcOid)
{
	return TblspcGetEncEntry(spcOid) != NULL;
}

/*
 * Store a plaintext DEK in the first available shmem slot.
 * If the OID already has a slot, overwrites it.
 * Returns the slot, or ereport(ERROR) if the table is full.
 */
static SpcEncEntry *
shmem_store_dek(Oid spcOid, uint8 enc_method,
				const uint8 *dek, int dek_len)
{
	int			i;
	int			max;
	SpcEncEntry *free_slot = NULL;

	LWLockAcquire(&TblspcKeyShmem->lock, LW_EXCLUSIVE);

	max = TblspcKeyShmem->max_entries;

	for (i = 0; i < max; i++)
	{
		SpcEncEntry *e = &TblspcKeyShmem->entries[i];

		if (e->spc_oid == spcOid)
		{
			/* overwrite existing entry */
			free_slot = e;
			break;
		}
		if (e->spc_oid == InvalidOid && free_slot == NULL)
			free_slot = e;
	}

	if (!free_slot)
	{
		LWLockRelease(&TblspcKeyShmem->lock);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("tablespace DEK cache is full (tde_max_tablespace_keys=%d)",
						tde_max_tablespace_keys)));
	}

	free_slot->spc_oid	= spcOid;
	free_slot->enc_method = enc_method;
	free_slot->dek_len	= dek_len;
	memcpy(free_slot->dek, dek, dek_len);
	free_slot->present	= 1;

	if (free_slot->spc_oid == spcOid && TblspcKeyShmem->nentries < max)
		TblspcKeyShmem->nentries++;

	LWLockRelease(&TblspcKeyShmem->lock);
	return free_slot;
}

/* ----------------------------------------------------------------
 * Helper: enc_method conversions
 * ---------------------------------------------------------------- */

uint8
TblspcEncMethodFromStr(const char *method_str)
{
	if (!method_str)
		return TBLSPC_ENC_NONE;
	if (pg_strcasecmp(method_str, "AES128") == 0) return TBLSPC_ENC_AES128;
	if (pg_strcasecmp(method_str, "AES192") == 0) return TBLSPC_ENC_AES192;
	if (pg_strcasecmp(method_str, "AES256") == 0) return TBLSPC_ENC_AES256;
	if (pg_strcasecmp(method_str, "SM4")    == 0) return TBLSPC_ENC_SM4;
	return TBLSPC_ENC_NONE;
}

int
TblspcEncMethodKeyLen(uint8 enc_method)
{
	switch (enc_method)
	{
		case TBLSPC_ENC_AES128: return PG_AES128_KEY_LEN;
		case TBLSPC_ENC_AES192: return PG_AES192_KEY_LEN;
		case TBLSPC_ENC_AES256: return PG_AES256_KEY_LEN;
		case TBLSPC_ENC_SM4:    return 16;	/* SM4 uses 128-bit key */
		default:				return 0;
	}
}

uint8
TblspcCurrentProvider(void)
{
	if (!tde_kms_provider || strcmp(tde_kms_provider, "none") == 0)
		return 0;
	if (strcmp(tde_kms_provider, "builtin") == 0)   return TBLSPC_KMS_BUILTIN;
	if (strcmp(tde_kms_provider, "cosmian") == 0)   return TBLSPC_KMS_COSMIAN;
	if (strcmp(tde_kms_provider, "kmip") == 0)      return TBLSPC_KMS_KMIP;
	if (strcmp(tde_kms_provider, "local_cmd") == 0) return TBLSPC_KMS_LOCAL_CMD;
	return 0;
}

/* ----------------------------------------------------------------
 * KmsGetOps dispatcher
 * ---------------------------------------------------------------- */

const KmsOps *
KmsGetOps(uint8 provider_code)
{
	switch (provider_code)
	{
		case TBLSPC_KMS_BUILTIN:
			return KmsBuiltinOps();
		case TBLSPC_KMS_LOCAL_CMD:
			return KmsLocalCmdOps();
		case TBLSPC_KMS_COSMIAN:
			return KmsCosmianOps();
		case TBLSPC_KMS_KMIP:
			return KmsKmipOps();
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unknown KMS provider code %u", provider_code)));
			break;
	}
	return NULL;				/* unreachable */
}

/* ----------------------------------------------------------------
 * GUC validation
 * ---------------------------------------------------------------- */

/*
 * Validate GUC parameter combinations at startup (PGC_POSTMASTER context).
 * Called by TblspcKmgrStartup().
 */
static void
validate_guc_combination(void)
{
	uint8	provider = TblspcCurrentProvider();

	if (provider == 0)
		return;					/* tde_kms_provider = 'none', nothing to check */

	if (provider == TBLSPC_KMS_BUILTIN)
	{
		if (!tde_kms_builtin_passphrase || tde_kms_builtin_passphrase[0] == '\0')
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("tde_kms_builtin_passphrase must not be empty "
							"when tde_kms_provider = 'builtin'")));
		return;
	}

	if (provider == TBLSPC_KMS_LOCAL_CMD)
	{
		if (!tde_kms_command || tde_kms_command[0] == '\0')
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("tde_kms_command must not be empty "
							"when tde_kms_provider = 'local_cmd'")));
		return;
	}

	/* cosmian / kmip: host is always required */
	if (!tde_kms_host || tde_kms_host[0] == '\0')
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("tde_kms_host must be set when tde_kms_provider = '%s'",
						tde_kms_provider)));

	if (provider == TBLSPC_KMS_KMIP)
	{
		/* Native KMIP TCP always requires TLS */
		if (!tde_kms_ca_cert || tde_kms_ca_cert[0] == '\0')
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("tde_kms_ca_cert must be set when tde_kms_provider = 'kmip'")));
	}
	else
	{
		/*
		 * For cosmian: tde_kms_ca_cert is optional (plain HTTP allowed in
		 * dev/internal deployments).
		 */
		if (!tde_kms_ca_cert || tde_kms_ca_cert[0] == '\0')
			ereport(WARNING,
					(errmsg("tde_kms_ca_cert is not set; connecting to '%s' "
							"over plain HTTP (no TLS)",
							tde_kms_host)));
	}

	/* Warn if only one of client cert/key is specified (mTLS half-configured) */
	{
		bool has_cert = (tde_kms_client_cert && tde_kms_client_cert[0] != '\0');
		bool has_key  = (tde_kms_client_key  && tde_kms_client_key[0]  != '\0');

		if (has_cert != has_key)
			ereport(WARNING,
					(errmsg("tde_kms_client_cert and tde_kms_client_key should "
							"be set together for mTLS authentication")));
	}
}

/* ----------------------------------------------------------------
 * Public API: startup
 * ---------------------------------------------------------------- */

void
TblspcKmgrStartup(void)
{
	struct stat st;

	/* No encryption configured; nothing to do */
	if (TblspcCurrentProvider() == 0)
		return;

	validate_guc_combination();

	/* Create directory if it does not yet exist */
	if (stat(TBLSPC_KMGR_DIR, &st) != 0)
	{
		if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat directory \"%s\": %m",
							TBLSPC_KMGR_DIR)));

		if (MakePGDirectory(TBLSPC_KMGR_DIR) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create directory \"%s\": %m",
							TBLSPC_KMGR_DIR)));
	}

	/*
	 * Remove any leftover .wkey.tmp files from a previous crashed run.
	 * These are partial writes that were never renamed to .wkey.
	 */
	{
		DIR		   *dir;
		struct dirent *de;

		dir = AllocateDir(TBLSPC_KMGR_DIR);
		while ((de = ReadDir(dir, TBLSPC_KMGR_DIR)) != NULL)
		{
			char	path[MAXPGPATH];
			int		len = strlen(de->d_name);

			if (len > 8 && strcmp(de->d_name + len - 8, ".wkey.tmp") == 0)
			{
				snprintf(path, sizeof(path), "%s/%s",
						 TBLSPC_KMGR_DIR, de->d_name);
				if (unlink(path) < 0)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not remove orphan temp key file \"%s\": %m",
									path)));
				else
					ereport(LOG,
							(errmsg("TDE: removed orphan temp key file \"%s\"",
									path)));
			}
		}
		FreeDir(dir);
	}

	/*
	 * Preload DEKs for all tablespaces that already have .wkey files.
	 * We attempt to load every <oid>.wkey in the directory; failures to
	 * contact the KMS are non-fatal (tablespace stays locked).
	 */
	{
		DIR		   *dir;
		struct dirent *de;

		dir = AllocateDir(TBLSPC_KMGR_DIR);
		while ((de = ReadDir(dir, TBLSPC_KMGR_DIR)) != NULL)
		{
			Oid		spcOid;
			char   *endptr;
			int		len = strlen(de->d_name);

			/* Match "<digits>.wkey" */
			if (len <= 5 || strcmp(de->d_name + len - 5, ".wkey") != 0)
				continue;

			errno = 0;
			spcOid = (Oid) strtoul(de->d_name, &endptr, 10);
			if (errno != 0 || endptr != de->d_name + len - 5 || spcOid == InvalidOid)
				continue;	/* not a valid OID-named file */

			(void) TblspcKmgrLoadKey(spcOid);	/* non-fatal on KMS failure */
		}
		FreeDir(dir);
	}

	/* Warn if WAL encryption is not also enabled (FPI plaintext issue) */
	{
		extern char *cluster_key_command;	/* from kmgr.h */

		if (!cluster_key_command || cluster_key_command[0] == '\0')
			ereport(WARNING,
					(errmsg("tablespace TDE is active but cluster-level WAL "
							"encryption is not enabled"),
					 errdetail("WAL full-page images for encrypted tablespaces "
							   "will be written in plaintext."),
					 errhint("Set cluster_key_command in postgresql.conf to "
							 "protect WAL archives as well.")));
	}
}

/* ----------------------------------------------------------------
 * Public API: CREATE TABLESPACE
 * ---------------------------------------------------------------- */

uint8
TblspcKmgrCreateKey(Oid spcOid,
					const char *enc_method_str,
					const char *kms_key_id)
{
	uint8			enc_method;
	uint8			provider;
	int				dek_len;
	uint8			dek[KMGR_MAX_KEY_LEN_BYTES];
	uint8			wrapped[TBLSPC_MAX_WRAPPED_DEK_LEN];
	int				wrapped_len;
	const KmsOps   *ops;
	const char	   *effective_key_id;

	enc_method = TblspcEncMethodFromStr(enc_method_str);
	if (enc_method == TBLSPC_ENC_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unknown encryption method \"%s\"",
						enc_method_str ? enc_method_str : "(null)"),
				 errhint("Valid values: AES128, AES192, AES256, SM4.")));

	provider = TblspcCurrentProvider();
	if (provider == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("no KMS provider configured"),
				 errhint("Set tde_kms_provider in postgresql.conf and restart.")));

	ops = KmsGetOps(provider);

	/* Determine the effective KMS key ID */
	if (kms_key_id && kms_key_id[0] != '\0')
		effective_key_id = kms_key_id;
	else if (tde_kms_default_key_id && tde_kms_default_key_id[0] != '\0')
		effective_key_id = tde_kms_default_key_id;
	else
		effective_key_id = "";	/* builtin ignores this */

	/* Generate a fresh random DEK */
	dek_len = TblspcEncMethodKeyLen(enc_method);
	Assert(dek_len > 0 && dek_len <= (int) sizeof(dek));
	memset(dek, 0, sizeof(dek));

	if (!pg_strong_random(dek, dek_len))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not generate random DEK for tablespace oid=%u",
						spcOid)));

	/* Wrap the DEK via KMS */
	wrapped_len = 0;
	if (!ops->wrap_dek(dek, dek_len, effective_key_id, wrapped, &wrapped_len))
	{
		explicit_bzero(dek, sizeof(dek));
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("KMS failed to wrap DEK for tablespace oid=%u", spcOid)));
	}

	/* Persist to disk (atomic write) */
	wkey_write(spcOid, enc_method, provider, effective_key_id,
			   wrapped, wrapped_len);

	/* Load into shared memory */
	shmem_store_dek(spcOid, enc_method, dek, dek_len);
	explicit_bzero(dek, sizeof(dek));
	explicit_bzero(wrapped, sizeof(wrapped));

	ereport(LOG,
			(errmsg("TDE: new DEK generated for tablespace oid=%u "
					"(method=%u, provider=%u)",
					spcOid, enc_method, provider)));

	return enc_method;
}

/* ----------------------------------------------------------------
 * Public API: DROP TABLESPACE
 * ---------------------------------------------------------------- */

void
TblspcKmgrDropKey(Oid spcOid)
{
	char	path[MAXPGPATH];
	int		i;

	/*
	 * Nothing to do when TDE is not configured: no DEK can be loaded and no
	 * .wkey file can exist, so skip the shmem lock, the unlink() syscall and
	 * the log line on every DROP TABLESPACE in a non-encrypted cluster.
	 */
	if (TblspcCurrentProvider() == 0)
		return;

	/* Evict from shmem */
	if (TblspcKeyShmem)
	{
		LWLockAcquire(&TblspcKeyShmem->lock, LW_EXCLUSIVE);
		for (i = 0; i < TblspcKeyShmem->max_entries; i++)
		{
			SpcEncEntry *e = &TblspcKeyShmem->entries[i];

			if (e->spc_oid == spcOid)
			{
				explicit_bzero(e->dek, sizeof(e->dek));
				e->spc_oid  = InvalidOid;
				e->present  = 0;
				e->dek_len  = 0;
				TblspcKeyShmem->nentries--;
				break;
			}
		}
		LWLockRelease(&TblspcKeyShmem->lock);
	}

	/* Remove .wkey file */
	TblspcWkeyPath(path, spcOid);
	if (unlink(path) < 0)
	{
		/*
		 * ENOENT means this tablespace was never encrypted (no .wkey), which
		 * is a normal case even when a KMS provider is configured -- stay
		 * silent rather than claim a DEK was dropped.
		 */
		if (errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove tablespace key file \"%s\": %m", path)));
	}
	else
		ereport(LOG,
				(errmsg("TDE: dropped DEK for tablespace oid=%u", spcOid)));
}

/* ----------------------------------------------------------------
 * Public API: load a single key from disk
 * ---------------------------------------------------------------- */

bool
TblspcKmgrLoadKey(Oid spcOid)
{
	uint8			enc_method;
	uint8			provider;
	char		   *kms_key_id = NULL;
	uint8		   *wrapped = NULL;
	int				wrapped_len;
	uint8			dek[KMGR_MAX_KEY_LEN_BYTES];
	int				dek_len;
	const KmsOps   *ops;
	bool			ok;

	if (!wkey_read(spcOid, &enc_method, &provider, &kms_key_id,
				   &wrapped, &wrapped_len))
		return false;

	/*
	 * Use the provider recorded in the .wkey file (not the current GUC)
	 * so that we can still load keys if the provider GUC was changed.
	 * This matters after a provider migration.
	 */
	PG_TRY();
	{
		ops = KmsGetOps(provider);
		ok = ops->unwrap_dek(wrapped, wrapped_len, kms_key_id, dek, &dek_len);
	}
	PG_CATCH();
	{
		pfree(kms_key_id);
		pfree(wrapped);
		explicit_bzero(dek, sizeof(dek));
		PG_RE_THROW();
	}
	PG_END_TRY();

	pfree(kms_key_id);
	pfree(wrapped);

	if (!ok)
	{
		explicit_bzero(dek, sizeof(dek));
		ereport(WARNING,
				(errmsg("TDE: could not unlock tablespace oid=%u; "
						"tablespace will be inaccessible until tde_open_tablespace() "
						"is called after the KMS issue is resolved",
						spcOid)));
		return false;
	}

	shmem_store_dek(spcOid, enc_method, dek, dek_len);
	explicit_bzero(dek, sizeof(dek));

	ereport(LOG,
			(errmsg("TDE: tablespace oid=%u unlocked via provider %u",
					spcOid, provider)));
	return true;
}

/* ----------------------------------------------------------------
 * Public API: rescan key directory and load any missing keys
 * ---------------------------------------------------------------- */

int
TblspcKmgrSyncKeys(void)
{
	DIR		   *dir;
	struct dirent *de;
	int			nloaded = 0;

	if (TblspcCurrentProvider() == 0)
		return 0;

	dir = AllocateDir(TBLSPC_KMGR_DIR);
	if (!dir)
		return 0;				/* directory may not exist yet */

	while ((de = ReadDir(dir, TBLSPC_KMGR_DIR)) != NULL)
	{
		Oid		spcOid;
		char   *endptr;
		int		len = strlen(de->d_name);

		if (len <= 5 || strcmp(de->d_name + len - 5, ".wkey") != 0)
			continue;

		errno = 0;
		spcOid = (Oid) strtoul(de->d_name, &endptr, 10);
		if (errno != 0 || endptr != de->d_name + len - 5 || spcOid == InvalidOid)
			continue;

		/* Load only if not already in shmem */
		if (!TblspcEncryptionEnabled(spcOid) && TblspcKmgrLoadKey(spcOid))
			nloaded++;
	}

	FreeDir(dir);
	return nloaded;
}
