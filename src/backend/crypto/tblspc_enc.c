/*-------------------------------------------------------------------------
 *
 * tblspc_enc.c
 *	  Per-tablespace encryption / decryption for all storage engines.
 *
 * Provides four public functions used by storage engine code:
 *
 *   EncryptPageForSpc()       – Heap / index / TOAST pages (called from
 *                               bufmgr.c at flush time)
 *   DecryptPageForSpc()       – Heap / index / TOAST pages (called from
 *                               bufmgr.c at read time)
 *   EncryptAOBlockForSpc()    – AO Row and AOCS blocks (cdbvarblock.c,
 *                               datumstreamblock.c)
 *   DecryptAOBlockForSpc()    – AO Row and AOCS blocks
 *
 * IV construction (little-endian, platform-independent):
 *
 *   Heap:  bytes 0-3 = spcOid (LE), bytes 4-7 = blkno (LE), bytes 8-15 = 0x80
 *   AO:    bytes 0-3 = spcOid (LE), bytes 4-7 = dbNode (LE), bytes 8-15 = 0x80
 *
 * Per-backend cipher context cache:
 *
 * PgCipherCtx (an EVP_CIPHER_CTX) is a heap-allocated OpenSSL object that
 * cannot live in shared memory.  Each backend keeps a small process-local
 * array of (spc_oid → enc_ctx, dec_ctx) pairs.  The cache is lazily
 * populated on first use and holds at most TBLSPC_LOCAL_CTX_MAX entries.
 * On cache eviction the old contexts are freed.
 *
 * SM4 note: SM4-OFB uses the same-named sm4_ctx struct from sm4_ofb.h and
 * does NOT go through OpenSSL EVP.  We detect SM4 by enc_method and call
 * the sm4_ofb_* functions directly, mirroring bufenc.c.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/crypto/tblspc_enc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/cipher.h"
#include "common/kmgr_utils.h"
#include "crypto/bufenc.h"
#include "crypto/sm4_ctr.h"
#include "crypto/sm4_ofb.h"
#include "crypto/tblspc_enc.h"
#include "crypto/tblspc_kmgr.h"
#include "storage/bufpage.h"
#include "storage/relfilenode.h"

/* ----------------------------------------------------------------
 * Per-backend cipher context cache
 * ---------------------------------------------------------------- */

#define TBLSPC_LOCAL_CTX_MAX	32

typedef struct SpcLocalCtx
{
	Oid				spc_oid;
	uint8			enc_method;
	/* For AES paths */
	PgCipherCtx	   *aes_enc_ctx;
	PgCipherCtx	   *aes_dec_ctx;
	/* For SM4-OFB path (Heap / AO / AOCS) */
	sm4_ctx			sm4_enc;
	sm4_ctx			sm4_dec;
	/* For SM4-CTR path (PAX random-offset reads) */
	SM4_KEY			sm4_ctr_key;
} SpcLocalCtx;

static SpcLocalCtx	LocalCtxCache[TBLSPC_LOCAL_CTX_MAX];
static int			LocalCtxCount = 0;
static bool			LocalCtxInited = false;

static void
ensure_local_ctx_inited(void)
{
	int		i;

	if (LocalCtxInited)
		return;
	for (i = 0; i < TBLSPC_LOCAL_CTX_MAX; i++)
		LocalCtxCache[i].spc_oid = InvalidOid;
	LocalCtxInited = true;
}

/*
 * Return the local context for spcOid, creating it if necessary.
 * If the cache is full, evict entry 0 (simple FIFO; good enough for
 * typical workloads with ≤ 10 encrypted tablespaces).
 */
static SpcLocalCtx *
get_or_create_local_ctx(Oid spcOid)
{
	SpcEncEntry	   *shmem_e;
	SpcLocalCtx	   *slot;
	int				i;
	int				pg_cipher_id;

	ensure_local_ctx_inited();

	/* Fast lookup */
	for (i = 0; i < LocalCtxCount; i++)
	{
		if (LocalCtxCache[i].spc_oid == spcOid)
			return &LocalCtxCache[i];
	}

	/* Not cached; retrieve DEK from shared memory */
	shmem_e = TblspcGetEncEntry(spcOid);
	if (!shmem_e)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("encrypted tablespace oid=%u is locked", spcOid),
				 errhint("Run tde_open_tablespace('%u') to unlock it.",
						 spcOid)));

	/* Choose a slot */
	if (LocalCtxCount < TBLSPC_LOCAL_CTX_MAX)
	{
		slot = &LocalCtxCache[LocalCtxCount++];
	}
	else
	{
		/* Evict slot 0 */
		slot = &LocalCtxCache[0];
		if (slot->enc_method != TBLSPC_ENC_SM4)
		{
			if (slot->aes_enc_ctx) pg_cipher_ctx_free(slot->aes_enc_ctx);
			if (slot->aes_dec_ctx) pg_cipher_ctx_free(slot->aes_dec_ctx);
		}
		/* shift remaining slots down one */
		memmove(&LocalCtxCache[0], &LocalCtxCache[1],
				(TBLSPC_LOCAL_CTX_MAX - 1) * sizeof(SpcLocalCtx));
		slot = &LocalCtxCache[TBLSPC_LOCAL_CTX_MAX - 1];
	}

	slot->spc_oid    = spcOid;
	slot->enc_method = shmem_e->enc_method;
	slot->aes_enc_ctx = NULL;
	slot->aes_dec_ctx = NULL;

	if (shmem_e->enc_method == TBLSPC_ENC_SM4)
	{
		sm4_ofb_setkey_enc(&slot->sm4_enc, shmem_e->dek);
		sm4_ofb_setkey_dec(&slot->sm4_dec, shmem_e->dek);
		sm4_ctr_setkey(&slot->sm4_ctr_key, shmem_e->dek);
	}
	else
	{
		switch (shmem_e->enc_method)
		{
			case TBLSPC_ENC_AES128: pg_cipher_id = PG_CIPHER_AES_CTR; break;
			case TBLSPC_ENC_AES192: pg_cipher_id = PG_CIPHER_AES_CTR; break;
			case TBLSPC_ENC_AES256: pg_cipher_id = PG_CIPHER_AES_CTR; break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unknown enc_method %u for tablespace oid=%u",
								shmem_e->enc_method, spcOid)));
				pg_cipher_id = PG_CIPHER_AES_CTR;	/* unreachable */
		}

		slot->aes_enc_ctx = pg_cipher_ctx_create(pg_cipher_id,
												 shmem_e->dek,
												 shmem_e->dek_len,
												 true);
		slot->aes_dec_ctx = pg_cipher_ctx_create(pg_cipher_id,
												 shmem_e->dek,
												 shmem_e->dek_len,
												 false);
		if (!slot->aes_enc_ctx || !slot->aes_dec_ctx)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not create cipher context for tablespace oid=%u",
							spcOid)));
	}

	return slot;
}

/* ----------------------------------------------------------------
 * IV helpers (always little-endian)
 * ---------------------------------------------------------------- */

/*
 * Build a 16-byte IV for a Heap page.
 * IV layout: [spcOid LE 4B] [blkno LE 4B] [0x80 × 8B]
 */
static void
build_heap_iv(uint8 iv[PG_AES_IV_SIZE], Oid spcOid, BlockNumber blkno)
{
	iv[0]  = (uint8) (spcOid);
	iv[1]  = (uint8) (spcOid >> 8);
	iv[2]  = (uint8) (spcOid >> 16);
	iv[3]  = (uint8) (spcOid >> 24);
	iv[4]  = (uint8) (blkno);
	iv[5]  = (uint8) (blkno >> 8);
	iv[6]  = (uint8) (blkno >> 16);
	iv[7]  = (uint8) (blkno >> 24);
	memset(iv + 8, 0x80, 8);
}

/*
 * Build a 16-byte IV for an AO / AOCS block.
 * IV layout: [spcOid LE 4B] [dbNode LE 4B] [0x80 × 8B]
 */
static void
build_ao_iv(uint8 iv[PG_AES_IV_SIZE], Oid spcOid, Oid dbNode)
{
	iv[0]  = (uint8) (spcOid);
	iv[1]  = (uint8) (spcOid >> 8);
	iv[2]  = (uint8) (spcOid >> 16);
	iv[3]  = (uint8) (spcOid >> 24);
	iv[4]  = (uint8) (dbNode);
	iv[5]  = (uint8) (dbNode >> 8);
	iv[6]  = (uint8) (dbNode >> 16);
	iv[7]  = (uint8) (dbNode >> 24);
	memset(iv + 8, 0x80, 8);
}

/* ----------------------------------------------------------------
 * Core encrypt / decrypt
 * ---------------------------------------------------------------- */

/*
 * Encrypt or decrypt `len` bytes in-place at `data` using the cipher
 * context for the given tablespace.  The IV is caller-supplied.
 */
static void
spc_crypt_inplace(Oid spcOid,
				  unsigned char *data, int len,
				  const uint8 iv[PG_AES_IV_SIZE],
				  bool encrypt)
{
	SpcLocalCtx	   *lctx;
	int				outlen;

	lctx = get_or_create_local_ctx(spcOid);

	if (lctx->enc_method == TBLSPC_ENC_SM4)
	{
		/*
		 * sm4_ofb_cipher(ctx, out, in, len, ivec)
		 * SM4-OFB is symmetric (encrypt == decrypt), but we maintain
		 * separate enc/dec contexts to mirror bufenc.c conventions.
		 * In-place: out == in.
		 */
		if (encrypt)
			sm4_ofb_cipher(&lctx->sm4_enc,
						   data,
						   (const unsigned char *) data,
						   (size_t) len,
						   (unsigned char *) iv);
		else
			sm4_ofb_cipher(&lctx->sm4_enc,	/* OFB: enc ctx used for both */
						   data,
						   (const unsigned char *) data,
						   (size_t) len,
						   (unsigned char *) iv);
	}
	else
	{
		bool ok;

		if (encrypt)
			ok = pg_cipher_encrypt(lctx->aes_enc_ctx,
								   PG_CIPHER_AES_CTR,
								   data, len,
								   data, &outlen,
								   iv, PG_AES_IV_SIZE,
								   NULL, 0);
		else
			ok = pg_cipher_decrypt(lctx->aes_dec_ctx,
								   PG_CIPHER_AES_CTR,
								   data, len,
								   data, &outlen,
								   iv, PG_AES_IV_SIZE,
								   NULL, 0);

		if (!ok)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("AES-CTR %s failed for tablespace oid=%u",
							encrypt ? "encryption" : "decryption", spcOid)));
	}
}

/* ----------------------------------------------------------------
 * Public API: Heap / index / TOAST pages
 * ---------------------------------------------------------------- */

/*
 * Encrypt a Heap page for tablespace spcOid.
 *
 * Mirrors EncryptPage() / PageEncryptCopy() in bufenc.c but uses the
 * per-tablespace DEK instead of the cluster key.
 *
 * Encrypts from PageEncryptOffset onward (skips the page header).
 * The page is modified in-place; caller must pass a writeable copy if
 * the original shared buffer must be preserved.
 */
void
EncryptPageForSpc(Page page, Oid spcOid, ForkNumber forkNum, BlockNumber blkno)
{
	uint8			iv[PG_AES_IV_SIZE];
	unsigned char  *data;
	int				len;

	/* Only encrypt main fork */
	if (forkNum != MAIN_FORKNUM)
		return;
	if (!TblspcEncryptionEnabled(spcOid))
		return;

	data = (unsigned char *) page + PageEncryptOffset;
	len  = (int) (BLCKSZ - PageEncryptOffset);

	build_heap_iv(iv, spcOid, blkno);
	spc_crypt_inplace(spcOid, data, len, iv, true);
}

/*
 * Decrypt a Heap page for tablespace spcOid.
 * Decryption is symmetric with encryption (AES-CTR / SM4-OFB are
 * self-inverse stream ciphers).
 */
void
DecryptPageForSpc(Page page, Oid spcOid, ForkNumber forkNum, BlockNumber blkno)
{
	uint8			iv[PG_AES_IV_SIZE];
	unsigned char  *data;
	int				len;

	if (forkNum != MAIN_FORKNUM)
		return;
	if (!TblspcEncryptionEnabled(spcOid))
		return;

	data = (unsigned char *) page + PageEncryptOffset;
	len  = (int) (BLCKSZ - PageEncryptOffset);

	build_heap_iv(iv, spcOid, blkno);
	spc_crypt_inplace(spcOid, data, len, iv, false);
}

/* ----------------------------------------------------------------
 * Public API: AO Row / AOCS blocks
 * ---------------------------------------------------------------- */

/*
 * Encrypt an AO / AOCS block buffer for tablespace spcOid.
 *
 * Mirrors EncryptAOBLock() in bufenc.c.
 * rnode->spcNode is used to identify the tablespace; rnode->dbNode
 * contributes to the IV for per-database uniqueness.
 */
void
EncryptAOBlockForSpc(unsigned char *data_buf,
					 int buf_len,
					 RelFileNode *rnode)
{
	uint8	iv[PG_AES_IV_SIZE];
	Oid		spcOid = rnode->spcNode;

	if (!TblspcEncryptionEnabled(spcOid))
		return;

	build_ao_iv(iv, spcOid, rnode->dbNode);
	spc_crypt_inplace(spcOid, data_buf, buf_len, iv, true);
}

/*
 * Decrypt an AO / AOCS block buffer for tablespace spcOid.
 */
void
DecryptAOBlockForSpc(unsigned char *data_buf,
					 int buf_len,
					 RelFileNode *rnode)
{
	uint8	iv[PG_AES_IV_SIZE];
	Oid		spcOid = rnode->spcNode;

	if (!TblspcEncryptionEnabled(spcOid))
		return;

	build_ao_iv(iv, spcOid, rnode->dbNode);
	spc_crypt_inplace(spcOid, data_buf, buf_len, iv, false);
}

/* ----------------------------------------------------------------
 * Public API: PAX ORC micro-partition files (offset-addressable CTR)
 * ---------------------------------------------------------------- */

/*
 * build_pax_iv — IV for a PAX micro-partition file at a given counter.
 *
 * IV layout: [spcOid LE 4B][dbNode LE 4B][block_counter BE 8B]
 * where block_counter = aligned_file_offset / PG_AES_IV_SIZE.
 */
static void
build_pax_iv(uint8 iv[PG_AES_IV_SIZE], Oid spcOid, Oid dbNode,
			 uint64 block_counter)
{
	iv[0]  = (uint8) (spcOid);
	iv[1]  = (uint8) (spcOid >> 8);
	iv[2]  = (uint8) (spcOid >> 16);
	iv[3]  = (uint8) (spcOid >> 24);
	iv[4]  = (uint8) (dbNode);
	iv[5]  = (uint8) (dbNode >> 8);
	iv[6]  = (uint8) (dbNode >> 16);
	iv[7]  = (uint8) (dbNode >> 24);
	/* Counter in big-endian so OpenSSL CTR increment matches our IVs */
	iv[8]  = (uint8) ((block_counter) >> 56);
	iv[9]  = (uint8) ((block_counter) >> 48);
	iv[10] = (uint8) ((block_counter) >> 40);
	iv[11] = (uint8) ((block_counter) >> 32);
	iv[12] = (uint8) ((block_counter) >> 24);
	iv[13] = (uint8) ((block_counter) >> 16);
	iv[14] = (uint8) ((block_counter) >> 8);
	iv[15] = (uint8) (block_counter);
}

/*
 * pax_crypt_at_offset — Encrypt or decrypt `len` bytes at `file_offset`.
 *
 * Only AES-CTR is supported for PAX; SM4-OFB is not position-addressable.
 * The IV is derived from (spcOid, dbNode, aligned_offset/16) so that any
 * positional read/write produces consistent ciphertext.
 */
static void
pax_crypt_at_offset(Oid spcOid, Oid dbNode,
					unsigned char *data, size_t len, off_t file_offset,
					bool encrypt)
{
	SpcLocalCtx    *lctx;
	uint8			iv[PG_AES_IV_SIZE];
	uint64			aligned_offset;
	size_t			skip;
	int				outlen;
	bool			ok;

	lctx = get_or_create_local_ctx(spcOid);

	if (len == 0)
		return;

	aligned_offset = (uint64) file_offset &
		~(uint64) (PG_AES_IV_SIZE - 1);
	skip = (size_t) ((uint64) file_offset - aligned_offset);

	build_pax_iv(iv, spcOid, dbNode,
				 aligned_offset / (uint64) PG_AES_IV_SIZE);

	if (lctx->enc_method == TBLSPC_ENC_SM4)
	{
		/*
		 * SM4-CTR path: position-addressable, identical to AES-CTR logic
		 * but uses our software SM4 implementation.  The "prepend zeros"
		 * trick works because CTR keystream blocks are independent.
		 */
		if (skip == 0)
		{
			sm4_ctr_cipher(&lctx->sm4_ctr_key, data,
						   (const unsigned char *) data, len, iv);
		}
		else
		{
			size_t			total = skip + len;
			unsigned char  *tmp = (unsigned char *) palloc(total);

			memset(tmp, 0, skip);
			memcpy(tmp + skip, data, len);
			sm4_ctr_cipher(&lctx->sm4_ctr_key, tmp,
						   (const unsigned char *) tmp, total, iv);
			memcpy(data, tmp + skip, len);
			pfree(tmp);
		}
		return;
	}

	/* AES-CTR path */
	if (skip == 0)
	{
		/* Fast path: offset is 16-byte aligned */
		if (encrypt)
			ok = pg_cipher_encrypt(lctx->aes_enc_ctx,
								   PG_CIPHER_AES_CTR,
								   data, (int) len,
								   data, &outlen,
								   iv, PG_AES_IV_SIZE, NULL, 0);
		else
			ok = pg_cipher_decrypt(lctx->aes_dec_ctx,
								   PG_CIPHER_AES_CTR,
								   data, (int) len,
								   data, &outlen,
								   iv, PG_AES_IV_SIZE, NULL, 0);
	}
	else
	{
		/*
		 * Unaligned path: prepend `skip` zero bytes to advance the CTR
		 * keystream to the right position within the aligned block.
		 */
		size_t			total = skip + len;
		unsigned char  *tmp = (unsigned char *) palloc(total);

		memset(tmp, 0, skip);
		memcpy(tmp + skip, data, len);

		if (encrypt)
			ok = pg_cipher_encrypt(lctx->aes_enc_ctx,
								   PG_CIPHER_AES_CTR,
								   tmp, (int) total,
								   tmp, &outlen,
								   iv, PG_AES_IV_SIZE, NULL, 0);
		else
			ok = pg_cipher_decrypt(lctx->aes_dec_ctx,
								   PG_CIPHER_AES_CTR,
								   tmp, (int) total,
								   tmp, &outlen,
								   iv, PG_AES_IV_SIZE, NULL, 0);

		if (ok)
			memcpy(data, tmp + skip, len);

		pfree(tmp);
	}

	if (!ok)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("AES-CTR %s failed for PAX file (tablespace oid=%u)",
						encrypt ? "encryption" : "decryption", spcOid)));
}

/*
 * EncryptPaxAtOffsetForSpc — Encrypt `len` bytes of PAX file data.
 * Called from LocalFile::PWrite() / LocalFile::Write().
 */
void
EncryptPaxAtOffsetForSpc(unsigned char *buf, size_t len, off_t file_offset,
						 Oid spcOid, Oid dbNode)
{
	if (!TblspcEncryptionEnabled(spcOid) || len == 0)
		return;

	pax_crypt_at_offset(spcOid, dbNode, buf, len, file_offset, true);
}

/*
 * DecryptPaxAtOffsetForSpc — Decrypt `len` bytes of PAX file data.
 * Called from LocalFile::PRead() / LocalFile::ReadBatch().
 */
void
DecryptPaxAtOffsetForSpc(unsigned char *buf, size_t len, off_t file_offset,
						 Oid spcOid, Oid dbNode)
{
	if (!TblspcEncryptionEnabled(spcOid) || len == 0)
		return;

	pax_crypt_at_offset(spcOid, dbNode, buf, len, file_offset, false);
}
