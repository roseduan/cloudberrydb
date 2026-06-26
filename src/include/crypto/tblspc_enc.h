/*-------------------------------------------------------------------------
 *
 * tblspc_enc.h
 *	  Per-tablespace encryption / decryption API for storage engines.
 *
 * Storage engine code (bufmgr.c, cdbvarblock.c, datumstreamblock.c, etc.)
 * calls these functions to perform tablespace-level TDE.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/include/crypto/tblspc_enc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TBLSPC_ENC_H
#define TBLSPC_ENC_H

#include <sys/types.h>			/* for off_t */

#include "crypto/tblspc_kmgr.h"
#include "storage/bufpage.h"
#include "storage/buf.h"
#include "storage/relfilenode.h"

/* Heap / index / TOAST pages */
extern void EncryptPageForSpc(Page page, Oid spcOid,
							  ForkNumber forkNum, BlockNumber blkno);
extern void DecryptPageForSpc(Page page, Oid spcOid,
							  ForkNumber forkNum, BlockNumber blkno);

/* AO Row / AOCS blocks */
extern void EncryptAOBlockForSpc(unsigned char *data_buf, int buf_len,
								 RelFileNode *rnode);
extern void DecryptAOBlockForSpc(unsigned char *data_buf, int buf_len,
								 RelFileNode *rnode);

/* PAX ORC micro-partition files (offset-based AES-CTR only) */
extern void EncryptPaxAtOffsetForSpc(unsigned char *buf, size_t len,
									 off_t file_offset,
									 Oid spcOid, Oid dbNode);
extern void DecryptPaxAtOffsetForSpc(unsigned char *buf, size_t len,
									 off_t file_offset,
									 Oid spcOid, Oid dbNode);

#endif							/* TBLSPC_ENC_H */
