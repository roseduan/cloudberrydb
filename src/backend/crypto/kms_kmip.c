/*-------------------------------------------------------------------------
 *
 * kms_kmip.c
 *	  Native KMIP 2.x provider for tablespace-level TDE.
 *
 * Implements the KMIP TTLV binary protocol over mTLS (TCP port 5696).
 * The KEK never leaves the KMS: Encrypt/Decrypt operations are performed
 * server-side.  Only the wrapped blob (IV + AuthTag + Ciphertext) is stored
 * in the .wkey file.
 *
 * Required GUCs:
 *   tde_kms_host           KMIP server hostname or IP
 *   tde_kms_port           Port (default 5696)
 *   tde_kms_ca_cert        Path to PEM CA certificate
 *   tde_kms_client_cert    Path to PEM client certificate (mTLS)
 *   tde_kms_client_key     Path to PEM client private key (mTLS)
 *
 * Optional:
 *   tde_kms_operation_timeout  seconds (SO_RCVTIMEO / SO_SNDTIMEO)
 *
 * Wrapped DEK on-disk layout (60 bytes for AES-256 DEK):
 *   [IV 12B][AuthTag 16B][Ciphertext N B]
 *
 * KMIP TTLV encoding (big-endian):
 *   [Tag 3B][Type 1B][Length 4B][Value (padded to 8-byte boundary)]
 *
 * KMIP tag constants (OASIS KMIP 2.1 spec, Table 4-1):
 *   0x420011  BlockCipherMode   0x420028  CryptographicAlgorithm
 *   0x42002B  CryptographicParameters   0x42003D  IVCounterNonce
 *   0x4200C2  Data (Encrypt/Decrypt)    0x4200FF  AuthenticatedEncryptionTag
 *   0x420094  UniqueIdentifier  0x42005C  Operation
 *   0x420078  RequestMessage    0x42007B  ResponseMessage
 *   0x420077  RequestHeader     0x42007A  ResponseHeader
 *   0x420069  ProtocolVersion   0x42006A  PVMajor   0x42006B  PVMinor
 *   0x42000D  BatchCount        0x42000F  BatchItem
 *   0x420079  RequestPayload    0x42007C  ResponsePayload
 *   0x42007F  ResultStatus      0x42007D  ResultMessage
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *	  src/backend/crypto/kms_kmip.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef USE_OPENSSL

#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/ssl.h>

#include "crypto/tblspc_kmgr.h"
#include "kms_client.h"
#include "utils/palloc.h"

/* ----------------------------------------------------------------
 * KMIP TTLV constants
 * ---------------------------------------------------------------- */

/* Tags (3 bytes, written big-endian as part of a 4-byte field) */
#define KMIP_TAG_BATCH_COUNT              0x42000D
#define KMIP_TAG_BATCH_ITEM               0x42000F
#define KMIP_TAG_BLOCK_CIPHER_MODE        0x420011	/* BlockCipherMode (Enumeration) */
#define KMIP_TAG_CRYPTOGRAPHIC_ALGORITHM  0x420028	/* CryptographicAlgorithm (Enumeration) */
#define KMIP_TAG_CRYPTOGRAPHIC_PARAMETERS 0x42002B	/* CryptographicParameters (Structure) */
#define KMIP_TAG_IV_COUNTER_NONCE         0x42003D	/* IVCounterNonce (ByteString) */
#define KMIP_TAG_OPERATION                0x42005C
#define KMIP_TAG_PROTOCOL_VERSION         0x420069
#define KMIP_TAG_PROTOCOL_VERSION_MAJOR   0x42006A
#define KMIP_TAG_PROTOCOL_VERSION_MINOR   0x42006B
#define KMIP_TAG_REQUEST_HEADER           0x420077
#define KMIP_TAG_REQUEST_MESSAGE          0x420078
#define KMIP_TAG_REQUEST_PAYLOAD          0x420079
#define KMIP_TAG_RESPONSE_HEADER          0x42007A
#define KMIP_TAG_RESPONSE_MESSAGE         0x42007B
#define KMIP_TAG_RESPONSE_PAYLOAD         0x42007C
#define KMIP_TAG_RESULT_MESSAGE           0x42007D
#define KMIP_TAG_RESULT_STATUS            0x42007F
#define KMIP_TAG_TIME_STAMP               0x420092
#define KMIP_TAG_UNIQUE_IDENTIFIER        0x420094
#define KMIP_TAG_DATA                     0x4200C2	/* Data (ByteString) — Encrypt/Decrypt payload */
#define KMIP_TAG_AUTH_ENCRYPTION_TAG      0x4200FF	/* AuthenticatedEncryptionTag (ByteString) */

/* TTLV Types */
#define KMIP_TYPE_STRUCTURE               0x01
#define KMIP_TYPE_INTEGER                 0x02
#define KMIP_TYPE_ENUMERATION             0x05
#define KMIP_TYPE_TEXT_STRING             0x07
#define KMIP_TYPE_BYTE_STRING             0x08

/* Operation enumeration values (KMIP spec Table 9-1) */
#define KMIP_OPERATION_ENCRYPT            0x0000001F
#define KMIP_OPERATION_DECRYPT            0x00000020

/* CryptographicAlgorithm enumeration */
#define KMIP_CRYPTO_ALGO_AES              0x00000003

/* BlockCipherMode enumeration */
#define KMIP_BLOCK_CIPHER_MODE_GCM        0x00000009

/* Result Status enumeration */
#define KMIP_RESULT_STATUS_SUCCESS        0x00000000

/* Wrapped DEK layout (same as kms_cosmian.c) */
#define KMIP_IV_LEN                       12
#define KMIP_TAG_LEN                      16
#define KMIP_HDR_LEN                      (KMIP_IV_LEN + KMIP_TAG_LEN)	/* 28 */

/* Max response buffer (1 MiB) */
#define KMIP_MAX_RESPONSE                 (1024 * 1024)

/* ----------------------------------------------------------------
 * TTLV buffer writer
 * ---------------------------------------------------------------- */

typedef struct
{
	uint8	   *buf;
	int			cap;
	int			len;
} TtlvBuf;

static void
tbuf_ensure(TtlvBuf *b, int need)
{
	if (b->len + need <= b->cap)
		return;
	b->cap = (b->len + need) * 2 + 64;
	b->buf = repalloc(b->buf, b->cap);
}

/* Write 4-byte big-endian uint32 */
static void
tbuf_put_u32(TtlvBuf *b, uint32 v)
{
	tbuf_ensure(b, 4);
	b->buf[b->len + 0] = (uint8) (v >> 24);
	b->buf[b->len + 1] = (uint8) (v >> 16);
	b->buf[b->len + 2] = (uint8) (v >> 8);
	b->buf[b->len + 3] = (uint8) (v);
	b->len += 4;
}

/*
 * Write TTLV item header: [tag 3B as top 3 bytes of u32][type 1B][length 4B]
 * Total header size = 8 bytes.
 */
static void
tbuf_put_header(TtlvBuf *b, uint32 tag, uint8 type, uint32 vlen)
{
	tbuf_ensure(b, 8);
	/* Tag occupies the high 3 bytes of a 32-bit word; low byte = type */
	b->buf[b->len + 0] = (uint8) (tag >> 16);
	b->buf[b->len + 1] = (uint8) (tag >> 8);
	b->buf[b->len + 2] = (uint8) (tag);
	b->buf[b->len + 3] = type;
	b->len += 4;
	tbuf_put_u32(b, vlen);
}

/* Integer / Enumeration: 4-byte big-endian value + 4 bytes padding */
static void
tbuf_put_int(TtlvBuf *b, uint32 tag, uint8 type, int32 val)
{
	tbuf_ensure(b, 16);
	tbuf_put_header(b, tag, type, 4);
	tbuf_put_u32(b, (uint32) val);
	tbuf_put_u32(b, 0);				/* 4-byte padding */
}

static void
tbuf_put_integer(TtlvBuf *b, uint32 tag, int32 val)
{
	tbuf_put_int(b, tag, KMIP_TYPE_INTEGER, val);
}

static void
tbuf_put_enumeration(TtlvBuf *b, uint32 tag, uint32 val)
{
	tbuf_put_int(b, tag, KMIP_TYPE_ENUMERATION, (int32) val);
}

/* Text String: variable length + padding to 8-byte boundary */
static void
tbuf_put_textstring(TtlvBuf *b, uint32 tag, const char *s, int slen)
{
	int			pad = (8 - (slen % 8)) % 8;

	tbuf_ensure(b, 8 + slen + pad);
	tbuf_put_header(b, tag, KMIP_TYPE_TEXT_STRING, (uint32) slen);
	memcpy(b->buf + b->len, s, slen);
	b->len += slen;
	if (pad > 0)
	{
		memset(b->buf + b->len, 0, pad);
		b->len += pad;
	}
}

/* Byte String: variable length + padding */
static void
tbuf_put_bytestring(TtlvBuf *b, uint32 tag, const uint8 *data, int dlen)
{
	int			pad = (8 - (dlen % 8)) % 8;

	tbuf_ensure(b, 8 + dlen + pad);
	tbuf_put_header(b, tag, KMIP_TYPE_BYTE_STRING, (uint32) dlen);
	memcpy(b->buf + b->len, data, dlen);
	b->len += dlen;
	if (pad > 0)
	{
		memset(b->buf + b->len, 0, pad);
		b->len += pad;
	}
}

/*
 * Begin a Structure item.  Returns the offset of the 4-byte length field so
 * that tbuf_end_structure can patch it in later.
 */
static int
tbuf_begin_structure(TtlvBuf *b, uint32 tag)
{
	int			len_off;

	tbuf_ensure(b, 8);
	b->buf[b->len + 0] = (uint8) (tag >> 16);
	b->buf[b->len + 1] = (uint8) (tag >> 8);
	b->buf[b->len + 2] = (uint8) (tag);
	b->buf[b->len + 3] = KMIP_TYPE_STRUCTURE;
	b->len += 4;
	len_off = b->len;				/* remember where length field is */
	tbuf_put_u32(b, 0);			/* placeholder */
	return len_off;
}

/* Patch the structure length field (content bytes after the header). */
static void
tbuf_end_structure(TtlvBuf *b, int len_off)
{
	uint32		body = (uint32) (b->len - len_off - 4);

	b->buf[len_off + 0] = (uint8) (body >> 24);
	b->buf[len_off + 1] = (uint8) (body >> 16);
	b->buf[len_off + 2] = (uint8) (body >> 8);
	b->buf[len_off + 3] = (uint8) (body);
}

/* ----------------------------------------------------------------
 * TTLV reader / tag finder
 * ---------------------------------------------------------------- */

static uint32
read_u32(const uint8 *p)
{
	return ((uint32) p[0] << 24) | ((uint32) p[1] << 16) |
		((uint32) p[2] << 8) | (uint32) p[3];
}

/*
 * Iterate items in a flat TTLV buffer [buf, buf+len).
 * Each call advances *pos past the current item and fills:
 *   *out_tag  : 24-bit KMIP tag
 *   *out_type : TTLV type byte
 *   *out_voff : byte offset of value in buf
 *   *out_vlen : length of value (before padding)
 * Returns true while items remain.
 */
static bool
ttlv_next(const uint8 *buf, int buflen, int *pos,
		  uint32 *out_tag, uint8 *out_type, int *out_voff, uint32 *out_vlen)
{
	uint32		tag;
	uint8		type;
	uint32		vlen;
	int			item_end;

	if (*pos + 8 > buflen)
		return false;

	tag  = ((uint32) buf[*pos] << 16) | ((uint32) buf[*pos + 1] << 8) | buf[*pos + 2];
	type = buf[*pos + 3];
	vlen = read_u32(buf + *pos + 4);
	*pos += 8;

	if (out_tag)
		*out_tag = tag;
	*out_type = type;
	*out_voff = *pos;
	*out_vlen = vlen;

	/* Advance past value + padding */
	item_end = *pos + (int) vlen;
	if (vlen % 8 != 0)
		item_end += 8 - (vlen % 8);
	*pos = item_end;

	return (*pos <= buflen);
}

/*
 * Find a ByteString item with the given tag anywhere in a flat TTLV buffer.
 * Searches recursively into Structure items.
 * Returns true and writes data to out_data/out_len on success.
 */
static bool
ttlv_find_bytestring(const uint8 *buf, int buflen, uint32 want_tag,
					 uint8 *out_data, int *out_len, int max_out)
{
	int			pos = 0;
	uint32		tag;
	uint8		type;
	int			voff;
	uint32		vlen;

	while (ttlv_next(buf, buflen, &pos, &tag, &type, &voff, &vlen))
	{
		if (tag == want_tag && type == KMIP_TYPE_BYTE_STRING)
		{
			if ((int) vlen > max_out)
				return false;
			memcpy(out_data, buf + voff, vlen);
			*out_len = (int) vlen;
			return true;
		}
		/* Recurse into structures */
		if (type == KMIP_TYPE_STRUCTURE)
		{
			if (ttlv_find_bytestring(buf + voff, (int) vlen, want_tag,
									 out_data, out_len, max_out))
				return true;
		}
	}
	return false;
}

/*
 * Find a TextString item with the given tag; writes to out_str (NUL terminated).
 */
static bool
ttlv_find_textstring(const uint8 *buf, int buflen, uint32 want_tag,
					 char *out_str, int max_out)
{
	int			pos = 0;
	uint32		tag;
	uint8		type;
	int			voff;
	uint32		vlen;

	while (ttlv_next(buf, buflen, &pos, &tag, &type, &voff, &vlen))
	{
		if (tag == want_tag && type == KMIP_TYPE_TEXT_STRING)
		{
			if ((int) vlen >= max_out)
				return false;
			memcpy(out_str, buf + voff, vlen);
			out_str[vlen] = '\0';
			return true;
		}
		if (type == KMIP_TYPE_STRUCTURE)
		{
			if (ttlv_find_textstring(buf + voff, (int) vlen, want_tag,
									 out_str, max_out))
				return true;
		}
	}
	return false;
}

/*
 * Find an Enumeration or Integer item with the given tag.
 */
static bool
ttlv_find_uint32(const uint8 *buf, int buflen, uint32 want_tag, uint32 *out_val)
{
	int			pos = 0;
	uint32		tag;
	uint8		type;
	int			voff;
	uint32		vlen;

	while (ttlv_next(buf, buflen, &pos, &tag, &type, &voff, &vlen))
	{
		if (tag == want_tag &&
			(type == KMIP_TYPE_ENUMERATION || type == KMIP_TYPE_INTEGER) &&
			vlen >= 4)
		{
			*out_val = read_u32(buf + voff);
			return true;
		}
		if (type == KMIP_TYPE_STRUCTURE)
		{
			if (ttlv_find_uint32(buf + voff, (int) vlen, want_tag, out_val))
				return true;
		}
	}
	return false;
}

/* ----------------------------------------------------------------
 * TLS connection
 * ---------------------------------------------------------------- */

static SSL_CTX *
kmip_build_ssl_ctx(void)
{
	SSL_CTX    *ctx;

	if (!tde_kms_ca_cert || tde_kms_ca_cert[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("KMIP: tde_kms_ca_cert must be set for TLS connections")));

	ctx = SSL_CTX_new(TLS_client_method());
	if (!ctx)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("KMIP: SSL_CTX_new failed: %s",
						ERR_error_string(ERR_get_error(), NULL))));

	SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);

	if (SSL_CTX_load_verify_locations(ctx, tde_kms_ca_cert, NULL) != 1)
	{
		SSL_CTX_free(ctx);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("KMIP: could not load CA cert \"%s\": %s",
						tde_kms_ca_cert,
						ERR_error_string(ERR_get_error(), NULL))));
	}

	if (tde_kms_client_cert && tde_kms_client_cert[0] != '\0')
	{
		if (SSL_CTX_use_certificate_file(ctx, tde_kms_client_cert,
										 SSL_FILETYPE_PEM) != 1 ||
			SSL_CTX_use_PrivateKey_file(ctx, tde_kms_client_key,
										SSL_FILETYPE_PEM) != 1)
		{
			SSL_CTX_free(ctx);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("KMIP: could not load client cert/key: %s",
							ERR_error_string(ERR_get_error(), NULL))));
		}
	}

	return ctx;
}

/*
 * Send a KMIP TTLV request over mTLS and return the response as a palloc'd
 * buffer.  *resp_len receives the byte count.  Returns NULL on failure.
 */
static uint8 *
kmip_send_request(const uint8 *req, int req_len, int *resp_len)
{
	SSL_CTX    *ctx = NULL;
	SSL		   *ssl = NULL;
	int			fd = -1;
	struct addrinfo hints,
			   *res = NULL;
	char		port_str[16];
	uint8	   *resp_buf;
	int			resp_cap;
	int			total;
	int			n;
	uint8		hdr[8];
	uint32		body_len;

	if (!tde_kms_host || tde_kms_host[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("KMIP: tde_kms_host must be set")));

	ctx = kmip_build_ssl_ctx();

	snprintf(port_str, sizeof(port_str), "%d", tde_kms_port);

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	if (getaddrinfo(tde_kms_host, port_str, &hints, &res) != 0 || !res)
	{
		SSL_CTX_free(ctx);
		ereport(WARNING,
				(errmsg("KMIP: could not resolve host \"%s\"", tde_kms_host)));
		return NULL;
	}

	fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
	if (fd < 0 || connect(fd, res->ai_addr, res->ai_addrlen) < 0)
	{
		freeaddrinfo(res);
		SSL_CTX_free(ctx);
		if (fd >= 0) close(fd);
		ereport(WARNING,
				(errmsg("KMIP: could not connect to %s:%s: %m",
						tde_kms_host, port_str)));
		return NULL;
	}
	freeaddrinfo(res);

	/* Apply socket timeouts */
	if (tde_kms_operation_timeout > 0)
	{
		struct timeval tv;

		tv.tv_sec  = tde_kms_operation_timeout;
		tv.tv_usec = 0;
		setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
		setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
	}

	ssl = SSL_new(ctx);
	if (!ssl)
	{
		close(fd);
		SSL_CTX_free(ctx);
		ereport(WARNING, (errmsg("KMIP: SSL_new failed")));
		return NULL;
	}
	SSL_set_fd(ssl, fd);
	SSL_set_tlsext_host_name(ssl, tde_kms_host);

	if (SSL_connect(ssl) <= 0)
	{
		SSL_free(ssl);
		close(fd);
		SSL_CTX_free(ctx);
		ereport(WARNING,
				(errmsg("KMIP: TLS handshake failed: %s",
						ERR_error_string(ERR_get_error(), NULL))));
		return NULL;
	}

	/* Send request */
	if (SSL_write(ssl, req, req_len) != req_len)
	{
		SSL_free(ssl);
		close(fd);
		SSL_CTX_free(ctx);
		ereport(WARNING, (errmsg("KMIP: send failed")));
		return NULL;
	}

	/*
	 * Read response.  TTLV is self-delimiting: read the 8-byte header first
	 * to learn the body length, then read the rest.
	 */
	total = 0;
	while (total < 8)
	{
		n = SSL_read(ssl, hdr + total, 8 - total);
		if (n <= 0) break;
		total += n;
	}
	if (total < 8)
	{
		SSL_free(ssl);
		close(fd);
		SSL_CTX_free(ctx);
		ereport(WARNING, (errmsg("KMIP: short read on response header")));
		return NULL;
	}

	body_len = read_u32(hdr + 4);
	/*
	 * Compare as unsigned: casting body_len to int wraps to a negative value
	 * for body_len > INT_MAX, which would bypass this guard and lead to a
	 * short palloc and a heap overflow at the memcpy below.
	 */
	if (body_len > (uint32) KMIP_MAX_RESPONSE)
	{
		SSL_free(ssl);
		close(fd);
		SSL_CTX_free(ctx);
		ereport(WARNING,
				(errmsg("KMIP: response body too large (%u bytes)", body_len)));
		return NULL;
	}

	resp_cap = 8 + (int) body_len;
	resp_buf = palloc(resp_cap);
	memcpy(resp_buf, hdr, 8);
	total = 8;

	while (total < resp_cap)
	{
		n = SSL_read(ssl, resp_buf + total, resp_cap - total);
		if (n <= 0) break;
		total += n;
	}

	SSL_free(ssl);
	close(fd);
	SSL_CTX_free(ctx);

	if (total < resp_cap)
	{
		ereport(WARNING,
				(errmsg("KMIP: short read on response body (%d/%d bytes)",
						total, resp_cap)));
		pfree(resp_buf);
		return NULL;
	}

	*resp_len = total;
	return resp_buf;
}

/* ----------------------------------------------------------------
 * Build KMIP Request Message
 * ---------------------------------------------------------------- */

/*
 * Build a minimal KMIP Request Header (Protocol Version 2.1, BatchCount 1).
 */
static void
kmip_append_request_header(TtlvBuf *b)
{
	int			hdr_off = tbuf_begin_structure(b, KMIP_TAG_REQUEST_HEADER);
	int			pv_off  = tbuf_begin_structure(b, KMIP_TAG_PROTOCOL_VERSION);

	tbuf_put_integer(b, KMIP_TAG_PROTOCOL_VERSION_MAJOR, 2);
	tbuf_put_integer(b, KMIP_TAG_PROTOCOL_VERSION_MINOR, 1);
	tbuf_end_structure(b, pv_off);

	tbuf_put_integer(b, KMIP_TAG_BATCH_COUNT, 1);
	tbuf_end_structure(b, hdr_off);
}

/*
 * Append CryptographicParameters(AES-GCM) structure.
 * Required by KMIP 2.1 servers to indicate the algorithm for Encrypt/Decrypt.
 */
static void
kmip_append_crypto_params(TtlvBuf *b)
{
	int			cp_off = tbuf_begin_structure(b, KMIP_TAG_CRYPTOGRAPHIC_PARAMETERS);

	tbuf_put_enumeration(b, KMIP_TAG_BLOCK_CIPHER_MODE, KMIP_BLOCK_CIPHER_MODE_GCM);
	tbuf_put_enumeration(b, KMIP_TAG_CRYPTOGRAPHIC_ALGORITHM, KMIP_CRYPTO_ALGO_AES);
	tbuf_end_structure(b, cp_off);
}

/*
 * Build a KMIP Encrypt request:
 *   RequestMessage
 *     RequestHeader { ProtocolVersion 2.1, BatchCount 1 }
 *     BatchItem
 *       Operation: Encrypt
 *       RequestPayload
 *         UniqueIdentifier: key_id
 *         CryptographicParameters { BlockCipherMode=GCM, CryptographicAlgorithm=AES }
 *         Data: plaintext_dek
 */
static void
kmip_build_encrypt_request(TtlvBuf *b,
							const char *key_id,
							const uint8 *plaintext, int plaintext_len)
{
	int			msg_off  = tbuf_begin_structure(b, KMIP_TAG_REQUEST_MESSAGE);
	int			item_off;
	int			payload_off;

	kmip_append_request_header(b);

	item_off = tbuf_begin_structure(b, KMIP_TAG_BATCH_ITEM);
	tbuf_put_enumeration(b, KMIP_TAG_OPERATION, KMIP_OPERATION_ENCRYPT);

	payload_off = tbuf_begin_structure(b, KMIP_TAG_REQUEST_PAYLOAD);
	tbuf_put_textstring(b, KMIP_TAG_UNIQUE_IDENTIFIER, key_id, strlen(key_id));
	kmip_append_crypto_params(b);
	tbuf_put_bytestring(b, KMIP_TAG_DATA, plaintext, plaintext_len);
	tbuf_end_structure(b, payload_off);

	tbuf_end_structure(b, item_off);
	tbuf_end_structure(b, msg_off);
}

/*
 * Build a KMIP Decrypt request:
 *   RequestPayload
 *     UniqueIdentifier: key_id
 *     CryptographicParameters { BlockCipherMode=GCM, CryptographicAlgorithm=AES }
 *     Data: ciphertext
 *     IVCounterNonce: iv
 *     AuthenticatedEncryptionTag: auth_tag
 */
static void
kmip_build_decrypt_request(TtlvBuf *b,
							const char *key_id,
							const uint8 *ciphertext, int ct_len,
							const uint8 *iv,
							const uint8 *auth_tag)
{
	int			msg_off     = tbuf_begin_structure(b, KMIP_TAG_REQUEST_MESSAGE);
	int			item_off;
	int			payload_off;

	kmip_append_request_header(b);

	item_off = tbuf_begin_structure(b, KMIP_TAG_BATCH_ITEM);
	tbuf_put_enumeration(b, KMIP_TAG_OPERATION, KMIP_OPERATION_DECRYPT);

	payload_off = tbuf_begin_structure(b, KMIP_TAG_REQUEST_PAYLOAD);
	tbuf_put_textstring(b, KMIP_TAG_UNIQUE_IDENTIFIER, key_id, strlen(key_id));
	kmip_append_crypto_params(b);
	tbuf_put_bytestring(b, KMIP_TAG_DATA, ciphertext, ct_len);
	tbuf_put_bytestring(b, KMIP_TAG_IV_COUNTER_NONCE, iv, KMIP_IV_LEN);
	tbuf_put_bytestring(b, KMIP_TAG_AUTH_ENCRYPTION_TAG, auth_tag, KMIP_TAG_LEN);
	tbuf_end_structure(b, payload_off);

	tbuf_end_structure(b, item_off);
	tbuf_end_structure(b, msg_off);
}

/* ----------------------------------------------------------------
 * Check response result status
 * ---------------------------------------------------------------- */
static bool
kmip_check_result(const uint8 *resp, int resp_len, const char *op_name)
{
	uint32		status;
	char		msg[256];

	if (!ttlv_find_uint32(resp, resp_len, KMIP_TAG_RESULT_STATUS, &status))
	{
		ereport(WARNING,
				(errmsg("KMIP %s: result status tag not found in response",
						op_name)));
		return false;
	}

	if (status != KMIP_RESULT_STATUS_SUCCESS)
	{
		msg[0] = '\0';
		ttlv_find_textstring(resp, resp_len, KMIP_TAG_RESULT_MESSAGE,
							 msg, sizeof(msg));
		ereport(WARNING,
				(errmsg("KMIP %s failed (status=0x%08X): %s",
						op_name, status,
						msg[0] ? msg : "(no message)")));
		return false;
	}
	return true;
}

/* ----------------------------------------------------------------
 * KmsOps: wrap_dek / unwrap_dek
 * ---------------------------------------------------------------- */

static bool
kmip_wrap_dek(const uint8 *in_dek, int in_dek_len,
			  const char *kms_key_id,
			  uint8 *out_wrapped, int *out_wrapped_len)
{
	TtlvBuf		b;
	uint8	   *resp;
	int			resp_len;
	uint8		iv[KMIP_IV_LEN];
	uint8		auth_tag[KMIP_TAG_LEN];
	uint8		ct[TBLSPC_MAX_WRAPPED_DEK_LEN];
	int			iv_len  = 0;
	int			tag_len = 0;
	int			ct_len  = 0;

	/* Use tde_kms_default_key_id if no per-tablespace key_id given */
	if (!kms_key_id || kms_key_id[0] == '\0')
		kms_key_id = tde_kms_default_key_id;

	if (!kms_key_id || kms_key_id[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("KMIP: tde_kms_default_key_id must be set")));

	/* Build request */
	b.buf = palloc(512);
	b.cap = 512;
	b.len = 0;
	kmip_build_encrypt_request(&b, kms_key_id, in_dek, in_dek_len);

	resp = kmip_send_request(b.buf, b.len, &resp_len);
	/* b.buf embeds the plaintext DEK in the Encrypt request; scrub before free. */
	explicit_bzero(b.buf, b.len);
	pfree(b.buf);

	if (!resp)
		return false;

	if (!kmip_check_result(resp, resp_len, "Encrypt"))
	{
		pfree(resp);
		return false;
	}

	/* Extract ciphertext, IV, auth tag */
	if (!ttlv_find_bytestring(resp, resp_len, KMIP_TAG_DATA,
							  ct, &ct_len, sizeof(ct)) ||
		!ttlv_find_bytestring(resp, resp_len, KMIP_TAG_IV_COUNTER_NONCE,
							  iv, &iv_len, sizeof(iv)) ||
		!ttlv_find_bytestring(resp, resp_len, KMIP_TAG_AUTH_ENCRYPTION_TAG,
							  auth_tag, &tag_len, sizeof(auth_tag)))
	{
		pfree(resp);
		ereport(WARNING,
				(errmsg("KMIP Encrypt: missing Data/IV/AuthTag in response")));
		return false;
	}
	pfree(resp);

	if (iv_len != KMIP_IV_LEN || tag_len != KMIP_TAG_LEN)
	{
		ereport(WARNING,
				(errmsg("KMIP Encrypt: unexpected IV (%d) or AuthTag (%d) size",
						iv_len, tag_len)));
		return false;
	}

	if (KMIP_HDR_LEN + ct_len > TBLSPC_MAX_WRAPPED_DEK_LEN)
	{
		ereport(WARNING,
				(errmsg("KMIP Encrypt: wrapped DEK too large (%d bytes)",
						KMIP_HDR_LEN + ct_len)));
		return false;
	}

	/* Pack: [IV 12B][AuthTag 16B][Ciphertext] */
	memcpy(out_wrapped, iv, KMIP_IV_LEN);
	memcpy(out_wrapped + KMIP_IV_LEN, auth_tag, KMIP_TAG_LEN);
	memcpy(out_wrapped + KMIP_HDR_LEN, ct, ct_len);
	*out_wrapped_len = KMIP_HDR_LEN + ct_len;

	return true;
}

static bool
kmip_unwrap_dek(const uint8 *in_wrapped, int in_len,
				const char *kms_key_id,
				uint8 *out_dek, int *out_dek_len)
{
	const uint8 *iv;
	const uint8 *auth_tag;
	const uint8 *ct;
	int			 ct_len;
	TtlvBuf		 b;
	uint8		*resp;
	int			 resp_len;
	uint8		 pt[TBLSPC_MAX_WRAPPED_DEK_LEN];
	int			 pt_len = 0;

	if (in_len < KMIP_HDR_LEN + 1)
	{
		ereport(WARNING,
				(errmsg("KMIP Decrypt: wrapped blob too short (%d bytes)",
						in_len)));
		return false;
	}

	iv       = in_wrapped;
	auth_tag = in_wrapped + KMIP_IV_LEN;
	ct       = in_wrapped + KMIP_HDR_LEN;
	ct_len   = in_len - KMIP_HDR_LEN;

	if (!kms_key_id || kms_key_id[0] == '\0')
		kms_key_id = tde_kms_default_key_id;

	if (!kms_key_id || kms_key_id[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("KMIP: tde_kms_default_key_id must be set")));

	b.buf = palloc(512);
	b.cap = 512;
	b.len = 0;
	kmip_build_decrypt_request(&b, kms_key_id, ct, ct_len, iv, auth_tag);

	resp = kmip_send_request(b.buf, b.len, &resp_len);
	pfree(b.buf);

	if (!resp)
		return false;

	if (!kmip_check_result(resp, resp_len, "Decrypt"))
	{
		pfree(resp);
		return false;
	}

	if (!ttlv_find_bytestring(resp, resp_len, KMIP_TAG_DATA,
							  pt, &pt_len, sizeof(pt)))
	{
		pfree(resp);
		ereport(WARNING, (errmsg("KMIP Decrypt: Data tag not found in response")));
		return false;
	}
	/* resp embeds the plaintext DEK (the Data field); scrub before releasing. */
	explicit_bzero(resp, resp_len);
	pfree(resp);

	memcpy(out_dek, pt, pt_len);
	*out_dek_len = pt_len;
	/* pt is a stack copy of the plaintext DEK; wipe it before returning. */
	explicit_bzero(pt, sizeof(pt));
	return true;
}

/* ----------------------------------------------------------------
 * Public entry point
 * ---------------------------------------------------------------- */

static const KmsOps kmip_ops = {
	.wrap_dek   = kmip_wrap_dek,
	.unwrap_dek = kmip_unwrap_dek,
};

const KmsOps *
KmsKmipOps(void)
{
	return &kmip_ops;
}

#endif							/* USE_OPENSSL */
