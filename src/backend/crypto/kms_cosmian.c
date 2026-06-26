/*-------------------------------------------------------------------------
 *
 * kms_cosmian.c
 *	  Cosmian KMS provider for tablespace-level TDE.
 *
 * Calls Cosmian KMS via its KMIP 2.1 REST API over HTTP(S).  The KEK
 * never leaves the KMS: Cosmian encrypts/decrypts the DEK server-side
 * using AES-256-GCM with a symmetric key identified by kms_key_id.
 *
 * Required GUCs:
 *   tde_kms_host          Cosmian server hostname or IP
 *   tde_kms_port          Port (default 9998)
 *
 * Optional GUCs:
 *   tde_kms_ca_cert       Path to PEM CA certificate (TLS mode)
 *   tde_kms_client_cert + tde_kms_client_key   mTLS
 *   tde_kms_password      Bearer token (no username) or password (with username)
 *   tde_kms_username      HTTP Basic auth username
 *   tde_kms_operation_timeout  seconds (applied via SO_RCVTIMEO/SO_SNDTIMEO)
 *
 * Wrapped DEK on-disk layout (60 bytes for AES-256 DEK):
 *   [IV 12B][AuthTag 16B][Ciphertext N B]
 * where N == plaintext DEK length (32 for AES-256).
 *
 * Cosmian KMIP 2.1 JSON format (verified against v5.22.0):
 *   Endpoint  : POST /kmip/2_1
 *   Request   : {"tag":"Encrypt","value":[{"tag":"UniqueIdentifier",...},
 *                                         {"tag":"Data",...}]}
 *   Response  : {"tag":"EncryptResponse","value":[..., "Data", "IVCounterNonce",
 *                                                  "AuthenticatedEncryptionTag"]}
 *   Decrypt   : same fields plus IVCounterNonce + AuthenticatedEncryptionTag in request
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *	  src/backend/crypto/kms_cosmian.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef USE_OPENSSL

#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>

#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/bio.h>

#include "common/base64.h"
#include "common/cipher.h"
#include "common/kmgr_utils.h"
#include "crypto/tblspc_kmgr.h"
#include "kms_client.h"
#include "lib/stringinfo.h"
#include "utils/palloc.h"

/* Maximum HTTP response body accepted (128 KiB) */
#define COSMIAN_MAX_RESPONSE	(128 * 1024)

/*
 * Wrapped DEK layout stored in .wkey:
 *   offset 0  : IV           (COSMIAN_IV_LEN bytes)
 *   offset 12 : AuthTag      (COSMIAN_TAG_LEN bytes)
 *   offset 28 : Ciphertext   (dek_len bytes, same as plaintext length)
 */
#define COSMIAN_IV_LEN		12
#define COSMIAN_TAG_LEN		16
#define COSMIAN_HDR_LEN		(COSMIAN_IV_LEN + COSMIAN_TAG_LEN)	/* 28 */

/* ----------------------------------------------------------------
 * Tiny hex helpers
 * ---------------------------------------------------------------- */

static void
bin_to_hex_upper(const uint8 *src, int len, char *dst)
{
	static const char hex[] = "0123456789ABCDEF";
	int		i;

	for (i = 0; i < len; i++)
	{
		dst[i * 2]     = hex[(src[i] >> 4) & 0xF];
		dst[i * 2 + 1] = hex[src[i] & 0xF];
	}
	dst[len * 2] = '\0';
}

/* Returns number of bytes decoded, or -1 on error */
static int
hex_to_bin(const char *src, uint8 *dst, int max_dst)
{
	int		slen = strlen(src);
	int		i;

	if (slen % 2 != 0 || slen / 2 > max_dst)
		return -1;
	for (i = 0; i < slen; i += 2)
	{
		unsigned int b;

		if (sscanf(src + i, "%02x", &b) != 1 &&
			sscanf(src + i, "%02X", &b) != 1)
			return -1;
		dst[i / 2] = (uint8) b;
	}
	return slen / 2;
}

/* ----------------------------------------------------------------
 * SSL context
 * ---------------------------------------------------------------- */

static SSL_CTX *
cosmian_build_ssl_ctx(bool require_tls)
{
	SSL_CTX *ctx;

	ctx = SSL_CTX_new(TLS_client_method());
	if (!ctx)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Cosmian KMS: SSL_CTX_new failed: %s",
						ERR_error_string(ERR_get_error(), NULL))));

	SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);

	if (tde_kms_ca_cert && tde_kms_ca_cert[0] != '\0')
	{
		if (SSL_CTX_load_verify_locations(ctx, tde_kms_ca_cert, NULL) != 1)
		{
			SSL_CTX_free(ctx);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Cosmian KMS: could not load CA cert \"%s\": %s",
							tde_kms_ca_cert,
							ERR_error_string(ERR_get_error(), NULL))));
		}
	}
	else if (require_tls)
	{
		SSL_CTX_free(ctx);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Cosmian KMS: tde_kms_ca_cert must be set for TLS connections")));
	}
	else
	{
		/* Plain HTTP mode: disable certificate verification */
		SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
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
					 errmsg("Cosmian KMS: could not load mTLS client cert/key: %s",
							ERR_error_string(ERR_get_error(), NULL))));
		}
	}

	return ctx;
}

/* ----------------------------------------------------------------
 * HTTP Authorization header
 * ---------------------------------------------------------------- */

static void
cosmian_append_auth_header(StringInfo s)
{
	bool has_user = (tde_kms_username && tde_kms_username[0] != '\0');
	bool has_pass = (tde_kms_password && tde_kms_password[0] != '\0');

	if (!has_pass)
		return;

	if (!has_user)
	{
		appendStringInfo(s, "Authorization: Bearer %s\r\n", tde_kms_password);
	}
	else
	{
		char   *cred = psprintf("%s:%s", tde_kms_username, tde_kms_password);
		int		cred_len = strlen(cred);
		int		enc_len = pg_b64_enc_len(cred_len);
		char   *enc = palloc(enc_len + 1);

		enc_len = pg_b64_encode(cred, cred_len, enc, enc_len);
		enc[enc_len] = '\0';
		appendStringInfo(s, "Authorization: Basic %s\r\n", enc);
		pfree(cred);
		pfree(enc);
	}
}

/* ----------------------------------------------------------------
 * HTTP/1.1 POST over plain TCP or TLS via OpenSSL BIO
 * ---------------------------------------------------------------- */

/*
 * Send an HTTP POST to Cosmian's /kmip/2_1 endpoint and return the
 * response body as a palloc'd NUL-terminated string.
 * Returns NULL and emits WARNING on failure.
 */
/*
 * cosmian_write_all — write the whole buffer to a blocking socket.
 *
 * write(2) on a TCP socket may transfer fewer bytes than requested without
 * reporting an error.  A naive "< 0" check treats such a short write as
 * success and ships a truncated HTTP request, so loop until every byte is
 * sent.  Returns true on success, false on error (errno is set).
 */
static bool
cosmian_write_all(int fd, const char *buf, size_t len)
{
	size_t		off = 0;

	while (off < len)
	{
		ssize_t		n = write(fd, buf + off, len - off);

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			return false;
		}
		off += (size_t) n;
	}
	return true;
}

static char *
cosmian_http_post(const char *json_body)
{
	bool		use_tls;
	SSL_CTX	   *ctx = NULL;
	BIO		   *bio = NULL;
	SSL		   *ssl = NULL;
	char		host_port[256];
	char		port_str[16];
	StringInfoData req;
	int			body_len;
	char	   *resp_buf;
	int			resp_cap;
	int			resp_len;
	int			n;
	int			status_code = 0;
	char	   *body_start;
	char	   *result;

	if (!tde_kms_host || tde_kms_host[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Cosmian KMS: tde_kms_host must be set")));

	use_tls = (tde_kms_ca_cert && tde_kms_ca_cert[0] != '\0') ||
			  (tde_kms_client_cert && tde_kms_client_cert[0] != '\0');

	snprintf(port_str, sizeof(port_str), "%d", tde_kms_port);
	snprintf(host_port, sizeof(host_port), "%s:%s", tde_kms_host, port_str);

	ctx = cosmian_build_ssl_ctx(false /* non-fatal if no CA cert */);

	bio = BIO_new_ssl_connect(ctx);
	if (!bio)
	{
		SSL_CTX_free(ctx);
		ereport(WARNING,
				(errmsg("Cosmian KMS: BIO_new_ssl_connect failed")));
		return NULL;
	}

	BIO_get_ssl(bio, &ssl);
	if (ssl && use_tls)
	{
		SSL_set_tlsext_host_name(ssl, tde_kms_host);
		SSL_set_min_proto_version(ssl, TLS1_2_VERSION);
	}
	else if (ssl && !use_tls)
	{
		/* Plain HTTP: tell BIO not to do TLS handshake */
		SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
		SSL_set_connect_state(ssl);
	}

	BIO_set_conn_hostname(bio, host_port);

	if (BIO_do_connect(bio) <= 0)
	{
		unsigned long err = ERR_get_error();

		BIO_free_all(bio);
		SSL_CTX_free(ctx);

		/*
		 * For plain HTTP, BIO_do_connect may fail with SSL errors because
		 * we set up an SSL BIO but the server speaks plain HTTP.  Try a
		 * plain TCP BIO instead.
		 */
		if (!use_tls)
		{
			char	   *host_copy = pstrdup(tde_kms_host);
			int			fd;
			struct addrinfo hints;
			struct addrinfo *res = NULL;

			memset(&hints, 0, sizeof(hints));
			hints.ai_family   = AF_UNSPEC;
			hints.ai_socktype = SOCK_STREAM;

			if (getaddrinfo(host_copy, port_str, &hints, &res) != 0 || !res)
			{
				pfree(host_copy);
				ereport(WARNING,
						(errmsg("Cosmian KMS: could not resolve host \"%s\"",
								tde_kms_host)));
				return NULL;
			}

			fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
			if (fd < 0 || connect(fd, res->ai_addr, res->ai_addrlen) < 0)
			{
				freeaddrinfo(res);
				pfree(host_copy);
				if (fd >= 0) close(fd);
				ereport(WARNING,
						(errmsg("Cosmian KMS: could not connect to %s: %m",
								host_port)));
				return NULL;
			}
			freeaddrinfo(res);
			pfree(host_copy);

			/* Apply timeouts */
			if (tde_kms_operation_timeout > 0)
			{
				struct timeval tv;

				tv.tv_sec  = tde_kms_operation_timeout;
				tv.tv_usec = 0;
				setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
				setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
			}

			/* Build and send HTTP request on raw socket */
			initStringInfo(&req);
			body_len = strlen(json_body);
			appendStringInfo(&req,
				"POST /kmip/2_1 HTTP/1.1\r\n"
				"Host: %s\r\n"
				"Content-Type: application/json\r\n"
				"Content-Length: %d\r\n"
				"Connection: close\r\n",
				host_port, body_len);
			cosmian_append_auth_header(&req);
			appendStringInfoString(&req, "\r\n");

			if (!cosmian_write_all(fd, req.data, req.len) ||
				!cosmian_write_all(fd, json_body, body_len))
			{
				close(fd);
				pfree(req.data);
				ereport(WARNING,
						(errmsg("Cosmian KMS: send failed: %m")));
				return NULL;
			}
			pfree(req.data);

			/* Read response */
			resp_cap = 8192;
			resp_buf = palloc(resp_cap + 1);
			resp_len = 0;
			while (resp_len < COSMIAN_MAX_RESPONSE)
			{
				if (resp_len >= resp_cap)
				{
					resp_cap = Min(resp_cap * 2, COSMIAN_MAX_RESPONSE);
					resp_buf = repalloc(resp_buf, resp_cap + 1);
				}
				n = read(fd, resp_buf + resp_len, resp_cap - resp_len);
				if (n <= 0) break;
				resp_len += n;
			}
			close(fd);
			resp_buf[resp_len] = '\0';
			goto parse_response;
		}

		ereport(WARNING,
				(errmsg("Cosmian KMS: could not connect to %s: %s",
						host_port,
						ERR_error_string(err, NULL))));
		return NULL;
	}

	if (use_tls && BIO_do_handshake(bio) <= 0)
	{
		BIO_free_all(bio);
		SSL_CTX_free(ctx);
		ereport(WARNING,
				(errmsg("Cosmian KMS: TLS handshake failed: %s",
						ERR_error_string(ERR_get_error(), NULL))));
		return NULL;
	}

	/* Apply operation timeout to connected socket */
	if (tde_kms_operation_timeout > 0)
	{
		int fd = -1;

		BIO_get_fd(bio, &fd);
		if (fd >= 0)
		{
			struct timeval tv;

			tv.tv_sec  = tde_kms_operation_timeout;
			tv.tv_usec = 0;
			setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
			setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
		}
	}

	/* Build and send HTTP request over TLS BIO */
	initStringInfo(&req);
	body_len = strlen(json_body);
	appendStringInfo(&req,
		"POST /kmip/2_1 HTTP/1.1\r\n"
		"Host: %s\r\n"
		"Content-Type: application/json\r\n"
		"Content-Length: %d\r\n"
		"Connection: close\r\n",
		host_port, body_len);
	cosmian_append_auth_header(&req);
	appendStringInfoString(&req, "\r\n");

	if (BIO_write(bio, req.data, req.len) != req.len ||
		BIO_write(bio, json_body, body_len) != body_len)
	{
		BIO_free_all(bio);
		SSL_CTX_free(ctx);
		pfree(req.data);
		ereport(WARNING,
				(errmsg("Cosmian KMS: could not send request to %s", host_port)));
		return NULL;
	}
	pfree(req.data);

	/* Read response */
	resp_cap = 8192;
	resp_buf = palloc(resp_cap + 1);
	resp_len = 0;
	while (resp_len < COSMIAN_MAX_RESPONSE)
	{
		if (resp_len >= resp_cap)
		{
			resp_cap = Min(resp_cap * 2, COSMIAN_MAX_RESPONSE);
			resp_buf = repalloc(resp_buf, resp_cap + 1);
		}
		n = BIO_read(bio, resp_buf + resp_len, resp_cap - resp_len);
		if (n <= 0) break;
		resp_len += n;
	}
	BIO_free_all(bio);
	SSL_CTX_free(ctx);
	resp_buf[resp_len] = '\0';

parse_response:
	/* Parse HTTP status */
	if (sscanf(resp_buf, "HTTP/%*d.%*d %d", &status_code) != 1 ||
		status_code != 200)
	{
		ereport(WARNING,
				(errmsg("Cosmian KMS: HTTP %d from %s", status_code, host_port)));
		pfree(resp_buf);
		return NULL;
	}

	/* Find body after blank line */
	body_start = strstr(resp_buf, "\r\n\r\n");
	if (!body_start)
		body_start = strstr(resp_buf, "\n\n");
	if (!body_start)
	{
		pfree(resp_buf);
		ereport(WARNING,
				(errmsg("Cosmian KMS: malformed HTTP response from %s",
						host_port)));
		return NULL;
	}
	/*
	 * body_start lands on the first byte of the separator.  For a CRLF
	 * response ("\r\n\r\n") it points at '\r' and we must skip 4 bytes; for a
	 * bare-LF response ("\n\n") it points at '\n' and we skip 2.  Branch on the
	 * byte actually pointed at, not body_start[1].
	 */
	body_start += (body_start[0] == '\r') ? 4 : 2;

	result = pstrdup(body_start);
	pfree(resp_buf);
	return result;
}

/* ----------------------------------------------------------------
 * JSON field extraction (minimal, no full parser)
 * ---------------------------------------------------------------- */

/*
 * Extract the hex value of the first occurrence of {"tag":"<tag_name>",
 * ..., "value":"<hex>"} in json.
 * Returns palloc'd hex string, or NULL if not found.
 */
static char *
extract_hex_field(const char *json, const char *tag_name)
{
	char		search[64];
	const char *p;
	const char *start;
	const char *end;
	int			len;

	snprintf(search, sizeof(search), "\"tag\":\"%s\"", tag_name);
	p = strstr(json, search);
	if (!p)
		return NULL;

	p = strstr(p, "\"value\":\"");
	if (!p)
		return NULL;
	p += strlen("\"value\":\"");

	start = p;
	end   = strchr(start, '"');
	if (!end)
		return NULL;

	len = (int) (end - start);
	if (len <= 0)
		return NULL;

	return pnstrdup(start, len);
}

/* ----------------------------------------------------------------
 * KmsOps: wrap_dek
 * ---------------------------------------------------------------- */

static bool
cosmian_wrap_dek(const uint8 *in_dek,
				 int in_dek_len,
				 const char *kms_key_id,
				 uint8 *out_wrapped,
				 int *out_wrapped_len)
{
	char	   *dek_hex;
	char	   *req_json;
	char	   *resp;
	char	   *data_hex;
	char	   *iv_hex;
	char	   *tag_hex;
	uint8		iv_bin[COSMIAN_IV_LEN];
	uint8		tag_bin[COSMIAN_TAG_LEN];
	uint8		ct_bin[KMGR_MAX_KEY_LEN_BYTES];
	int			iv_len;
	int			tag_len;
	int			ct_len;

	if (!kms_key_id || kms_key_id[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Cosmian KMS: kms_key_id must be specified in CREATE TABLESPACE "
						"or via tde_kms_default_key_id")));

	/* Convert DEK to uppercase hex */
	dek_hex = palloc(in_dek_len * 2 + 1);
	bin_to_hex_upper(in_dek, in_dek_len, dek_hex);

	req_json = psprintf(
		"{\"tag\":\"Encrypt\",\"value\":["
		"{\"tag\":\"UniqueIdentifier\",\"type\":\"TextString\",\"value\":\"%s\"},"
		"{\"tag\":\"Data\",\"type\":\"ByteString\",\"value\":\"%s\"}"
		"]}",
		kms_key_id, dek_hex);
	/* dek_hex is the plaintext DEK in hex; scrub it before returning to the pool. */
	explicit_bzero(dek_hex, (size_t) in_dek_len * 2 + 1);
	pfree(dek_hex);

	resp = cosmian_http_post(req_json);
	/* req_json embeds the same plaintext DEK hex; scrub it too. */
	explicit_bzero(req_json, strlen(req_json));
	pfree(req_json);

	if (!resp)
		return false;

	/* Extract Data, IVCounterNonce, AuthenticatedEncryptionTag */
	data_hex = extract_hex_field(resp, "Data");
	iv_hex   = extract_hex_field(resp, "IVCounterNonce");
	tag_hex  = extract_hex_field(resp, "AuthenticatedEncryptionTag");
	pfree(resp);

	if (!data_hex || !iv_hex || !tag_hex)
	{
		ereport(WARNING,
				(errmsg("Cosmian KMS: Encrypt response missing Data/IV/AuthTag fields")));
		return false;
	}

	iv_len  = hex_to_bin(iv_hex,   iv_bin,  sizeof(iv_bin));
	tag_len = hex_to_bin(tag_hex,  tag_bin, sizeof(tag_bin));
	ct_len  = hex_to_bin(data_hex, ct_bin,  sizeof(ct_bin));

	pfree(data_hex);
	pfree(iv_hex);
	pfree(tag_hex);

	if (iv_len != COSMIAN_IV_LEN || tag_len != COSMIAN_TAG_LEN || ct_len <= 0)
	{
		ereport(WARNING,
				(errmsg("Cosmian KMS: unexpected field lengths: iv=%d tag=%d ct=%d",
						iv_len, tag_len, ct_len)));
		return false;
	}

	if (COSMIAN_HDR_LEN + ct_len > TBLSPC_MAX_WRAPPED_DEK_LEN)
	{
		ereport(WARNING,
				(errmsg("Cosmian KMS: wrapped DEK too large (%d bytes)",
						COSMIAN_HDR_LEN + ct_len)));
		return false;
	}

	/* Layout: [IV 12B][AuthTag 16B][Ciphertext] */
	memcpy(out_wrapped,                    iv_bin,  COSMIAN_IV_LEN);
	memcpy(out_wrapped + COSMIAN_IV_LEN,   tag_bin, COSMIAN_TAG_LEN);
	memcpy(out_wrapped + COSMIAN_HDR_LEN,  ct_bin,  ct_len);
	*out_wrapped_len = COSMIAN_HDR_LEN + ct_len;

	ereport(DEBUG1,
			(errmsg("Cosmian KMS: wrapped DEK (%d bytes) for key %s",
					*out_wrapped_len, kms_key_id)));

	return true;
}

/* ----------------------------------------------------------------
 * KmsOps: unwrap_dek
 * ---------------------------------------------------------------- */

static bool
cosmian_unwrap_dek(const uint8 *in_wrapped,
				   int in_len,
				   const char *kms_key_id,
				   uint8 *out_dek,
				   int *out_dek_len)
{
	int			ct_len;
	char		iv_hex[(COSMIAN_IV_LEN * 2) + 1];
	char		tag_hex[(COSMIAN_TAG_LEN * 2) + 1];
	char	   *ct_hex;
	char	   *req_json;
	char	   *resp;
	char	   *data_hex;
	int			dek_len;

	if (!kms_key_id || kms_key_id[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Cosmian KMS: kms_key_id must be specified")));

	if (in_len <= COSMIAN_HDR_LEN)
	{
		ereport(WARNING,
				(errmsg("Cosmian KMS: wrapped DEK too short (%d bytes)", in_len)));
		return false;
	}

	ct_len = in_len - COSMIAN_HDR_LEN;

	/* Unpack layout: [IV 12B][AuthTag 16B][Ciphertext] */
	bin_to_hex_upper(in_wrapped,                   COSMIAN_IV_LEN,  iv_hex);
	bin_to_hex_upper(in_wrapped + COSMIAN_IV_LEN,  COSMIAN_TAG_LEN, tag_hex);
	ct_hex = palloc(ct_len * 2 + 1);
	bin_to_hex_upper(in_wrapped + COSMIAN_HDR_LEN, ct_len,          ct_hex);

	req_json = psprintf(
		"{\"tag\":\"Decrypt\",\"value\":["
		"{\"tag\":\"UniqueIdentifier\",\"type\":\"TextString\",\"value\":\"%s\"},"
		"{\"tag\":\"Data\",\"type\":\"ByteString\",\"value\":\"%s\"},"
		"{\"tag\":\"IVCounterNonce\",\"type\":\"ByteString\",\"value\":\"%s\"},"
		"{\"tag\":\"AuthenticatedEncryptionTag\",\"type\":\"ByteString\",\"value\":\"%s\"}"
		"]}",
		kms_key_id, ct_hex, iv_hex, tag_hex);
	pfree(ct_hex);

	resp = cosmian_http_post(req_json);
	pfree(req_json);

	if (!resp)
		return false;

	data_hex = extract_hex_field(resp, "Data");
	pfree(resp);

	if (!data_hex)
	{
		ereport(WARNING,
				(errmsg("Cosmian KMS: Decrypt response missing Data field")));
		return false;
	}

	dek_len = hex_to_bin(data_hex, out_dek, KMGR_MAX_KEY_LEN_BYTES);
	/* data_hex is the plaintext DEK in hex; scrub it before returning to the pool. */
	explicit_bzero(data_hex, strlen(data_hex));
	pfree(data_hex);

	if (dek_len <= 0)
	{
		ereport(WARNING,
				(errmsg("Cosmian KMS: could not decode Decrypt response Data")));
		return false;
	}

	*out_dek_len = dek_len;

	ereport(DEBUG1,
			(errmsg("Cosmian KMS: unwrapped DEK (%d bytes) for key %s",
					dek_len, kms_key_id)));

	return true;
}

/* KmsOps vtable */
static const KmsOps cosmian_ops = {
	.wrap_dek	= cosmian_wrap_dek,
	.unwrap_dek = cosmian_unwrap_dek,
};

const KmsOps *
KmsCosmianOps(void)
{
	return &cosmian_ops;
}

#else  /* !USE_OPENSSL */

#include "common/cipher.h"
#include "common/kmgr_utils.h"
#include "crypto/tblspc_kmgr.h"
#include "kms_client.h"

static bool
cosmian_wrap_nossl(const uint8 *in_dek, int in_dek_len,
				   const char *kms_key_id,
				   uint8 *out_wrapped, int *out_wrapped_len)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("tde_kms_provider = 'cosmian' requires OpenSSL support")));
	return false;
}

static bool
cosmian_unwrap_nossl(const uint8 *in_wrapped, int in_len,
					 const char *kms_key_id,
					 uint8 *out_dek, int *out_dek_len)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("tde_kms_provider = 'cosmian' requires OpenSSL support")));
	return false;
}

static const KmsOps cosmian_ops = {
	.wrap_dek	= cosmian_wrap_nossl,
	.unwrap_dek = cosmian_unwrap_nossl,
};

const KmsOps *
KmsCosmianOps(void)
{
	return &cosmian_ops;
}

#endif  /* USE_OPENSSL */
