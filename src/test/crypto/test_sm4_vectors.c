/*
 * test_sm4_vectors.c
 *
 * Verify our SM4 implementation against the GB/T 32907-2016 standard test
 * vectors and a basic SM4-CTR encrypt/decrypt symmetry test.
 *
 * Compile:
 *   gcc -I../../include -I../../backend/crypto \
 *       test_sm4_vectors.c \
 *       ../../backend/crypto/sm4_ofb.o \
 *       ../../backend/crypto/sm4_ctr.o \
 *       -lssl -lcrypto -o test_sm4_vectors
 *
 * Run:
 *   ./test_sm4_vectors
 */

#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>

/* ----------------------------------------------------------------
 * Minimal SM4 type definitions (mirrors sm4_ofb.h without postgres.h)
 * ---------------------------------------------------------------- */
#define SM4_BLOCK_SIZE    16
#define SM4_KEY_SCHEDULE  32

typedef struct SM4_KEY_st {
    uint32_t rk[SM4_KEY_SCHEDULE];
} SM4_KEY;

/* Forward declarations of our SM4 functions */
int  ossl_sm4_set_key(const uint8_t *key, SM4_KEY *ks);
void ossl_sm4_encrypt(const uint8_t *in, uint8_t *out, const SM4_KEY *ks);

void sm4_ctr_setkey(SM4_KEY *ks, const uint8_t *key);
void sm4_ctr_cipher(const SM4_KEY *ks, unsigned char *out,
                    const unsigned char *in, size_t len,
                    const uint8_t iv[SM4_BLOCK_SIZE]);

/* ----------------------------------------------------------------
 * Minimal postgres.h stubs
 * ---------------------------------------------------------------- */
#include <stdarg.h>
void *palloc(size_t sz) { return malloc(sz); }
void  pfree(void *p)    { free(p); }
void *repalloc(void *p, size_t sz) { return realloc(p, sz); }

/* pg_printf / pg_fprintf / pg_vfprintf used by postgres headers */
int pg_printf(const char *fmt, ...)  { va_list ap; va_start(ap,fmt); int r=vprintf(fmt,ap);  va_end(ap); return r; }
int pg_fprintf(FILE *f, const char *fmt, ...) { va_list ap; va_start(ap,fmt); int r=vfprintf(f,fmt,ap); va_end(ap); return r; }
int pg_vfprintf(FILE *f, const char *fmt, va_list ap) { return vfprintf(f,fmt,ap); }

/* ----------------------------------------------------------------
 * Helpers
 * ---------------------------------------------------------------- */

static void
hex_to_bytes(const char *hex, uint8_t *out, int len)
{
    for (int i = 0; i < len; i++)
    {
        unsigned int v;
        sscanf(hex + 2*i, "%02x", &v);
        out[i] = (uint8_t)v;
    }
}

static void
print_hex(const char *label, const uint8_t *buf, int len)
{
    printf("  %-12s: ", label);
    for (int i = 0; i < len; i++)
        printf("%02x", buf[i]);
    printf("\n");
}

static int
test_passed(const char *name, const uint8_t *got, const uint8_t *want, int len)
{
    if (memcmp(got, want, len) == 0)
    {
        printf("PASS  %s\n", name);
        return 1;
    }
    printf("FAIL  %s\n", name);
    print_hex("got ", got, len);
    print_hex("want", want, len);
    return 0;
}

/* ----------------------------------------------------------------
 * Test 1: GB/T 32907-2016 single-block ECB vector
 *
 *   Key:       0123456789abcdeffedcba9876543210
 *   Plaintext: 0123456789abcdeffedcba9876543210
 *   Expected:  681edf34d206965e86b3e94f536e4246
 * ---------------------------------------------------------------- */
static int
test_sm4_ecb_single(void)
{
    uint8_t key[16], pt[16], ct[16], want[16];
    SM4_KEY ks;

    hex_to_bytes("0123456789abcdeffedcba9876543210", key, 16);
    hex_to_bytes("0123456789abcdeffedcba9876543210", pt,  16);
    hex_to_bytes("681edf34d206965e86b3e94f536e4246", want, 16);

    ossl_sm4_set_key(key, &ks);
    ossl_sm4_encrypt(pt, ct, &ks);

    return test_passed("SM4-ECB single block (GB/T 32907-2016 §A.1)", ct, want, 16);
}

/* ----------------------------------------------------------------
 * Test 2: GB/T 32907-2016 iterated 1,000,000 times
 *
 *   Same key and plaintext; after 1,000,000 single-block encryptions
 *   (each time using previous output as next input):
 *   Expected:  595298c7c6fd271f0402f804c33d3f66
 * ---------------------------------------------------------------- */
static int
test_sm4_ecb_million(void)
{
    uint8_t key[16], buf[16], want[16];
    SM4_KEY ks;

    hex_to_bytes("0123456789abcdeffedcba9876543210", key, 16);
    hex_to_bytes("0123456789abcdeffedcba9876543210", buf, 16);
    hex_to_bytes("595298c7c6fd271f0402f804c33d3f66", want, 16);

    ossl_sm4_set_key(key, &ks);
    for (int i = 0; i < 1000000; i++)
        ossl_sm4_encrypt(buf, buf, &ks);

    return test_passed("SM4-ECB x1,000,000 (GB/T 32907-2016 §A.2)", buf, want, 16);
}

/* ----------------------------------------------------------------
 * Test 3: SM4-CTR encrypt/decrypt symmetry
 *
 *   Encrypt 48 bytes (3 full blocks), then decrypt; expect original.
 * ---------------------------------------------------------------- */
static int
test_sm4_ctr_symmetry(void)
{
    uint8_t key[16] = {
        0x01,0x23,0x45,0x67,0x89,0xab,0xcd,0xef,
        0xfe,0xdc,0xba,0x98,0x76,0x54,0x32,0x10
    };
    uint8_t iv[16] = {
        0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
        0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00
    };
    uint8_t plain[48], cipher[48], recover[48];
    SM4_KEY ks;

    for (int i = 0; i < 48; i++)
        plain[i] = (uint8_t)i;

    sm4_ctr_setkey(&ks, key);
    sm4_ctr_cipher(&ks, cipher, plain, 48, iv);

    /* Verify no plaintext leaked: at least one byte must differ */
    int differs = 0;
    for (int i = 0; i < 48; i++)
        if (cipher[i] != plain[i]) { differs = 1; break; }
    if (!differs)
    {
        printf("FAIL  SM4-CTR symmetry (ciphertext == plaintext, no encryption)\n");
        return 0;
    }

    sm4_ctr_cipher(&ks, recover, cipher, 48, iv);

    return test_passed("SM4-CTR encrypt→decrypt symmetry (48 bytes)", recover, plain, 48);
}

/* ----------------------------------------------------------------
 * Test 4: SM4-CTR partial block (17 bytes: 1 full + 1 partial)
 * ---------------------------------------------------------------- */
static int
test_sm4_ctr_partial(void)
{
    uint8_t key[16] = {
        0xfe,0xdc,0xba,0x98,0x76,0x54,0x32,0x10,
        0x01,0x23,0x45,0x67,0x89,0xab,0xcd,0xef
    };
    uint8_t iv[16] = {0};
    uint8_t plain[17], cipher[17], recover[17];
    SM4_KEY ks;

    for (int i = 0; i < 17; i++)
        plain[i] = (uint8_t)(0xA0 + i);

    sm4_ctr_setkey(&ks, key);
    sm4_ctr_cipher(&ks, cipher, plain, 17, iv);
    sm4_ctr_cipher(&ks, recover, cipher, 17, iv);

    return test_passed("SM4-CTR partial final block (17 bytes)", recover, plain, 17);
}

/* ----------------------------------------------------------------
 * Test 5: SM4-CTR position-independence
 *
 *   Encrypt bytes 0..47 in one call.
 *   Separately encrypt only bytes 16..31 (second block) starting at
 *   the block-aligned IV for block 1 (counter = 1).
 *   Both must produce identical ciphertext for that range.
 * ---------------------------------------------------------------- */
static int
test_sm4_ctr_position(void)
{
    uint8_t key[16] = {
        0x00,0x11,0x22,0x33,0x44,0x55,0x66,0x77,
        0x88,0x99,0xaa,0xbb,0xcc,0xdd,0xee,0xff
    };
    /* IV for block 0 */
    uint8_t iv0[16] = {0};
    /* IV for block 1: counter = 1 in big-endian */
    uint8_t iv1[16] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1};

    uint8_t plain[48];
    uint8_t cipher_full[48], cipher_block1[16];
    SM4_KEY ks;

    for (int i = 0; i < 48; i++)
        plain[i] = (uint8_t)i;

    sm4_ctr_setkey(&ks, key);

    /* Full 48-byte encryption from block 0 */
    sm4_ctr_cipher(&ks, cipher_full, plain, 48, iv0);

    /* Single block encryption starting at block 1 */
    sm4_ctr_cipher(&ks, cipher_block1, plain + 16, 16, iv1);

    return test_passed("SM4-CTR position-independence (block 1 matches)",
                       cipher_block1, cipher_full + 16, 16);
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */
int
main(void)
{
    int passed = 0, total = 0;

    printf("SM4 algorithm correctness tests\n");
    printf("================================\n");

    total++; passed += test_sm4_ecb_single();
    total++; passed += test_sm4_ecb_million();
    total++; passed += test_sm4_ctr_symmetry();
    total++; passed += test_sm4_ctr_partial();
    total++; passed += test_sm4_ctr_position();

    printf("================================\n");
    printf("Result: %d/%d passed\n", passed, total);

    return (passed == total) ? 0 : 1;
}
