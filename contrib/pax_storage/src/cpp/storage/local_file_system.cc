/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * local_file_system.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/local_file_system.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/local_file_system.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <iostream>

#include "access/pax_access_handle.h"
#include "comm/cbdb_wrappers.h"
#include "comm/fast_io.h"
#include "comm/fmt.h"
#include "comm/guc.h"
#include "comm/pax_memory.h"
#include "comm/pax_resource.h"
#include "exceptions/CException.h"

/* Postgres cipher/TDE headers */
extern "C" {
#include "common/cipher.h"
#include "common/kmgr_utils.h"
#include "crypto/sm4_ctr.h"
#include "crypto/tblspc_enc.h"
#include "crypto/tblspc_kmgr.h"
}

/*
 * PaxFileId — derive a stable 32-bit per-file identifier from the file path
 * using FNV-1a.  The path embeds the tablespace, database, and relfilenode, so
 * the hash distinguishes every micro-partition file from every other one, even
 * across databases.  It is folded into the IV nonce (see PaxBuildIvFixed) so
 * that two files never share an AES/SM4 keystream at the same offset.
 */
static uint32
PaxFileId(const std::string &path)
{
    uint32 h = 2166136261u;              /* FNV offset basis */
    for (unsigned char c : path)
    {
        h ^= (uint32) c;
        h *= 16777619u;                  /* FNV prime */
    }
    return h;
}

/*
 * PaxCryptAtOffsetFixed — AES-CTR encrypt/decrypt with a big-endian block
 * counter so our per-offset IV matches OpenSSL's auto-increment behavior.
 *
 * This uses the same big-endian counter as build_pax_iv() in tblspc_enc.c
 * and is called directly from LocalFile::PRead /
 * LocalFile::PWrite instead of EncryptPaxAtOffsetForSpc /
 * DecryptPaxAtOffsetForSpc.
 *
 * The IV nonce (bytes 0-7) combines spcOid with a per-file identifier so that
 * distinct files never produce an identical keystream at the same offset
 * (which would be a two-time pad).  dbNode is intentionally not encoded
 * separately: file_id is derived from the full path, which already includes
 * the database directory.
 */
static void
PaxBuildIvFixed(uint8 iv[PG_AES_IV_SIZE], Oid spcOid, uint32 file_id,
                uint64_t block_counter)
{
    /* spcOid in little-endian */
    iv[0] = (uint8)(spcOid);
    iv[1] = (uint8)(spcOid >> 8);
    iv[2] = (uint8)(spcOid >> 16);
    iv[3] = (uint8)(spcOid >> 24);
    /* per-file identifier in little-endian */
    iv[4] = (uint8)(file_id);
    iv[5] = (uint8)(file_id >> 8);
    iv[6] = (uint8)(file_id >> 16);
    iv[7] = (uint8)(file_id >> 24);
    /* block_counter in big-endian (matches OpenSSL CTR 128-bit BE counter) */
    iv[8]  = (uint8)(block_counter >> 56);
    iv[9]  = (uint8)(block_counter >> 48);
    iv[10] = (uint8)(block_counter >> 40);
    iv[11] = (uint8)(block_counter >> 32);
    iv[12] = (uint8)(block_counter >> 24);
    iv[13] = (uint8)(block_counter >> 16);
    iv[14] = (uint8)(block_counter >> 8);
    iv[15] = (uint8)(block_counter);
}

/*
 * PaxCryptAtOffsetFixed — thread-safe variant that accepts cached key material
 * directly instead of calling TblspcGetEncEntry().
 *
 * This function may be called from std::async background prefetch threads
 * (see OrcReader::PrefetchGroup).  PostgreSQL's LWLock, palloc, and ereport
 * infrastructure all require the main process thread, so this function:
 *   - receives enc_method / dek / dek_len from the caller (cached at open time
 *     on the main thread — see LocalFile constructor),
 *   - uses malloc/free instead of palloc/pfree for the alignment-pad buffer,
 *   - throws a cbdb::CException on failure rather than calling ereport().
 */
static void
PaxCryptAtOffsetFixed(Oid spcOid, uint32 file_id,
                      uint8 enc_method, const uint8 *dek, int dek_len,
                      unsigned char *data, size_t len, off_t file_offset,
                      bool encrypt)
{
    uint8           iv[PG_AES_IV_SIZE];
    uint64_t        aligned_offset;
    size_t          skip;
    int             outlen;
    int             pg_cipher_id;
    bool            ok;

    if (len == 0)
        return;

    aligned_offset = (uint64_t)file_offset & ~(uint64_t)(PG_AES_IV_SIZE - 1);
    skip = (size_t)((uint64_t)file_offset - aligned_offset);

    PaxBuildIvFixed(iv, spcOid, file_id,
                    aligned_offset / (uint64_t)PG_AES_IV_SIZE);

    if (enc_method == TBLSPC_ENC_SM4)
    {
        /*
         * SM4-CTR path: set up a one-shot key schedule and call our
         * software SM4-CTR implementation.  The "prepend zeros" trick
         * works identically to AES-CTR because CTR keystream blocks are
         * independent of each other.
         *
         * Use malloc/free (not palloc/pfree) so this is safe to call from
         * background threads.
         */
        SM4_KEY ks;
        sm4_ctr_setkey(&ks, dek);

        if (skip == 0)
        {
            sm4_ctr_cipher(&ks, data, (const unsigned char *) data, len, iv);
        }
        else
        {
            size_t total = skip + len;
            unsigned char *tmp = (unsigned char *) malloc(total);
            if (!tmp)
            {
                /* Wipe the DEK-derived key schedule before unwinding. */
                explicit_bzero(&ks, sizeof(ks));
                CBDB_RAISE(cbdb::CException::ExType::kExTypeOOM,
                           pax::fmt("out of memory allocating %zu bytes for PAX "
                                    "SM4 alignment pad", total));
            }
            memset(tmp, 0, skip);
            memcpy(tmp + skip, data, len);
            sm4_ctr_cipher(&ks, tmp, (const unsigned char *) tmp, total, iv);
            memcpy(data, tmp + skip, len);
            free(tmp);
        }
        /*
         * ks holds the SM4 round-key schedule derived from the DEK; wipe it
         * before the stack frame is released.  (The AES-CTR path below relies
         * on pg_cipher_ctx_free() to scrub its EVP context.)
         */
        explicit_bzero(&ks, sizeof(ks));
        return;
    }

    /* AES-CTR path */
    switch (enc_method) {
        case TBLSPC_ENC_AES128:
        case TBLSPC_ENC_AES192:
        case TBLSPC_ENC_AES256:
            pg_cipher_id = PG_CIPHER_AES_CTR;
            break;
        default:
            /*
             * The tablespace is known to be encrypted (the caller only reaches
             * here after TblspcGetEncEntry returned a non-NULL entry), so an
             * unrecognised method must never silently skip the cipher: doing so
             * would write plaintext on PWrite or hand raw ciphertext to the
             * caller on PRead.  Fail loudly instead.  (CBDB_RAISE, not ereport,
             * because this may run on a background prefetch thread.)
             */
            CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError,
                       pax::fmt("unsupported PAX encryption method %d for "
                                "tablespace oid=%u",
                                (int) enc_method, spcOid));
    }

    /* Create a one-shot context for this operation */
    PgCipherCtx *ctx = pg_cipher_ctx_create(pg_cipher_id,
                                             const_cast<uint8 *>(dek),
                                             dek_len,
                                             encrypt ? true : false);
    if (!ctx)
        CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError,
                   pax::fmt("could not create cipher context for PAX %s",
                            encrypt ? "encryption" : "decryption"));

    if (skip == 0) {
        if (encrypt)
            ok = pg_cipher_encrypt(ctx, pg_cipher_id,
                                   data, (int)len, data, &outlen,
                                   iv, PG_AES_IV_SIZE, NULL, 0);
        else
            ok = pg_cipher_decrypt(ctx, pg_cipher_id,
                                   data, (int)len, data, &outlen,
                                   iv, PG_AES_IV_SIZE, NULL, 0);
    } else {
        size_t total = skip + len;
        unsigned char *tmp = (unsigned char *) malloc(total);
        if (!tmp) {
            pg_cipher_ctx_free(ctx);
            CBDB_RAISE(cbdb::CException::ExType::kExTypeOOM,
                       pax::fmt("out of memory allocating %zu bytes for PAX "
                                "AES alignment pad", total));
        }
        memset(tmp, 0, skip);
        memcpy(tmp + skip, data, len);

        if (encrypt)
            ok = pg_cipher_encrypt(ctx, pg_cipher_id,
                                   tmp, (int)total, tmp, &outlen,
                                   iv, PG_AES_IV_SIZE, NULL, 0);
        else
            ok = pg_cipher_decrypt(ctx, pg_cipher_id,
                                   tmp, (int)total, tmp, &outlen,
                                   iv, PG_AES_IV_SIZE, NULL, 0);

        if (ok)
            memcpy(data, tmp + skip, len);
        free(tmp);
    }

    pg_cipher_ctx_free(ctx);
    if (!ok)
        CBDB_RAISE(cbdb::CException::ExType::kExTypeLogicError,
                   pax::fmt("PAX block cipher %s failed at offset %lu",
                            encrypt ? "encryption" : "decryption",
                            (unsigned long) file_offset));
}

namespace pax {

struct DIRCloser {
  void operator()(DIR *d) const noexcept {
    if (d) closedir(d);
  }
};

class LocalFile final : public File {

 public:
  LocalFile(int fd, const std::string &file_path,
            unsigned int spc_oid, unsigned int db_node);
  virtual ~LocalFile();

  ssize_t Read(void *ptr, size_t n) const override;
  ssize_t Write(const void *ptr, size_t n) override;
  ssize_t PWrite(const void *ptr, size_t n, off_t offset) override;
  ssize_t PRead(void *ptr, size_t n, off_t offset) const override;
  void ReadBatch(const std::vector<IORequest> &requests) const override;
  size_t FileLength() const override;
  void Flush() override;
  void Delete() override;
  void Close() override;
  std::string GetPath() const override;
  std::string DebugString() const override;

 private:
  int fd_ = -1;
  std::string file_path_;
  unsigned int spc_oid_ = 0;
  unsigned int db_node_ = 0;
  /*
   * Encryption state cached at open time on the main thread.
   *
   * PAX uses std::async background threads for ORC prefetch (OrcReader), and
   * those threads call LocalFile::PRead / ReadBatch.  PostgreSQL's LWLock and
   * error-reporting infrastructure requires the main process thread, so we must
   * not call TblspcEncryptionEnabled() / TblspcGetEncEntry() from a background
   * thread.  Instead we snapshot the necessary encryption data here, while
   * still running on the main thread, and use only these cached values in
   * PRead / PWrite / ReadBatch.
   */
  bool    enc_enabled_ = false;
  uint8_t enc_method_  = 0;
  int     enc_dek_len_ = 0;
  uint8_t enc_dek_[KMGR_MAX_KEY_LEN_BYTES] = {};
  /* Per-file IV distinguisher derived from file_path_ (see PaxBuildIvFixed). */
  uint32_t file_id_ = 0;
};

LocalFile::LocalFile(int fd, const std::string &file_path,
                     unsigned int spc_oid, unsigned int db_node)
    : File(), fd_(fd), file_path_(file_path),
      spc_oid_(spc_oid), db_node_(db_node) {
  Assert(fd_ >= 0);

  /* Snapshot encryption parameters on the main thread while it is safe to
   * call TblspcGetEncEntry (which acquires an LWLock). */
  if (spc_oid != 0) {
    SpcEncEntry *entry = TblspcGetEncEntry(spc_oid);
    if (entry) {
      enc_enabled_ = true;
      enc_method_  = entry->enc_method;
      enc_dek_len_ = entry->dek_len;
      memcpy(enc_dek_, entry->dek, (size_t) entry->dek_len);
      /* Fold dbNode into the path hash for an extra cross-database guarantee. */
      file_id_ = PaxFileId(file_path_) ^ (uint32_t) db_node_;
    }
  }
}
LocalFile::~LocalFile() {
  /*
   * Wipe the cached DEK so key material does not linger in the freed heap
   * region (exposable via a core dump, /proc/self/mem, or cold-boot attack in
   * a long-running postmaster).
   */
  if (enc_enabled_)
    explicit_bzero(enc_dek_, sizeof(enc_dek_));
  if (fd_ >= 0) {
    int rc;
    do {
      rc = close(fd_);
    } while (unlikely(rc == -1 && errno == EINTR));
    fd_ = -1;
  }
}

ssize_t LocalFile::Read(void *ptr, size_t n) const {
  ssize_t num;

  do {
    num = read(fd_, ptr, n);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(num >= 0, cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to read [require=%lu, rc=%ld, errno=%d], %s", n, num,
                 errno, DebugString().c_str()));
  return num;
}

ssize_t LocalFile::Write(const void *ptr, size_t n) {
  ssize_t num;

  do {
    num = write(fd_, ptr, n);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(num >= 0, cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to write [require=%lu, rc=%ld, errno=%d], %s", n, num,
                 errno, DebugString().c_str()));
  return num;
}

ssize_t LocalFile::PRead(void *ptr, size_t n, off_t offset) const {
  ssize_t num;

  do {
    num = pread(fd_, ptr, n, offset);
  } while (unlikely(num == -1 && errno == EINTR));

  CBDB_CHECK(
      num >= 0, cbdb::CException::ExType::kExTypeIOError,
      fmt("Fail to pread [offset=%ld, require=%lu, rc=%ld, errno=%d], %s",
          offset, n, num, errno, DebugString().c_str()));

  if (num > 0 && enc_enabled_) {
    PaxCryptAtOffsetFixed(spc_oid_, file_id_,
                          enc_method_, enc_dek_, enc_dek_len_,
                          static_cast<unsigned char *>(ptr),
                          static_cast<size_t>(num),
                          offset, false /* decrypt */);
  }
  return num;
}

ssize_t LocalFile::PWrite(const void *ptr, size_t n, off_t offset) {
  ssize_t num;

  if (n > 0 && enc_enabled_) {
    /* Encrypt a copy; PWrite takes const void* so we need our own buffer. */
    auto *enc_buf = static_cast<unsigned char *>(malloc(n));
    if (!enc_buf)
      CBDB_RAISE(cbdb::CException::ExType::kExTypeOOM,
                 pax::fmt("out of memory allocating %zu bytes for PAX PWrite "
                          "encrypt buffer", n));
    memcpy(enc_buf, ptr, n);
    /*
     * PaxCryptAtOffsetFixed can throw a CException (OOM / logic error); guard
     * enc_buf so it is freed on the exception path instead of being leaked.
     */
    try {
      PaxCryptAtOffsetFixed(spc_oid_, file_id_,
                            enc_method_, enc_dek_, enc_dek_len_,
                            enc_buf, n, offset, true /* encrypt */);
    } catch (...) {
      free(enc_buf);
      throw;
    }

    do {
      num = pwrite(fd_, enc_buf, n, offset);
    } while (unlikely(num == -1 && errno == EINTR));

    free(enc_buf);
  } else {
    do {
      num = pwrite(fd_, ptr, n, offset);
    } while (unlikely(num == -1 && errno == EINTR));
  }

  CBDB_CHECK(
      num >= 0, cbdb::CException::ExType::kExTypeIOError,
      fmt("Fail to pwrite [offset=%ld, require=%lu, rc=%ld, errno=%d], %s",
          offset, n, num, errno, DebugString().c_str()));
  return num;
}

void LocalFile::ReadBatch(const std::vector<IORequest> &requests) const {
  if (unlikely(requests.empty())) return;

  if (pax::pax_enable_iouring && IOUringFastIO::available()) {
    IOUringFastIO fast_io(requests.size());
    std::vector<bool> result(requests.size(), false);
    auto res = fast_io.read(fd_, const_cast<std::vector<IORequest>&>(requests), result);
    CBDB_CHECK(res.first == 0, cbdb::CException::ExType::kExTypeIOError,
               fmt("Fail to ReadBatch with io_uring [successful=%d, total=%lu], %s",
                   res.second, requests.size(), DebugString().c_str()));
  } else {
    SyncFastIO fast_io;
    std::vector<bool> result(requests.size(), false);
    auto res = fast_io.read(fd_, const_cast<std::vector<IORequest>&>(requests), result);
    CBDB_CHECK(res.first == 0, cbdb::CException::ExType::kExTypeIOError,
               fmt("Fail to ReadBatch with sync read [successful=%d, total=%lu], %s",
                   res.second, requests.size(), DebugString().c_str()));
  }

  /* Decrypt each buffer after reading */
  if (enc_enabled_) {
    for (const auto &req : requests) {
      if (req.size > 0)
        PaxCryptAtOffsetFixed(spc_oid_, file_id_,
                              enc_method_, enc_dek_, enc_dek_len_,
                              static_cast<unsigned char *>(req.buffer),
                              req.size, req.offset, false /* decrypt */);
    }
  }
}

size_t LocalFile::FileLength() const {
  struct stat file_stat {};
  int rc;
  rc = fstat(fd_, &file_stat);

  CBDB_CHECK(rc == 0, cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to fstat [rc=%d, errno=%d], %s", rc, errno,
                 DebugString().c_str()));
  return static_cast<size_t>(file_stat.st_size);
}

void LocalFile::Flush() {
  int rc;
  rc = fsync(fd_);

  CBDB_CHECK(rc == 0, cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to fsync [rc=%d, errno=%d], %s", rc, errno,
                 DebugString().c_str()));
}

void LocalFile::Delete() {
  int rc;
  rc = remove(file_path_.c_str());
  CBDB_CHECK(rc == 0 || errno == ENOENT,
             cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to remove [rc=%d, errno=%d], %s", rc, errno,
                 DebugString().c_str()));
}

void LocalFile::Close() {
  int rc;

  do {
    rc = close(fd_);
  } while (unlikely(rc == -1 && errno == EINTR));
  CBDB_CHECK(rc == 0, cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to close [rc=%d, errno=%d], %s", rc, errno,
                 DebugString().c_str()));

  fd_ = -1;
}

std::string LocalFile::GetPath() const { return file_path_; }

std::string LocalFile::DebugString() const {
  return fmt("LOCAL file [path=%s]", file_path_.c_str());
}

std::unique_ptr<File> LocalFileSystem::Open(const std::string &file_path, int flags,
                            const std::shared_ptr<FileSystemOptions> &options) {
  int fd;
  unsigned int spc_oid = 0;
  unsigned int db_node = 0;

  if (options) {
    auto *local_opts =
        dynamic_cast<const LocalFileSystemOptions *>(options.get());
    if (local_opts) {
      spc_oid = local_opts->spc_oid;
      db_node = local_opts->db_node;
    }
  }

  if (flags & O_CREAT) {
    fd = open(file_path.c_str(), flags, fs::kDefaultWritePerm);
  } else {
    fd = open(file_path.c_str(), flags);
  }

  CBDB_CHECK(fd >= 0, cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to open [rc=%d, errno=%d, path=%s, flags=%d]", fd, errno,
                 file_path.c_str(), flags));

  return std::make_unique<LocalFile>(fd, file_path, spc_oid, db_node);
}

void LocalFileSystem::Delete(const std::string &file_path,
                             const std::shared_ptr<FileSystemOptions> & /*options*/) const {
  int rc;

  rc = remove(file_path.c_str());
  CBDB_CHECK(rc == 0 || errno == ENOENT,
             cbdb::CException::ExType::kExTypeIOError,
             fmt("Fail to remove [rc=%d, errno=%d, path=%s]", rc, errno,
                 file_path.c_str()));
}

std::string LocalFileSystem::BuildPath(const File *file) const {
  return file->GetPath();
}

int LocalFileSystem::CopyFile(const File *src_file, File *dst_file) {
  const size_t buf_size = 32 * 1024;
  char buf[buf_size];
  off_t read_off = 0;
  ssize_t num_write = 0;
  ssize_t num_read = 0;

  while ((num_read = src_file->PRead(buf, buf_size, read_off)) > 0) {
    read_off += num_read;
    num_write = dst_file->Write(buf, num_read);
    CBDB_CHECK(
        num_write == num_read, cbdb::CException::kExTypeIOError,
        fmt("Fail to copy LOCAL file from %s to %s. \n"
            "Write failed [read off=%ld, require=%ld, written=%ld, errno=%d]",
            src_file->DebugString().c_str(), dst_file->DebugString().c_str(),
            read_off, num_read, num_write, errno));
  }

  // no need check num_read >= 0 again
  // already checked in `src_file->Read`

  return 0;
}

std::vector<std::string> LocalFileSystem::ListDirectory(
    const std::string &path, const std::shared_ptr<FileSystemOptions> & /*options*/) const {
  DIR *dir;
  std::vector<std::string> filelist;
  const char *filepath = path.c_str();

  Assert(filepath != NULL && filepath[0] != '\0');

  dir = opendir(filepath);
  CBDB_CHECK(dir, cbdb::CException::ExType::kExTypeFileOperationError,
             fmt("Fail to opendir [path=%s, errno=%d]", filepath, errno));

  std::unique_ptr<DIR, DIRCloser> dir_guard(dir);

  try {
    struct dirent *direntry;
    while ((direntry = readdir(dir)) != NULL) {
      char *filename = &direntry->d_name[0];
      // skip to add '.' or '..' direntry for file enumerating under folder on
      // linux OS.
      if (*filename == '.' &&
          (!strcmp(filename, ".") || !strcmp(filename, "..")))
        continue;
      filelist.push_back(std::string(filename));
    }
  } catch (std::exception &ex) {
    CBDB_RAISE(cbdb::CException::ExType::kExTypeFileOperationError,
               fmt("List directory failed. [path=%s] %s", path.c_str(), ex.what()));
  }

  return filelist;
}

int LocalFileSystem::CreateDirectory(const std::string &path,
                                     const std::shared_ptr<FileSystemOptions> &options) const {
  return cbdb::PathNameCreateDir(path.c_str());
}

void LocalFileSystem::DeleteDirectory(const std::string &path,
                                      bool delete_topleveldir,
                                      const std::shared_ptr<FileSystemOptions> &options) const {
  cbdb::PathNameDeleteDir(path.c_str(), delete_topleveldir);
}

}  // namespace pax
