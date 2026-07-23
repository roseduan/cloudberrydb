# Adding a New Storage Backend to datalake_fdw

Target audience: you want to add support for a new object store or file
system (Azure Blob, GCS, WebHDFS, MinIO-via-custom-auth, …) to the
open-source build of datalake_fdw.

## The whole procedure

Three files, three lines of registration glue. No central plumbing edits.

1. **`src/common/myFileSystem.h`** — class declaration inheriting
   `Datalake::Internal::FileSystem`.

2. **`src/common/myFileSystem.cpp`** — implement all 10 pure-virtual
   methods declared in `fileSystem.h`, plus override `getName()` to
   return your backend's short name (e.g., `"HDFS"`, `"Azure"`). At
   file scope (conventionally at the bottom), register:

   ```cpp
   DATALAKE_REGISTER_BACKEND("myproto", MyFileSystem);
   ```

   `"myproto"` is the string users will supply in
   `CREATE SERVER foo FOREIGN DATA WRAPPER datalake_fdw OPTIONS (protocol 'myproto', ...)`.

3. **`Makefile`** — in the non-Gopher branch of the storage backend
   block (around line 183), add:

   ```makefile
   OBJS += src/common/myFileSystem.o
   STORAGE_BACKEND_OBJS += src/common/myFileSystem.o
   ```

4. **`src/datalake_def.h` + `src/datalake_type.h`** — add your protocol
   name and the corresponding `DLProt` enum value. This is a **one-line
   allowlist** consulted by the option validator at `CREATE SERVER` time
   so unknown protocols fail fast at DDL rather than at query time. See
   existing entries for `s3`, `hdfs`, `ali`, etc.

5. **`src/datalake_option.c`** — if your protocol needs option
   validation beyond the allowlist (e.g., required auth fields), wire
   it into the existing validator. Most new backends just need the
   allowlist entry from step 4.

Steps 1–3 are the registry framework. Steps 4–5 are the existing
option-validation layer which predates the registry — it's defense in
depth (fail at DDL, not at query execution). Collapsing steps 4–5 into
the registry is tracked as a follow-up (spec §13 open questions).

Rebuild; your backend resolves at runtime.

## Build system contract (read this once)

Backend object files **must** be added directly to `OBJS` (and the
`STORAGE_BACKEND_OBJS` compile-rule list), not wrapped in a static
archive. The registration is a C++ static constructor, and linker DCE
will drop the whole TU from a `.a` if nothing references its symbols —
causing your backend to silently fail to register, with a "no storage
backend registered for protocol 'myproto'" error at query time.

If you must use an archive (rare), compile with
`-Wl,--whole-archive <archive> -Wl,--no-whole-archive`.

## FileSystem interface — what each method must do

See `src/common/fileSystem.h` for the full list. Brief notes:

- **`createHandle(void *storageOptions)`** — parse
  `(struct storageOptions*)` for your backend's config, open any
  persistent connection/client.
- **`openFile`, `read`, `write`, `seek`, `closeFile`** — POSIX-ish file
  I/O semantics; return values follow existing backends. Backends that
  cannot support a mode (e.g. random writes on append-only HDFS
  streams) throw `Datalake::Internal::Error` with a clear message.
- **`listInfo(path, &count, recursive, iswrite)`** — return a
  `palloc`'d array of `datalakeFileInfo`. Memory ownership contract is
  documented in `fileSystem.h`.
- **`getFileInfo(path)`** — `palloc`'d single entry.
- **`getUfsId()`** — returns a small integer identifying your backend
  family. `0` for S3/object-store, `9` for HDFS-family (the wrapper's
  `datalakeGetFileInfo` prepends `/` to HDFS paths). Pick a unique value
  for novel families.
- **`getName() const`** — return a short human-readable name (e.g.
  `"S3"`, `"HDFS"`, `"Azure"`) used in diagnostic messages. Not
  pure-virtual; default is `"Unknown"`. Always override — it costs one
  line and helps every future error report.
- **`destroyHandle()`** — release the persistent connection / client
  allocated in `createHandle`.

## Testing your backend locally

See [`testing-a-backend.md`](testing-a-backend.md) for a
copy-paste-runnable walkthrough: bring up a minimal HDFS + MinIO
stack, write the server config files dlagent expects, and run an
end-to-end SQL test (INSERT / SELECT / cross-backend JOIN).

## Minimum skeleton

The smallest working backend — a no-op that logs every call and returns
safe defaults. Copy, rename `MyFileSystem`/`"myproto"`, fill in the
method bodies for your storage.

**`src/common/myFileSystem.h`**:

```cpp
#ifndef DATALAKE_MY_FILESYSTEM_H
#define DATALAKE_MY_FILESYSTEM_H

#include "fileSystem.h"

namespace Datalake {
namespace Internal {

class MyFileSystem : public FileSystem {
public:
	MyFileSystem() = default;
	~MyFileSystem() override = default;

	int createHandle(void *storageOptions) override;
	int openFile(const char *path, int flag) override;
	int write(void *buff, int64_t size) override;
	int read(void *buff, int64_t size) override;
	int seek(int64_t position) override;
	int closeFile() override;
	int getUfsId() override;
	const char *getName() const override { return "MyBackend"; }
	datalakeFileInfo* listInfo(const char *path, int &count,
							   int recursive = 1, bool iswrite = false) override;
	datalakeFileInfo* getFileInfo(const char *path) override;
	int destroyHandle() override;
};

} /* namespace Internal */
} /* namespace Datalake */
#endif
```

**`src/common/myFileSystem.cpp`**:

```cpp
extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include "myFileSystem.h"
#include "backendRegistry.h"

namespace Datalake {
namespace Internal {

int MyFileSystem::createHandle(void *storageOptions)            { elog(LOG, "MyFileSystem: createHandle"); return 0; }
int MyFileSystem::openFile(const char *path, int flag)          { return 0; }
int MyFileSystem::write(void *buff, int64_t size)               { return 0; }
int MyFileSystem::read(void *buff, int64_t size)                { return 0; }
int MyFileSystem::seek(int64_t position)                        { return 0; }
int MyFileSystem::closeFile()                                   { return 0; }
int MyFileSystem::getUfsId()                                    { return 0; }
datalakeFileInfo* MyFileSystem::listInfo(const char *p, int &n, int, bool) { n = 0; return nullptr; }
datalakeFileInfo* MyFileSystem::getFileInfo(const char *p)      { return nullptr; }
int MyFileSystem::destroyHandle()                               { return 0; }

/*
 * Register INSIDE the namespace using the bare class name.
 * Writing `DATALAKE_REGISTER_BACKEND("myproto",
 * ::Datalake::Internal::MyFileSystem)` would break the macro's
 * `cls##_Registrar` token-concatenation — see Troubleshooting.
 */
DATALAKE_REGISTER_BACKEND("myproto", MyFileSystem);

} /* namespace Internal */
} /* namespace Datalake */
```

## Reference implementations — read these after the skeleton

Two real in-tree examples:

| File | What it demonstrates |
|---|---|
| `src/common/s3FileSystem.{h,cpp}` | Object storage via AWS SDK C++. Shows multi-alias registration (one class, 7 protocol names: `s3`/`ali`/`cos`/`qs`/`s3b`/`huawei`/`ks3`), multipart upload, fork-safe SDK init. |
| `src/common/hdfsFileSystem.{h,cpp}` | HDFS via libhdfs3. Shows HA config via `hdfsBuilderConfSetStr`, Kerberos auth, `dfs.client.use.datanode.hostname=true` for Docker networks, and rich error reporting via `hdfsGetLastError()` + `errno`. |

Patterns worth stealing from each:

- **S3's manual file-scope registrar struct** — use this when one class
  serves multiple protocol names (the single-name
  `DATALAKE_REGISTER_BACKEND` macro token-concatenates, so aliases need
  an explicit registrar).
- **HDFS's enriched error in `openFile`** — when libhdfs3/AWS/etc. return
  generic "failed" with an opaque errno, `throw Error("... : %s (errno=%d: %s)", hdfsGetLastError(), savedErrno, strerror(savedErrno))` saves a lot of debugging time.

## Troubleshooting

**`elog(ERROR): no storage backend registered for protocol 'X'; registered: Y, Z`**
— the registry didn't see your `DATALAKE_REGISTER_BACKEND` call. Most
likely cause: your `.o` was not linked into the final `.so` (see
"Build system contract" above). Confirm with `nm datalake_fdw.so | grep
_registrar_instance` — the symbol should be present for every backend.

**Duplicate-registration warning at server startup**
— two backends claimed the same protocol name. Rename one.

**`no template named 'function' in namespace 'std'` / `'override' keyword is a C++11 extension`**
— LSP false positives if your editor doesn't pick up `-std=c++17`. The
real build uses the toolchain's `g++ -std=c++17` (see Makefile) and
compiles cleanly. Ignore the editor diagnostics.

**`ld: cannot find src/common/myFileSystem.o: No such file or directory`**
— parallel-make race: PGXS snapshots `OBJS` at include time; your
`OBJS +=` after that isn't in the shared library's prereq list. The
Makefile has `datalake_fdw.so: $(STORAGE_BACKEND_OBJS)` as belt-and-
suspenders — make sure you added your `.o` to both `OBJS` and
`STORAGE_BACKEND_OBJS`.

**`DATALAKE_REGISTER_BACKEND("hdfs", ::Datalake::Internal::HdfsFileSystem)`
fails with `global qualification of class name is invalid before '{' token`**
— the macro token-concatenates `cls##_Registrar`; `::`-qualified names
don't form valid identifiers. Call the macro **inside** the
`Datalake::Internal` namespace block with the bare class name:

```cpp
namespace Datalake { namespace Internal {
    // ... class HdfsFileSystem { ... };
    DATALAKE_REGISTER_BACKEND("hdfs", HdfsFileSystem);
}}
```

**Segments crash with `storageOptions or protocol is NULL`**
— your backend is being dispatched correctly on the coordinator but
segments don't receive the protocol string. The `opt->gopher->protocol`
field must be populated in `datalake_option.c`'s option parser (commit
`a05bf1ad0c6` added this). New backends benefit for free.

**`CREATE SERVER ... OPTIONS (protocol 'myproto')` fails with
`invalid protocol "myproto"`**
— the option validator's allowlist in `datalake_def.h` /
`datalake_type.h` doesn't know your protocol. This fires *before* the
registry; see steps 4–5 in "The whole procedure" above.

**`Failed to initialize GopherFileIO` when trying to read Iceberg/Hudi**
— only relevant if your backend uses the Java `dlagent` for catalog
resolution. The agent defaults to Gopher mode; override at JVM startup
with `JAVA_TOOL_OPTIONS=-Dgopher.enabled=false` in the environment of
the process that spawns dlagent (e.g., before `gpstart`). Also set
`gopher.enabled: 'false'` in the server's `.conf` file (`s3.conf`,
`gphdfs.conf`, etc.).
