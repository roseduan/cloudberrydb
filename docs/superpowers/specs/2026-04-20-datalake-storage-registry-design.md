# datalake_fdw Storage Backend Registry — Design

**Author:** zhanaguohai (p_guohai.zhang@synxdata.com)
**Date:** 2026-04-20
**Status:** Draft
**Related branch:** `datalake_s3_sdk`
**Scope:** Framework only. Specific new backends (HDFS, Azure, GCS, …)
are separate specs that consume this framework.

---

## 1. Goal

Design the **smallest, cleanest registration framework** so that adding
a new storage backend to `datalake_fdw` is a self-contained, zero-touch
change to any existing file.

**Success criterion:** a new engineer can add a working backend by
producing exactly two artifacts — one `.cpp/.h` pair and one build-system
line — without editing `fileSystemWrapper.cpp`, the registry, or any
other backend. If they need to edit anything else, the framework has
failed.

This spec does **not** cover which backends to build. HDFS, Azure, etc.
are separate specs that use this framework.

## 2. Current State

The abstraction layer introduced in commit `026bfe5` has:

- `FileSystem` — pure-virtual C++ base class (10 methods). **Good, keep.**
- `fileSystemWrapper.{h,cpp}` — C API + try/catch + thread-local
  last-error. **Good, keep.**
- `datalake_storage.h` — storage-agnostic `datalakeFileInfo`. **Good, keep.**

The only weak spot is selection. Today `fileSystemWrapper.cpp` picks
the implementation with:

```cpp
#ifdef USE_GOPHER
    FileSystem *file = new GopherFileSystem();
#else
    FileSystem *file = new S3FileSystem();
#endif
```

The `#else` branch hard-codes one backend. This is what the framework
replaces.

Gopher (commercial, whole-replacement) stays on its `#ifdef` — it is
never in the same binary as the framework and is out of scope here.

## 3. The Contract

Three things, and only three things, make up the framework.

### 3.1 The backend interface — `FileSystem`

Already defined in `fileSystem.h`. Every backend is a C++ class
inheriting `Datalake::Internal::FileSystem` and implementing all 10
pure-virtual methods. Methods a backend cannot meaningfully support
throw `Datalake::Internal::Error` with a clear message.

**One small addition — `getName()`:**

```cpp
virtual const char *getName() const { return "Unknown"; }
```

Each backend reports a short human-readable name (`"S3"`, `"HDFS"`,
`"Gopher"`, `"Echo"`). Used in wrapper diagnostics — error messages
like `"S3: connection timeout"` beat bare `"connection timeout"` when
multiple backends coexist. Default implementation keeps the commercial
Gopher build path compiling without changes; non-Gopher backends
override.

Inspired by DuckDB's `FileSystem::GetName()` (see
`duckdb/src/include/duckdb/common/file_system.hpp`).

### 3.2 The registry — `BackendRegistry`

**New file:** `src/common/backendRegistry.{h,cpp}`.

```cpp
namespace Datalake {
namespace Internal {

class BackendRegistry {
public:
    using Factory = std::function<FileSystem*()>;

    // Meyers singleton — thread-safe under C++11.
    static BackendRegistry &instance();

    // Register under a protocol name. Backends may register the same
    // factory under multiple names (aliases — see §3.3).
    // Duplicate name with different factory is a programmer error:
    // asserts in debug, logs WARNING + last-wins in release.
    void registerBackend(const std::string &name, Factory factory);

    // Construct a backend. Throws Error with a useful "registered: …"
    // list on miss. Caller owns the returned pointer.
    FileSystem *create(const std::string &name) const;

    std::vector<std::string> listRegistered() const;

private:
    BackendRegistry() = default;
    std::unordered_map<std::string, Factory> backends_;
};

} // namespace Internal
} // namespace Datalake
```

The implementation is deliberately trivial: an `unordered_map` and a
lookup. No locking needed — all `registerBackend` calls happen during
static initialization (single-threaded by C++ rules); all `create` calls
happen on a single PG backend thread.

### 3.3 The registration macro — `DATALAKE_REGISTER_BACKEND`

```cpp
// Use at file scope in the backend's .cpp file. Anonymous namespace
// keeps symbols local to the TU; no collisions across backends.
#define DATALAKE_REGISTER_BACKEND(name, cls)                          \
    namespace {                                                       \
        struct cls##_Registrar {                                      \
            cls##_Registrar() {                                       \
                ::Datalake::Internal::BackendRegistry::instance()     \
                    .registerBackend(                                 \
                        (name),                                       \
                        []() -> ::Datalake::Internal::FileSystem* {   \
                            return new cls();                         \
                        });                                           \
            }                                                         \
        };                                                            \
        static cls##_Registrar cls##_registrar_instance;              \
    }
```

**Aliases.** A backend that responds to multiple protocol names (e.g.,
the S3-compatible OSS family) writes its own registrar struct at file
scope and loops the names. Keeping the macro single-use avoids
`__LINE__`-concatenation tricks and keeps each registrar's symbol name
grep-able in a debugger.

Example — a backend with aliases:

```cpp
// s3FileSystem.cpp, at file scope, outside the class
namespace {
    struct S3_Registrar {
        S3_Registrar() {
            auto factory = []() -> FileSystem* { return new S3FileSystem(); };
            for (const char *name : {"s3","ali","cos","qs","s3b","huawei","ks3"})
                BackendRegistry::instance().registerBackend(name, factory);
        }
    } s3_registrar_instance;
}
```

## 4. How the Registry Is Used — one edit, once

`fileSystemWrapper.cpp`'s `#else` branch is rewritten once:

```cpp
#ifdef USE_GOPHER
    FileSystem *file = new GopherFileSystem();
#else
    struct storageOptions *opts =
        static_cast<struct storageOptions *>(storageOptions);
    FileSystem *file = BackendRegistry::instance().create(opts->protocol);
#endif
```

No enum↔string helper needed: `storageOptions.protocol` is already a
`char*` populated directly from the `CREATE SERVER ... OPTIONS
(protocol '...')` DDL clause (see `datalake_option.c:584`). The
registry key is exactly the string the user typed.

After this one edit, `fileSystemWrapper.cpp` is closed for modification.
No new backend ever needs to touch it.

## 5. How to Add a New Backend — the developer-facing recipe

This is the real deliverable of the framework. The complete procedure:

1. **Write `myFileSystem.h`**: class declaration inheriting
   `Datalake::Internal::FileSystem`.
2. **Write `myFileSystem.cpp`**: implement the 10 virtual methods plus
   `getName()`. At file scope (usually at the bottom), add one line:
   ```cpp
   DATALAKE_REGISTER_BACKEND("myproto", MyFileSystem);
   ```
3. **In the Makefile** (non-Gopher branch): `OBJS +=
   src/common/myFileSystem.o` and `STORAGE_BACKEND_OBJS +=
   src/common/myFileSystem.o`.

Done. Rebuild and the backend resolves at runtime under `CREATE
SERVER ... OPTIONS (protocol 'myproto')`.

## 6. Build System Contract — why the registry actually works

Self-registering plugins in C++ have one classic failure mode: the
linker's discarded-code elimination (DCE). If a backend lives in a
static archive (`.a`) and nothing else references its symbols, the
linker drops the whole TU including the registrar — and the backend
silently fails to register at runtime.

**The contract:** backends must be added to `MODULE_big`'s `OBJS`
directly, not to an intermediate archive. PGXS links `OBJS` unconditionally;
the registrar's static constructor is guaranteed to run at library load.

This is exactly how `s3FileSystem.o` ships today — the rule is
pre-existing, backends simply follow it. If a future change ever needs
to split backends into a `.a`, `-Wl,--whole-archive` on that archive is
the escape hatch. Out of scope now.

## 7. Moving S3 Into the Registry — proof of the framework

Part of this change: migrate the existing `S3FileSystem` from
compile-time selection to registry-driven selection. The migration is
the first validation that the framework works end-to-end.

Changes required:

- `s3FileSystem.cpp`: append the `S3_Registrar` block shown in §3.3.
  No method body changes.
- `fileSystemWrapper.cpp`: the one-time edit from §4.
- `Makefile`: `OBJS += src/common/backendRegistry.o`. (S3 is already
  in OBJS.)

Expected behavior: identical to today. The Iceberg regression set
validated on `026bfe5` re-runs with byte-identical results.

## 8. Deliberate Simplicity — things the framework does not do

Each of these is a real design choice, not an oversight.

| Not doing | Rationale |
|---|---|
| Thread-safe `registerBackend` | All registrations run during static init (single-threaded). Adding a mutex buys nothing. |
| Per-backend priority / ordering | Protocol name is unique; first match wins. No ordering question exists. |
| Dynamic `dlopen` of third-party backends | Meaningful cost (ABI stability, symbol visibility) for a demand nobody has voiced yet. Revisit if external contributors request. |
| Plugin metadata (version, author, capabilities) | Backends are in-tree C++ code reviewed by humans. Metadata is noise. |
| Backend hot-reload | Not a real requirement. |
| DuckDB-style `CanHandleFile(path)` dispatch (runtime polling by path prefix) | PG FDW declares protocol explicitly in DDL (`CREATE SERVER ... OPTIONS (protocol 'X')`) — exact-match string key on `storageOptions->protocol` is both simpler and faster. If a future requirement needs path-level dispatch (e.g., URI-scheme routing inside a single protocol), add a `virtual bool canHandleFile(const char*)` default-false method as a secondary match pass; do not rearchitect. |

## 9. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Static-init DCE drops a registrar, causing runtime "unknown protocol" | Direct `OBJS` linkage, not `.a`. §6. Plus: the `create()` error message lists all registered names, so a missing registration is diagnosed in one look. |
| Duplicate name registered with different factories | Assert in debug builds; WARNING + last-wins in release. Grep-friendly `cls##_Registrar` symbol name makes it easy to spot the offending TU. |

## 10. Validation

The framework is considered validated when **both** are true:

1. **Regression parity.** The existing Iceberg regression set (10
   queries, already validated on `026bfe5`) runs with byte-identical
   results after S3 is migrated into the registry.

2. **Dogfood test.** A no-op `EchoFileSystem` that logs every call can
   be added by producing only: `echoFileSystem.h`, `echoFileSystem.cpp`
   with a single `DATALAKE_REGISTER_BACKEND("echo", EchoFileSystem)`,
   and one Makefile line. `CREATE SERVER foo … OPTIONS (protocol
   'echo')` then works — with zero edits to the wrapper, the registry,
   or any other backend file.

The dogfood test is the framework's acceptance criterion: if it
requires editing anything else, the framework needs more work.

## 11. Out of Scope

- Any specific new backend (HDFS, Azure, GCS, …). Each gets its own
  spec that lists the backend's libraries, auth model, fork-safety
  audit, and test plan.
- Commercial Gopher build path.
- Running Gopher and registry-driven backends in the same binary.
