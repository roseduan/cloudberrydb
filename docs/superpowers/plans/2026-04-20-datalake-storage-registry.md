# datalake_fdw Storage Backend Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Post-implementation note (2026-04-22):**
> The plan below specifies shipping `docs/examples/echoFileSystem.{h,cpp}`
> as a standalone reference backend. During verification we found that
> un-CI'd reference files drift (the shipped example tripped its own
> Troubleshooting section by using a `::`-qualified class name in
> `DATALAKE_REGISTER_BACKEND`, which breaks the macro's
> `cls##_Registrar` token-concatenation). Final decision: the no-op
> skeleton is **inlined into `contrib/datalake_fdw/docs/adding-a-backend.md`
> under "Minimum skeleton"** so it lives next to the prose it illustrates
> and is updated together with the doc. The separate `.h`/`.cpp` files
> are not shipped. References below to `echoFileSystem.{h,cpp}` should
> be read as "the inlined skeleton in adding-a-backend.md".

**Goal:** Replace the single-backend `#else new S3FileSystem()` site in
`datalake_fdw` with a runtime registry so any number of storage backends
can be added by writing one `.cpp/.h` pair and one Makefile line — no
central plumbing edits.

**Architecture:** A single C++ singleton (`BackendRegistry`) keyed by
protocol string, populated at program start by static-init registrars in
each backend's own translation unit. `fileSystemWrapper.cpp`'s non-Gopher
branch performs one lookup. The commercial Gopher build path is untouched.

**Tech Stack:** C++ (C++11 static local for Meyers singleton), PostgreSQL
PGXS build system, existing AWS SDK C++ S3 client.

**Spec:** `docs/superpowers/specs/2026-04-20-datalake-storage-registry-design.md`

**Working branch:** `datalake_s3_sdk` (already checked out).

---

## File Structure

**Create:**
- `contrib/datalake_fdw/src/common/backendRegistry.h` — registry class declaration + `DATALAKE_REGISTER_BACKEND` macro.
- `contrib/datalake_fdw/src/common/backendRegistry.cpp` — singleton impl (~40 lines).
- `contrib/datalake_fdw/docs/adding-a-backend.md` — §5 recipe from spec + troubleshooting.
- `contrib/datalake_fdw/docs/examples/echoFileSystem.h` — reference example backend (not compiled).
- `contrib/datalake_fdw/docs/examples/echoFileSystem.cpp` — reference example backend (not compiled).

**Modify:**
- `contrib/datalake_fdw/src/common/fileSystem.h` — add one non-pure virtual method `getName()` with `"Unknown"` default (spec §3.1 addition, DuckDB-inspired).
- `contrib/datalake_fdw/src/common/s3FileSystem.{h,cpp}` — declare + implement `getName()` returning `"S3"`; append file-scope `S3_Registrar` block in .cpp.
- `contrib/datalake_fdw/src/common/fileSystemWrapper.cpp` — rewire `#else` branch (lines ~98-113) to `BackendRegistry::instance().create(...)`.
- `contrib/datalake_fdw/Makefile` — add `backendRegistry.o` to non-Gopher `OBJS` and `STORAGE_BACKEND_OBJS` compile rule.

**Not modified:**
- `datalake_storage.h` — storage-agnostic types already correct.
- `gopherFileSystem.{h,cpp}` — commercial build inherits the default `getName()` returning `"Unknown"` until/unless a follow-up overrides it; no source edit required.
- `datalake_type.h` / `datalake_def.h` — `DLProt` enum untouched; we dispatch on the existing `storageOptions.protocol` (`char*`) field populated at line 584 of `datalake_option.c`.

---

## Task 1: Create BackendRegistry header

**Files:**
- Create: `contrib/datalake_fdw/src/common/backendRegistry.h`

- [ ] **Step 1: Write the header**

Create `contrib/datalake_fdw/src/common/backendRegistry.h` with the exact content below:

```cpp
/*-------------------------------------------------------------------------
 *
 * backendRegistry.h
 *    Runtime registry of FileSystem backend factories.
 *
 *    Each backend's .cpp file invokes DATALAKE_REGISTER_BACKEND at file
 *    scope, associating a protocol name (the string used in
 *    CREATE SERVER ... OPTIONS (protocol '...')) with a factory that
 *    constructs a new FileSystem instance.
 *
 *    Selection happens at runtime inside fileSystemWrapper.cpp's
 *    non-Gopher branch: create(options->protocol) → FileSystem*.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/backendRegistry.h
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_BACKEND_REGISTRY_H
#define DATALAKE_BACKEND_REGISTRY_H

#include "fileSystem.h"
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

namespace Datalake {
namespace Internal {

class BackendRegistry {
public:
	using Factory = std::function<FileSystem*()>;

	/*
	 * Meyers singleton. Thread-safe under C++11, but thread-safety is
	 * not required here: registrations happen during static init
	 * (single-threaded per C++ rules) and create() runs on one PG
	 * backend thread per query.
	 */
	static BackendRegistry &instance();

	/*
	 * Associate a protocol name with a factory. A backend may register
	 * the same factory under multiple names (aliases — see S3 family).
	 * Duplicate names with *different* factories is a programmer error;
	 * we overwrite and log a WARNING.
	 */
	void registerBackend(const std::string &name, Factory factory);

	/*
	 * Construct a new FileSystem for the given protocol name. Throws
	 * Datalake::Internal::Error with a human-readable "registered: ..."
	 * list on miss. Caller owns the returned pointer.
	 */
	FileSystem *create(const std::string &name) const;

	/* For diagnostics — included in create()'s miss-error message. */
	std::vector<std::string> listRegistered() const;

private:
	BackendRegistry() = default;
	BackendRegistry(const BackendRegistry &) = delete;
	BackendRegistry &operator=(const BackendRegistry &) = delete;

	std::unordered_map<std::string, Factory> backends_;
};

} /* namespace Internal */
} /* namespace Datalake */

/*
 * DATALAKE_REGISTER_BACKEND(name, cls)
 *
 * Use at FILE SCOPE in a backend's .cpp file. Example:
 *
 *     DATALAKE_REGISTER_BACKEND("myproto", MyFileSystem);
 *
 * The anonymous namespace keeps the registrar symbol local to the TU.
 * Use only once per file; for multiple aliases, write a file-scope
 * registrar struct that loops (see s3FileSystem.cpp).
 *
 * IMPORTANT: The TU containing this macro must be linked directly into
 * MODULE_big's OBJS. Packing into a .a archive will cause the linker's
 * DCE to discard the registrar at static init time. See
 * docs/adding-a-backend.md §"Build system contract".
 */
#define DATALAKE_REGISTER_BACKEND(name, cls)                             \
	namespace {                                                          \
		struct cls##_Registrar {                                         \
			cls##_Registrar() {                                          \
				::Datalake::Internal::BackendRegistry::instance()        \
					.registerBackend(                                    \
						(name),                                          \
						[]() -> ::Datalake::Internal::FileSystem* {      \
							return new cls();                            \
						});                                              \
			}                                                            \
		};                                                               \
		static cls##_Registrar cls##_registrar_instance;                 \
	}

#endif /* DATALAKE_BACKEND_REGISTRY_H */
```

- [ ] **Step 2: Sanity-check the header compiles (as part of the later build)**

This header is not yet included anywhere. Defer compilation check to Task 3. Do not run any build yet.

- [ ] **Step 3: Commit**

```bash
git add contrib/datalake_fdw/src/common/backendRegistry.h
git commit -m "Feature: add BackendRegistry header for runtime storage backend dispatch

First of a small series that replaces the compile-time hard-coded
storage backend selection in fileSystemWrapper.cpp with a runtime
registry. Declares the BackendRegistry singleton, the Factory type,
and the DATALAKE_REGISTER_BACKEND macro used by each backend's .cpp
file to self-register.

No behavior change until the implementation and wiring land in
subsequent commits.

See: docs/superpowers/specs/2026-04-20-datalake-storage-registry-design.md"
```

---

## Task 2: Create BackendRegistry implementation

**Files:**
- Create: `contrib/datalake_fdw/src/common/backendRegistry.cpp`

- [ ] **Step 1: Write the implementation**

Create `contrib/datalake_fdw/src/common/backendRegistry.cpp` with the exact content below:

```cpp
/*-------------------------------------------------------------------------
 *
 * backendRegistry.cpp
 *    Implementation of the runtime storage backend registry.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/backendRegistry.cpp
 *-------------------------------------------------------------------------
 */
extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include "backendRegistry.h"
#include <sstream>

namespace Datalake {
namespace Internal {

BackendRegistry &
BackendRegistry::instance()
{
	static BackendRegistry instance;
	return instance;
}

void
BackendRegistry::registerBackend(const std::string &name, Factory factory)
{
	auto existing = backends_.find(name);
	if (existing != backends_.end())
	{
		/*
		 * Duplicate registration. In debug builds we'd like to catch
		 * this aggressively, but we can't use assert() here because
		 * static init runs before Postgres memory contexts / logging
		 * are fully up. Log at elog(WARNING) and overwrite — last
		 * registration wins. The symbol name (cls##_registrar_instance)
		 * is grep-friendly for debugging.
		 */
		elog(WARNING,
			 "datalake_fdw: duplicate backend registration for \"%s\"; "
			 "replacing existing factory",
			 name.c_str());
	}
	backends_[name] = std::move(factory);
}

FileSystem *
BackendRegistry::create(const std::string &name) const
{
	auto it = backends_.find(name);
	if (it == backends_.end())
	{
		std::ostringstream oss;
		oss << "no storage backend registered for protocol '"
			<< name << "'; registered: ";
		bool first = true;
		for (const auto &kv : backends_)
		{
			if (!first) oss << ", ";
			oss << kv.first;
			first = false;
		}
		if (first) oss << "(none)";
		throw Error("%s", oss.str().c_str());
	}
	return it->second();
}

std::vector<std::string>
BackendRegistry::listRegistered() const
{
	std::vector<std::string> names;
	names.reserve(backends_.size());
	for (const auto &kv : backends_)
		names.push_back(kv.first);
	return names;
}

} /* namespace Internal */
} /* namespace Datalake */
```

- [ ] **Step 2: Commit**

```bash
git add contrib/datalake_fdw/src/common/backendRegistry.cpp
git commit -m "Feature: implement BackendRegistry singleton

Meyers singleton + unordered_map + throw-on-miss. Duplicate-name
protection logs WARNING and overwrites. create() miss produces a
diagnostic error listing all registered names so misconfiguration is
obvious at query time.

No caller wires the registry yet; behavior unchanged."
```

---

## Task 3: Wire backendRegistry.o into the Makefile (non-Gopher build)

**Files:**
- Modify: `contrib/datalake_fdw/Makefile` around line 177-195 (the `ifeq ($(with_gopher),yes)` block).

- [ ] **Step 1: Inspect current Makefile region**

Read lines 175-200 of `contrib/datalake_fdw/Makefile` to confirm the existing structure:

```bash
sed -n '175,200p' contrib/datalake_fdw/Makefile
```

Expected: the `ifeq ($(with_gopher),yes) … else … endif` block that adds `gopherFileSystem.o` or `s3FileSystem.o`, plus the `STORAGE_BACKEND_OBJ` variable and the `all: $(STORAGE_BACKEND_OBJ)` prerequisite.

- [ ] **Step 2: Edit the Makefile to add backendRegistry.o to the non-Gopher branch**

Replace this block:

```makefile
else
OBJS += src/common/s3FileSystem.o
# AWS SDK C++ (static): high-level S3 → core → CRT C++ → C libs → crypto
SHLIB_LINK += \
  -Wl,--start-group \
  -laws-cpp-sdk-s3 -laws-cpp-sdk-core -laws-cpp-sdk-sts \
  -laws-crt-cpp -laws-c-s3 -laws-c-auth -laws-c-http \
  -laws-c-io -laws-c-cal -laws-c-compression -laws-c-mqtt \
  -laws-c-event-stream -laws-c-sdkutils -laws-c-common \
  -laws-checksums -ls2n \
  -Wl,--end-group -lpthread
STORAGE_BACKEND_OBJ = src/common/s3FileSystem.o
endif
```

with:

```makefile
else
OBJS += src/common/s3FileSystem.o
OBJS += src/common/backendRegistry.o
# AWS SDK C++ (static): high-level S3 → core → CRT C++ → C libs → crypto
SHLIB_LINK += \
  -Wl,--start-group \
  -laws-cpp-sdk-s3 -laws-cpp-sdk-core -laws-cpp-sdk-sts \
  -laws-crt-cpp -laws-c-s3 -laws-c-auth -laws-c-http \
  -laws-c-io -laws-c-cal -laws-c-compression -laws-c-mqtt \
  -laws-c-event-stream -laws-c-sdkutils -laws-c-common \
  -laws-checksums -ls2n \
  -Wl,--end-group -lpthread
STORAGE_BACKEND_OBJS = src/common/s3FileSystem.o src/common/backendRegistry.o
endif
```

Then further down in the file, change this block:

```makefile
all: $(STORAGE_BACKEND_OBJ)

$(STORAGE_BACKEND_OBJ): %.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CFLAGS_SL) $(CPPFLAGS) -c -o $@ $<
```

to:

```makefile
# Gopher branch sets STORAGE_BACKEND_OBJ (single file); non-Gopher branch
# sets STORAGE_BACKEND_OBJS (list). Normalize to one variable for the
# compile rule.
STORAGE_BACKEND_OBJS ?= $(STORAGE_BACKEND_OBJ)

all: $(STORAGE_BACKEND_OBJS)

$(STORAGE_BACKEND_OBJS): %.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CFLAGS_SL) $(CPPFLAGS) -c -o $@ $<
```

(The `?=` leaves `STORAGE_BACKEND_OBJS` alone if the non-Gopher branch already set it, and inherits the Gopher-branch `STORAGE_BACKEND_OBJ` otherwise.)

- [ ] **Step 3: Build to verify compilation**

From the development container shell (enter via `/enter-devel-container` skill if not already inside):

```bash
cd contrib/datalake_fdw
make clean && make -j$(nproc) 2>&1 | tail -40
```

Expected:
- `src/common/backendRegistry.o` appears in the output.
- No errors. Link stage completes.
- Warning output should be empty or match the baseline from before this change.

If the compile fails on `backendRegistry.cpp` complaining about `Error` being undefined, verify the include order: `backendRegistry.h` must come after `postgres.h` in `backendRegistry.cpp` (it already does — `Error` is declared in `fileSystem.h` which is included via `backendRegistry.h`).

- [ ] **Step 4: Commit**

```bash
git add contrib/datalake_fdw/Makefile
git commit -m "Feature: link backendRegistry.o into non-Gopher datalake_fdw build

Adds backendRegistry.o to OBJS for the open-source build path and
generalizes the STORAGE_BACKEND_OBJ compile rule to a list
(STORAGE_BACKEND_OBJS) so multiple registry-related .cpp files share
the same rule without duplication.

No callers of the registry yet; behavior unchanged."
```

---

## Task 4: Add `getName()` + register S3FileSystem into the registry

**Files:**
- Modify: `contrib/datalake_fdw/src/common/fileSystem.h` — add one non-pure virtual method `getName()`.
- Modify: `contrib/datalake_fdw/src/common/s3FileSystem.h` — declare `getName()` override.
- Modify: `contrib/datalake_fdw/src/common/s3FileSystem.cpp` — implement `getName()` + append file-scope registrar.

### Context

Inspired by DuckDB's `FileSystem::GetName()` (spec §3.1). Each backend
returns a short human-readable name used in wrapper diagnostics. Default
implementation returns `"Unknown"` so the commercial Gopher build keeps
compiling without source edits; native backends override.

- [ ] **Step 1: Add `getName()` to the abstract interface**

Open `contrib/datalake_fdw/src/common/fileSystem.h`. Inside the
`FileSystem` class body, add the following method after the existing
`destroyHandle` declaration (i.e., after line ~79):

```cpp
	/*
	 * getName - return a short, human-readable backend name used in
	 * diagnostic messages (e.g. "S3: connection timeout" vs bare
	 * "connection timeout"). Default "Unknown" keeps the commercial
	 * Gopher path compiling without source edits; native backends
	 * override. Inspired by DuckDB's FileSystem::GetName().
	 */
	virtual const char *getName() const { return "Unknown"; }
```

Non-pure virtual on purpose — forcing Gopher to implement would be an
unrelated commercial-side edit. Follow-up PR can override it there.

- [ ] **Step 2: Override `getName()` in S3FileSystem**

In `contrib/datalake_fdw/src/common/s3FileSystem.h`, inside the
`S3FileSystem` class body add the override declaration (alongside the
other `override` methods):

```cpp
	const char *getName() const override { return "S3"; }
```

Inline is fine — the method is trivial, no need to split across .h/.cpp.

No `.cpp` edit for this step — the inline definition is the whole
implementation.

- [ ] **Step 3: Build to verify `getName()` compiles**

```bash
cd contrib/datalake_fdw
make -j$(nproc) 2>&1 | tail -10
```

Expected: compile succeeds; no warnings about overriding a non-virtual.

- [ ] **Step 4: Commit the interface addition separately**

Commit `fileSystem.h` + `s3FileSystem.h` before the registrar work so
the interface change has its own reviewable commit:

```bash
git add contrib/datalake_fdw/src/common/fileSystem.h \
        contrib/datalake_fdw/src/common/s3FileSystem.h
git commit -m "Feature: add FileSystem::getName() for backend diagnostics

Small non-pure virtual addition: every backend reports a short name
used in wrapper error messages and logs. Default \"Unknown\" keeps the
commercial Gopher build path compiling without source edits. S3
overrides to \"S3\".

Inspired by DuckDB's FileSystem::GetName() — see
docs/superpowers/specs/2026-04-20-datalake-storage-registry-design.md
§3.1."
```

- [ ] **Step 5: Confirm current end of s3FileSystem.cpp file**

```bash
tail -20 contrib/datalake_fdw/src/common/s3FileSystem.cpp
```

Expected: the last lines close the `Datalake::Internal` namespace (two `}` lines with `namespace Internal` / `namespace Datalake` comments). We append AFTER those closing braces.

- [ ] **Step 6: Add the include near the top of s3FileSystem.cpp**

Near the top of `s3FileSystem.cpp`, add `#include "backendRegistry.h"` alongside the other local includes. The existing file already includes `s3FileSystem.h` and `fileSystem.h`; add the new include right after `fileSystem.h` (or after `s3FileSystem.h` if `fileSystem.h` is not directly included — it comes transitively via `s3FileSystem.h`).

Example (assuming current includes are `#include "s3FileSystem.h"`):

Before:
```cpp
#include "s3FileSystem.h"
```

After:
```cpp
#include "s3FileSystem.h"
#include "backendRegistry.h"
```

- [ ] **Step 7: Append the S3_Registrar block to the end of s3FileSystem.cpp**

Append this block to the END of the file, AFTER the closing namespace braces (the registrar lives outside the `Datalake::Internal` namespace because the macro already qualifies its references — but we need the `S3FileSystem` symbol in scope, so we use a `using` declaration):

```cpp

/*
 * ---- Backend registration ----
 *
 * Registers S3FileSystem under every protocol name that today maps to
 * an S3-compatible object store. The OSS family (ali, cos, qs, s3b,
 * huawei, ks3) all use the same S3 client code path; per-vendor
 * subclasses are a future refinement (spec §13) if any vendor's
 * signing / endpoint rules diverge.
 *
 * File-scope registrar struct (not DATALAKE_REGISTER_BACKEND) because
 * we register the same factory under multiple names.
 */
using ::Datalake::Internal::BackendRegistry;
using ::Datalake::Internal::FileSystem;
using ::Datalake::Internal::S3FileSystem;

namespace {
struct S3_Registrar {
	S3_Registrar() {
		auto factory = []() -> FileSystem* {
			return new S3FileSystem();
		};
		const char *names[] = {
			"s3", "ali", "cos", "qs", "s3b", "huawei", "ks3"
		};
		auto &reg = BackendRegistry::instance();
		for (const char *n : names)
			reg.registerBackend(n, factory);
	}
};
static S3_Registrar s3_registrar_instance;
} /* anonymous namespace */
```

- [ ] **Step 8: Build to verify**

```bash
cd contrib/datalake_fdw
make -j$(nproc) 2>&1 | tail -20
```

Expected: compile succeeds; link succeeds. No undefined reference errors (BackendRegistry symbols come from backendRegistry.o).

At this point the registry is populated at static-init time but nothing consults it yet.

- [ ] **Step 9: Commit**

```bash
git add contrib/datalake_fdw/src/common/s3FileSystem.cpp
git commit -m "Feature: register S3FileSystem under all S3-compatible protocol names

Adds a file-scope S3_Registrar that registers S3FileSystem under all
seven OSS family names (s3, ali, cos, qs, s3b, huawei, ks3) at static
init. No dispatch site consults the registry yet — that's the next
commit. Behavior unchanged."
```

---

## Task 5: Rewire fileSystemWrapper.cpp to use the registry

**Files:**
- Modify: `contrib/datalake_fdw/src/common/fileSystemWrapper.cpp` — the `#else` branch of `datalakeCreateFileSystem` (currently around lines 98-113).

- [ ] **Step 1: Read the current dispatch site**

```bash
sed -n '85,130p' contrib/datalake_fdw/src/common/fileSystemWrapper.cpp
```

Expected: `datalakeCreateFileSystem` with the `#ifdef USE_GOPHER … new GopherFileSystem() … #else … new S3FileSystem() … #endif` pattern plus a block that casts and calls `getUfsType()` on Gopher.

- [ ] **Step 2: Update the include block**

Near the top of the file, where the backend headers are conditionally included, replace:

```cpp
#ifdef USE_GOPHER
#include "gopherFileSystem.h"
#else
#include "s3FileSystem.h"
#endif
```

with:

```cpp
#ifdef USE_GOPHER
#include "gopherFileSystem.h"
#else
#include "backendRegistry.h"
/*
 * We no longer include a specific backend header here — backends
 * register themselves via DATALAKE_REGISTER_BACKEND. The wrapper only
 * knows the abstract FileSystem interface and the registry.
 */
#endif
```

Separately, `src/datalake_def.h` (for `struct storageOptions`) must be
pulled in inside the existing `extern "C"` block at the top of the
file, matching the pattern already used by `s3FileSystem.cpp`. Modify
the existing `extern "C"` block:

```cpp
extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}
```

to:

```cpp
extern "C" {
#include "postgres.h"
#include "utils/elog.h"
#include "src/datalake_def.h"   /* for struct storageOptions */
}
```

Also remove or update the corresponding `using` declarations lower in the file. Replace:

```cpp
#ifdef USE_GOPHER
using Datalake::Internal::GopherFileSystem;
#else
using Datalake::Internal::S3FileSystem;
#endif
```

with:

```cpp
#ifdef USE_GOPHER
using Datalake::Internal::GopherFileSystem;
#else
using Datalake::Internal::BackendRegistry;
#endif
```

- [ ] **Step 3: Rewire the dispatch site**

In `datalakeCreateFileSystem`, replace:

```cpp
		/* Select implementation at compile time */
#ifdef USE_GOPHER
		FileSystem *file = new GopherFileSystem();
#else
		FileSystem *file = new S3FileSystem();
#endif
		file->createHandle(storageOptions);
		fileStream = new ossInternalFileStream(file);

#ifdef USE_GOPHER
		/* Cache UFS type for getFileInfo path adjustment (HDFS = 9) */
		GopherFileSystem *gfs = dynamic_cast<GopherFileSystem*>(file);
		if (gfs)
			fileStream->type = gfs->getUfsType();
#else
		fileStream->type = 0;
#endif
```

with:

```cpp
		/*
		 * Select implementation:
		 *   - Commercial (USE_GOPHER): GopherFileSystem drives all
		 *     backends internally via ufsType.
		 *   - Open-source: BackendRegistry dispatches on the protocol
		 *     string set by CREATE SERVER ... OPTIONS (protocol '...').
		 */
#ifdef USE_GOPHER
		FileSystem *file = new GopherFileSystem();
		file->createHandle(storageOptions);
		fileStream = new ossInternalFileStream(file);
		/* Cache UFS type for getFileInfo path adjustment (HDFS = 9) */
		GopherFileSystem *gfs = dynamic_cast<GopherFileSystem*>(file);
		if (gfs)
			fileStream->type = gfs->getUfsType();
#else
		{
			struct storageOptions *opts =
				static_cast<struct storageOptions *>(storageOptions);
			if (opts == NULL || opts->protocol == NULL)
				throw Datalake::Internal::Error(
					"datalakeCreateFileSystem: storageOptions or "
					"protocol is NULL");

			FileSystem *file =
				BackendRegistry::instance().create(opts->protocol);
			file->createHandle(storageOptions);
			fileStream = new ossInternalFileStream(file);
			/*
			 * Each backend reports its own UFS type via getUfsId();
			 * cache it for datalakeGetFileInfo()'s path-prefix logic.
			 */
			fileStream->type = file->getUfsId();
		}
#endif
```

Note that in the non-Gopher branch `file` is now a local inside the inner block, so its scope ends at the `}`. This is fine — `fileStream` owns the pointer via `ossInternalFileStream`.

- [ ] **Step 4: Build and confirm link**

```bash
cd contrib/datalake_fdw
make -j$(nproc) 2>&1 | tail -30
```

Expected: clean compile and link. No undefined references. No warnings about unused variables in the rewritten function.

If the compile complains that `struct storageOptions` is incomplete, ensure the `#include "datalake_def.h"` added in Step 2 is present and that `datalake_def.h` forward-declares or fully declares `storageOptions` before it's used here.

- [ ] **Step 5: Install and smoke-test**

```bash
cd contrib/datalake_fdw
make install
```

From psql, create a dummy S3 foreign server and confirm the extension loads without error (it doesn't connect anywhere yet):

```sql
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
-- If an S3 test server is available in your dev env, CREATE SERVER and
-- run a simple SELECT against a Parquet foreign table. Otherwise skip
-- to Task 6 which runs the full regression.
```

- [ ] **Step 6: Commit**

```bash
git add contrib/datalake_fdw/src/common/fileSystemWrapper.cpp
git commit -m "Feature: dispatch non-Gopher backends via BackendRegistry

Replaces the hardcoded \"new S3FileSystem()\" in the #else branch with
a runtime lookup by protocol string. The wrapper no longer includes
any specific backend header — adding a new backend requires zero edits
to this file.

The per-backend UFS type (previously Gopher-only via getUfsType) is
now obtained uniformly through FileSystem::getUfsId() for all non-
Gopher backends.

Commercial USE_GOPHER path is unchanged."
```

---

## Task 6: Regression parity validation (no code changes)

**Files:** none modified. This task runs the existing regression set to confirm Phase-1 zero-diff acceptance criterion from spec §10.

- [ ] **Step 1: Run the existing datalake_fdw regression test that matters most**

The S3-targeting subset of the regression is what exercises the rewired path. From inside the dev container:

```bash
cd contrib/datalake_fdw
make install
make installcheck ICEBERG_HUDI_TEST=1 2>&1 | tee /tmp/reg-after-registry.log
```

Expected:
- All queries in `REGRESS = setup_hudi iceberg_read hudi_read` pass.
- `regression.diffs` is empty or does not exist.

If individual tests fail, compare against a baseline taken from the parent commit (commit before Task 1). The baseline should be captured once, before starting Task 1:

```bash
# BASELINE (run once before Task 1):
git stash            # or git checkout parent
make installcheck ICEBERG_HUDI_TEST=1 2>&1 | tee /tmp/reg-baseline.log
git stash pop        # return to working state
```

Compare:
```bash
diff /tmp/reg-baseline.log /tmp/reg-after-registry.log
```

Expected diff: only timing lines (`ok N — N ms`). No changed query output.

- [ ] **Step 2: Run the broader regression set if the dev env supports it**

```bash
make installcheck 2>&1 | tail -50
```

Expected: all tests pass. This exercises S3 via the many `*_s3` regression files listed in `Makefile:135-145`.

If a test fails that wasn't failing on the parent commit, it is a bug introduced by this change — investigate and fix before proceeding.

- [ ] **Step 3: No commit (this is a validation checkpoint, not a code change)**

Leave a note in your work log that regression parity was verified.

---

## Task 7: Write "Adding a Backend" documentation + Echo reference example

**Files:**
- Create: `contrib/datalake_fdw/docs/adding-a-backend.md`
- Create: `contrib/datalake_fdw/docs/examples/echoFileSystem.h`
- Create: `contrib/datalake_fdw/docs/examples/echoFileSystem.cpp`

- [ ] **Step 1: Ensure docs/examples directory exists**

```bash
mkdir -p contrib/datalake_fdw/docs/examples
```

- [ ] **Step 2: Create the developer-facing guide**

Create `contrib/datalake_fdw/docs/adding-a-backend.md` with the following content:

````markdown
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

That's it. Rebuild; your backend resolves at runtime.

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
  documented in `fileSystem.h:72-77`.
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

## Reference example

See `docs/examples/echoFileSystem.{h,cpp}` for a no-op backend that
logs every call. Useful as a skeleton and for verifying the registry
works end-to-end. Compiling it requires adding it to `OBJS` manually
(not part of the default build).

## Troubleshooting

**`elog(ERROR): no storage backend registered for protocol 'X'; registered: Y, Z`**
— the registry didn't see your `DATALAKE_REGISTER_BACKEND` call. Most
likely cause: your `.o` was not linked into the final `.so` (see
"Build system contract" above). Confirm with `nm datalake_fdw.so | grep
_registrar_instance` — the symbol should be present for every backend.

**Duplicate-registration warning at server startup**
— two backends claimed the same protocol name. Rename one.
````

- [ ] **Step 3: Create the Echo reference example header**

Create `contrib/datalake_fdw/docs/examples/echoFileSystem.h`:

```cpp
/*-------------------------------------------------------------------------
 *
 * echoFileSystem.h
 *    Reference example: a no-op storage backend that logs every call.
 *
 *    Not compiled by default. Copy into src/common/ (and add to OBJS)
 *    to exercise the BackendRegistry wiring end-to-end.
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_ECHO_FILESYSTEM_H
#define DATALAKE_ECHO_FILESYSTEM_H

#include "fileSystem.h"

namespace Datalake {
namespace Internal {

class EchoFileSystem : public FileSystem {
public:
	EchoFileSystem() = default;
	~EchoFileSystem() override = default;

	int createHandle(void *storageOptions) override;
	int openFile(const char *path, int flag) override;
	int write(void *buff, int64_t size) override;
	int read(void *buff, int64_t size) override;
	int seek(int64_t position) override;
	int closeFile() override;
	int getUfsId() override;
	const char *getName() const override { return "Echo"; }
	datalakeFileInfo* listInfo(const char *path, int &count,
							   int recursive = 1, bool iswrite = false) override;
	datalakeFileInfo* getFileInfo(const char *path) override;
	int destroyHandle() override;
};

} /* namespace Internal */
} /* namespace Datalake */

#endif
```

- [ ] **Step 4: Create the Echo reference example implementation**

Create `contrib/datalake_fdw/docs/examples/echoFileSystem.cpp`:

```cpp
/*-------------------------------------------------------------------------
 *
 * echoFileSystem.cpp
 *    Reference example backend for the BackendRegistry framework.
 *    Logs every call and returns safe zero values / empty lists.
 *
 *    Not compiled by default. To activate:
 *       1. Copy echoFileSystem.{h,cpp} to src/common/
 *       2. In Makefile non-Gopher branch add:
 *            OBJS += src/common/echoFileSystem.o
 *            STORAGE_BACKEND_OBJS += src/common/echoFileSystem.o
 *       3. make && make install
 *       4. CREATE SERVER foo FOREIGN DATA WRAPPER datalake_fdw
 *               OPTIONS (protocol 'echo');
 *
 *-------------------------------------------------------------------------
 */
extern "C" {
#include "postgres.h"
#include "utils/elog.h"
#include "utils/palloc.h"
}

#include "echoFileSystem.h"
#include "backendRegistry.h"

namespace Datalake {
namespace Internal {

int EchoFileSystem::createHandle(void *storageOptions)
{
	elog(LOG, "EchoFileSystem: createHandle");
	return 0;
}

int EchoFileSystem::openFile(const char *path, int flag)
{
	elog(LOG, "EchoFileSystem: openFile(%s, %d)", path, flag);
	return 0;
}

int EchoFileSystem::write(void *buff, int64_t size)
{
	elog(LOG, "EchoFileSystem: write(size=%lld)", (long long) size);
	return (int) size;
}

int EchoFileSystem::read(void *buff, int64_t size)
{
	elog(LOG, "EchoFileSystem: read(size=%lld)", (long long) size);
	return 0;  /* EOF */
}

int EchoFileSystem::seek(int64_t position)
{
	elog(LOG, "EchoFileSystem: seek(%lld)", (long long) position);
	return 0;
}

int EchoFileSystem::closeFile()
{
	elog(LOG, "EchoFileSystem: closeFile");
	return 0;
}

int EchoFileSystem::getUfsId()
{
	return 0;  /* object-store family */
}

datalakeFileInfo *EchoFileSystem::listInfo(const char *path, int &count,
										   int recursive, bool iswrite)
{
	elog(LOG, "EchoFileSystem: listInfo(%s)", path);
	count = 0;
	return NULL;
}

datalakeFileInfo *EchoFileSystem::getFileInfo(const char *path)
{
	elog(LOG, "EchoFileSystem: getFileInfo(%s)", path);
	return NULL;
}

int EchoFileSystem::destroyHandle()
{
	elog(LOG, "EchoFileSystem: destroyHandle");
	return 0;
}

} /* namespace Internal */
} /* namespace Datalake */

/* ---- Backend registration ---- */
DATALAKE_REGISTER_BACKEND("echo", ::Datalake::Internal::EchoFileSystem);
```

- [ ] **Step 5: Commit**

```bash
git add contrib/datalake_fdw/docs/adding-a-backend.md \
        contrib/datalake_fdw/docs/examples/echoFileSystem.h \
        contrib/datalake_fdw/docs/examples/echoFileSystem.cpp
git commit -m "Doc: add \"Adding a Backend\" guide with EchoFileSystem example

Developer-facing recipe for the BackendRegistry framework. Covers the
three-file procedure, the build-system contract (why OBJS, not .a),
the FileSystem interface semantics, and a copy-pasteable reference
backend (EchoFileSystem) that logs every call.

The example is intentionally not compiled by default — it is a
dogfood / skeleton reference that users activate manually when
adding their own backend."
```

---

## Task 8: Dogfood acceptance run (manual, not committed)

**Files:** none permanently modified. This task exercises the Echo example to confirm the framework's §10 acceptance criterion (a new backend adds without touching any existing plumbing).

- [ ] **Step 1: Temporarily copy Echo into the build tree**

```bash
cp contrib/datalake_fdw/docs/examples/echoFileSystem.h   contrib/datalake_fdw/src/common/
cp contrib/datalake_fdw/docs/examples/echoFileSystem.cpp contrib/datalake_fdw/src/common/
```

- [ ] **Step 2: Temporarily add Echo to the Makefile**

In the non-Gopher `else` branch of `contrib/datalake_fdw/Makefile` (around line 183), add:

```makefile
OBJS += src/common/echoFileSystem.o
STORAGE_BACKEND_OBJS += src/common/echoFileSystem.o
```

Note: this edit is the **only** edit required to a non-new file. The
acceptance criterion is satisfied if no other existing file is edited.

- [ ] **Step 3: Build and install**

```bash
cd contrib/datalake_fdw
make -j$(nproc) && make install
```

Expected: clean build. Link includes `echoFileSystem.o`.

- [ ] **Step 4: Confirm the registrar symbol is present**

```bash
nm --demangle $(pg_config --pkglibdir)/datalake_fdw.so | grep -i registrar
```

Expected output contains (among others):
```
... EchoFileSystem_Registrar ...
... S3_Registrar ...
```

If `EchoFileSystem_Registrar` is missing, DCE dropped it — re-check
Step 2 added Echo to `OBJS` (not just `STORAGE_BACKEND_OBJS`).

- [ ] **Step 5: Exercise the registry via SQL**

From psql:

```sql
CREATE EXTENSION IF NOT EXISTS datalake_fdw;

CREATE SERVER echo_srv
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (protocol 'echo');

CREATE USER MAPPING FOR current_user
    SERVER echo_srv
    OPTIONS (user 'dummy', password 'dummy');

CREATE FOREIGN TABLE echo_t (a int)
    SERVER echo_srv
    OPTIONS (filepath '/any/path', format 'text');

SELECT count(*) FROM echo_t;  -- expect 0 rows, no errors
```

Check the server log (or `tail -f` the log during the SELECT). Expect
`EchoFileSystem: createHandle`, `listInfo`, `openFile`, `read`, `closeFile`
messages.

If `CREATE SERVER` itself fails with "no storage backend registered for
protocol 'echo'", the registrar is not running. Re-check §8 / Step 4.

- [ ] **Step 6: Revert the temporary Echo activation**

```bash
rm contrib/datalake_fdw/src/common/echoFileSystem.h
rm contrib/datalake_fdw/src/common/echoFileSystem.cpp
```

Then restore the Makefile: remove the two lines added in Step 2.

```bash
cd contrib/datalake_fdw
make clean && make -j$(nproc) && make install
```

Confirm `git status` shows only the plan artifacts and committed files — no stray changes from the dogfood run.

- [ ] **Step 7: No commit (this is an acceptance run, not a code change)**

Record the dogfood result in your work log: "Echo backend registered and served via `protocol 'echo'` without edits to wrapper / registry / other backends; acceptance met."

---

## Done Criteria

- [ ] BackendRegistry header and implementation committed (Tasks 1–2).
- [ ] Makefile wires backendRegistry.o into the non-Gopher build (Task 3).
- [ ] `FileSystem::getName()` virtual method added with `"Unknown"` default; S3 overrides to `"S3"` (Task 4, Steps 1-4).
- [ ] S3FileSystem registers under all 7 S3-compatible protocol names (Task 4, Steps 5-9).
- [ ] fileSystemWrapper.cpp's `#else` branch dispatches through the registry; gopher branch untouched (Task 5).
- [ ] Existing datalake_fdw regression set runs with byte-identical results vs. pre-change baseline (Task 6).
- [ ] `docs/adding-a-backend.md` and `docs/examples/echoFileSystem.*` committed (Task 7).
- [ ] Echo dogfood backend compiles, registers, and serves a `protocol 'echo'` foreign server without edits to any existing file (Task 8).

## Notes for future work (out of scope)

- Drop `DLProt` enum and centralize protocol names as the registry's
  string keys — follow-up PR after registry settles.
- Specific new backends (HDFS, Azure Blob, GCS, …) — each gets its own
  spec + plan that consumes this framework.
- Dynamic `dlopen` plugin loading — if/when third-party ecosystem emerges.
