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
 *    non-Gopher branch: create(options->protocol) -> FileSystem*.
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
	 * the same factory under multiple names (aliases - see S3 family).
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

	/* For diagnostics - included in create()'s miss-error message. */
	std::vector<std::string> listRegistered() const;

	/* Non-copyable / non-assignable (public per modernize-use-equals-delete). */
	BackendRegistry(const BackendRegistry &) = delete;
	BackendRegistry &operator=(const BackendRegistry &) = delete;

private:
	BackendRegistry() = default;

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
 * docs/adding-a-backend.md "Build system contract".
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
