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
		 * are fully up. Log at elog(WARNING) and overwrite - last
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
