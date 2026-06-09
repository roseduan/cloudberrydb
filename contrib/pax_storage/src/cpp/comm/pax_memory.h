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
 * pax_memory.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/pax_memory.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/cbdb_api.h"
#include <cstdio>
#include <memory>
#include <type_traits>

#include "exceptions/CException.h"

namespace pax {

// class tag for object that needs to release its memory resource
// in the destructor function.
class MemoryObject {
public:
 virtual ~MemoryObject() = default;
};

// PAX_ALLOC failures raise a cbdb::CException of kExTypeOOM (instead of a raw
// std::bad_alloc) so that the CBDB_CATCH_DEFAULT path preserves the "Out of
// Memory" classification, a stack trace, and the requested size in the user-
// visible ereport — rather than degrading to the bare "__func__" fallback in
// CBDB_END_TRY which leaks internal file:line and hides the real cause.
[[noreturn]] static inline void PaxRaiseOOM(const char *file, int line, const char *what, size_t size) {
  char buf[96];
  snprintf(buf, sizeof(buf), "%s: out of memory (requested %zu bytes)",
           what, size);
  cbdb::CException::Raise(file, line,
                          cbdb::CException::ExType::kExTypeOOM, buf);
}

template <typename T=void>
static inline T* PAX_ALLOC(size_t size) {
  auto p = malloc(size);
  if (unlikely(!p)) PaxRaiseOOM(__FILE__, __LINE__, "PAX_ALLOC", size);

  return reinterpret_cast<T *>(p);
}

template <typename T=void>
static inline T* PAX_ALLOC0(size_t size) {
  auto p = PAX_ALLOC<T>(size);
  memset(reinterpret_cast<void *>(p), 0, size);
  return p;
}

template <typename T=void>
static inline T* PAX_REALLOC(void *ptr, size_t new_size) {
  auto p = realloc(reinterpret_cast<void *>(ptr), new_size);
  if (unlikely(!p)) PaxRaiseOOM(__FILE__, __LINE__, "PAX_REALLOC", new_size);

  return reinterpret_cast<T *>(p);
}

template <typename T=void>
static inline void PAX_FREE(const T *ptr) {
  T *p = const_cast<T *>(ptr);
  if (p) free(reinterpret_cast<void *>(p));
}

template <typename T, typename... Args>
static inline T* PAX_NEW(Args&&... args) {
  return new T(std::forward<Args>(args)...);
}

template <typename T>
static inline T* PAX_NEW_ARRAY(size_t N) {
  return new T[N];
}

template <typename T>
static inline void PAX_DELETE(T *obj) {
  delete obj;
}

template <typename T>
static inline void PAX_DELETE(const T *obj) {
  delete obj;
}

template <typename T>
static inline void PAX_DELETE_ARRAY(const T *obj) {
  delete[] obj;
}

template <typename T>
static inline void PAX_DELETE_ARRAY(T * const &obj) {
  delete []obj;
}

template <typename T>
static inline void ReleaseTopObject(Datum arg) {
  auto obj = reinterpret_cast<T *>(DatumGetPointer(arg));
  Assert(obj);

  PAX_DELETE(obj);
}

}

