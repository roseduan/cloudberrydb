# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
## protobuf
include(FindProtobuf)
find_package(Protobuf 3.5.0 REQUIRED)

# tabulate (header-only, used to format UDF output) and yyjson (the
# manifest-API catalog implementation's JSON layer) are consumed as
# system-installed CMake config packages -- not vendored submodules --
# matching how main resolves them.
find_package(tabulate REQUIRED CONFIG)
if (USE_MANIFEST_API)
  find_package(yyjson REQUIRED CONFIG)
endif()

# protobuf 22+ moved its runtime logging (used e.g. by parse_context.h's
# CHECK-style asserts) into separate libabsl_log_* static archives; the
# legacy FindProtobuf module (used above, since this protobuf predates
# CMake config-file packages) has no idea these exist, so anything that
# links libprotobuf.a directly -- not just as part of another .so, which
# is allowed undefined symbols by default -- needs them listed explicitly.
# macOS instead gets these transitively via pkg-config (see pax(_format).cmake).
if(NOT APPLE)
  set(PAX_PROTOBUF_ABSL_LIBS
      absl_log_internal_check_op
      absl_leak_check
      absl_die_if_null
      absl_log_internal_conditions
      absl_log_internal_message
      absl_log_internal_nullguard
      absl_examine_stack
      absl_log_internal_format
      absl_log_internal_proto
      absl_log_internal_log_sink_set
      absl_log_sink
      absl_log_entry
      absl_flags
      absl_flags_internal
      absl_flags_marshalling
      absl_flags_reflection
      absl_flags_config
      absl_flags_program_name
      absl_flags_private_handle_accessor
      absl_flags_commandlineflag
      absl_flags_commandlineflag_internal
      absl_log_initialize
      absl_log_globals
      absl_log_internal_globals
      absl_raw_hash_set
      absl_hash
      absl_city
      absl_low_level_hash
      absl_hashtablez_sampler
      absl_statusor
      absl_status
      absl_cord
      absl_cordz_info
      absl_cord_internal
      absl_cordz_functions
      absl_exponential_biased
      absl_cordz_handle
      absl_crc_cord_state
      absl_crc32c
      absl_crc_internal
      absl_crc_cpu_detect
      absl_bad_optional_access
      absl_str_format_internal
      absl_strerror
      absl_synchronization
      absl_stacktrace
      absl_symbolize
      absl_debugging_internal
      absl_demangle_internal
      absl_graphcycles_internal
      absl_kernel_timeout_internal
      absl_malloc_internal
      absl_time
      absl_strings
      absl_string_view
      absl_throw_delegate
      absl_strings_internal
      absl_base
      absl_spinlock_wait
      absl_int128
      absl_civil_time
      absl_time_zone
      absl_bad_variant_access
      absl_raw_logging_internal
      absl_log_severity)
endif()

# ztsd
# in our image snapshot, zstd is managed using pkg-config, so so the pkg-config method is used first here
find_package(PkgConfig QUIET)
if(PKGCONFIG_FOUND)
    pkg_check_modules(ZSTD libzstd)
endif()
if(NOT ZSTD_FOUND)
    find_package(ZSTD QUIET)
    if(NOT ZSTD_FOUND)
        message(FATAL_ERROR "zstd not found")
    endif()
endif()

## for vectorazition
if (VEC_BUILD)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(GLIB REQUIRED glib-2.0)
endif(VEC_BUILD)
