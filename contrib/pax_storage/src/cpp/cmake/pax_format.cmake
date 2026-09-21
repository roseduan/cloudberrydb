# paxformat.so
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

set(pax_comm_src
    comm/bitmap.cc
    comm/bloomfilter.cc
    comm/byte_buffer.cc
    comm/fast_io.cc
    comm/guc.cc
    comm/paxc_wrappers.cc
    comm/pax_memory.cc
    comm/pax_resource.cc
    comm/cbdb_wrappers.cc
    comm/vec_numeric.cc)

set(pax_exceptions_src
    exceptions/CException.cc)

set(pax_storage_src
    storage/columns/pax_column_traits.cc
    storage/columns/pax_column.cc
    storage/columns/pax_compress.cc
    storage/columns/pax_columns.cc
    storage/columns/pax_encoding_utils.cc
    storage/columns/pax_encoding_non_fixed_column.cc
    storage/columns/pax_encoding_column.cc
    storage/columns/pax_dict_encoding.cc
    storage/columns/pax_decoding.cc
    storage/columns/pax_encoding.cc
    storage/columns/pax_delta_encoding.cc
    storage/columns/pax_deltadelta_encoding.cc
    storage/columns/pax_gorilla_encoding.cc
    storage/columns/pax_simple8b_rle.cc
    storage/columns/pax_bool_encoding.cc
    storage/columns/pax_rlev2_decoding.cc
    storage/columns/pax_rlev2_encoding.cc
    storage/columns/pax_vec_column.cc
    storage/columns/pax_vec_bitpacked_column.cc
    storage/columns/pax_vec_bpchar_column.cc
    storage/columns/pax_vec_encoding_column.cc
    storage/columns/pax_vec_numeric_column.cc
    storage/oper/pax_oper_udf.cc
    storage/filter/pax_filter.cc
    storage/filter/pax_fast_filter.cc
    storage/filter/pax_row_filter.cc
    storage/filter/pax_sparse_filter.cc
    storage/filter/pax_sparse_pg_path.cc
    storage/filter/pax_sparse_vec_path.cc
    storage/oper/pax_oper.cc
    storage/oper/pax_stats.cc
    storage/file_system.cc
    storage/local_file_system.cc
    storage/micro_partition.cc
    storage/micro_partition_file_factory.cc
    storage/micro_partition_metadata.cc
    storage/micro_partition_row_filter_reader.cc
    storage/micro_partition_stats.cc
    storage/micro_partition_stats_updater.cc
    storage/micro_partition_udf.cc
    storage/orc/orc_dump_reader.cpp
    storage/orc/orc_format_reader.cc
    storage/orc/orc_group.cc
    storage/orc/orc_vec_group.cc
    storage/orc/orc_reader.cc
    storage/orc/orc_type.cc
    storage/orc/orc_writer.cc
    storage/pax_buffer.cc
    storage/proto/protobuf_stream.cc
    storage/toast/pax_toast.cc
    storage/wal/pax_wal.cc
    storage/wal/paxc_desc.c
    storage/wal/paxc_wal.cc
   )

   set(pax_clustering_src
   clustering/clustering.cc
   clustering/sorter_tuple.cc
   clustering/sorter_index.cc
   clustering/zorder_clustering.cc
   clustering/index_clustering.cc
   clustering/lexical_clustering.cc
   clustering/zorder_utils.cc
 )
 
set(pax_vec_src)
if(VEC_BUILD)
set(pax_vec_src
  storage/vec/arrow_wrapper.cc
  storage/vec/pax_porc_adpater.cc
  storage/vec/pax_porc_vec_adpater.cc
  storage/vec/pax_vec_adapter.cc
  storage/vec/pax_vec_comm.cc
  storage/vec/pax_vec_reader.cc
)
set(pax_vec_src ${pax_vec_src} storage/vec_parallel_common.cc)

endif()

set(pax_target_include ${ZTSD_HEADER} ${CMAKE_CURRENT_SOURCE_DIR} ${CBDB_INCLUDE_DIR})
# tabulate is header-only; the tabulate::tabulate INTERFACE target (from
# find_package(tabulate CONFIG) in FindDependencies.cmake) carries its
# include path, so no separate include dir is needed here.
set(pax_target_link_libs zstd z tabulate::tabulate)
# protobuf v22+ on macOS splits its abseil deps into separate libs;
# pull them in via pkg-config.
if(APPLE)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(PB_PC REQUIRED protobuf)
  set(pax_target_include ${pax_target_include} ${PB_PC_INCLUDE_DIRS})
  list(APPEND pax_target_link_libs ${PB_PC_LIBRARIES})
else()
  list(APPEND pax_target_link_libs protobuf ${PAX_PROTOBUF_ABSL_LIBS})
endif()
# liburing is Linux-only (kernel io_uring iface). macOS provides uuid_*
# functions in libSystem, so -luuid is also Linux-only.
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  list(APPEND pax_target_link_libs uuid uring)
endif()
if (PAX_USE_LZ4)
  list(APPEND pax_target_link_libs lz4)
endif()
set(pax_target_link_directories ${PROJECT_SOURCE_DIR}/../../src/backend/)
if(APPLE)
  list(APPEND pax_target_link_directories ${PB_PC_LIBRARY_DIRS})
endif()

# vec build
if (VEC_BUILD)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(GLIB REQUIRED glib-2.0)
  set(pax_target_include
      ${pax_target_include}
      ${VEC_HOME}/src/include # for utils/tuptable_vec.h
      ${INSTALL_HOME}/include  # for arrow-glib/arrow-glib.h and otehr arrow interface
      ${GLIB_INCLUDE_DIRS} # for glib-object.h
  )
  list(GET CMAKE_PREFIX_PATH 0 PAX_THIRDPARTY_DIR)
  set(pax_target_link_directories
      ${pax_target_link_directories}
      ${INSTALL_HOME}/lib
      ${PAX_THIRDPARTY_DIR}/lib)
  set(pax_target_link_libs
      ${pax_target_link_libs}
      arrow arrow_dataset
      orc
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
endif(VEC_BUILD)

add_library(paxformat SHARED ${PROTO_SRCS} ${pax_storage_src} ${pax_clustering_src} ${pax_exceptions_src} ${pax_comm_src} ${pax_vec_src})
target_include_directories(paxformat PUBLIC ${pax_target_include})
target_link_directories(paxformat PUBLIC ${pax_target_link_directories})
target_link_libraries(paxformat PRIVATE ${pax_target_link_libs})

set_target_properties(paxformat PROPERTIES
  OUTPUT_NAME paxformat)
if(APPLE)
  # PAX C++ code calls PG backend functions (write_stderr,
  # xlog_check_consistency_hook, ...). On Linux ld defers unresolved
  # references in .so; macOS ld rejects them unless told otherwise.
  # Defer them to load time so paxformat is usable wherever PG symbols
  # are provided.
  set_target_properties(paxformat PROPERTIES
    LINK_FLAGS "-Wl,-undefined,dynamic_lookup")
endif()
add_dependencies(paxformat generate_protobuf)

# export headers
set(PAX_COMM_HEADERS
  comm/bitmap.h
  comm/cbdb_api.h
  comm/log.h
  comm/cbdb_wrappers.h
  comm/pax_rel.h
  comm/pax_memory.h
  comm/guc.h
)

set(PAX_EXCEPTION_HEADERS
  exceptions/CException.h
)

set(PAX_CLUSTERING_HEADERS
  clustering/clustering.h
  clustering/clustering_reader.h
  clustering/clustering_writer.h
  clustering/index_clustering.h
  clustering/lexical_clustering.h
  clustering/zorder_clustering.h
  clustering/zorder_utils.h
)

# TODO(gongxun):
# We should explicitly specify the headers
# that need to be exported, and use the syntax of
# install(FILES,...) to install the header files
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/storage
  DESTINATION ${CMAKE_INSTALL_PREFIX}/include/pax
  FILES_MATCHING
  PATTERN "*.h"
)

install(FILES ${PAX_COMM_HEADERS}
  DESTINATION ${CMAKE_INSTALL_PREFIX}/include/pax/comm
)

install(FILES ${PAX_EXCEPTION_HEADERS}
  DESTINATION ${CMAKE_INSTALL_PREFIX}/include/pax/exceptions
)

install(FILES ${PAX_CLUSTERING_HEADERS}
  DESTINATION ${CMAKE_INSTALL_PREFIX}/include/pax/clustering
)

## install dynamic libraray
install(TARGETS paxformat
  LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}/lib)

## test whether libpaxformat.so can be linked normally
add_executable(paxformat_test paxformat_test.cc)
target_include_directories(paxformat_test PUBLIC ${pax_target_include} ${CMAKE_CURRENT_SOURCE_DIR})
add_dependencies(paxformat_test paxformat)
if(APPLE)
  # No libpostgres.so to link against on macOS; defer PG symbols to
  # load time. The test still validates that paxformat itself is a
  # complete dylib.
  target_link_libraries(paxformat_test PRIVATE paxformat)
  set_target_properties(paxformat_test PROPERTIES
    LINK_FLAGS "-Wl,-undefined,dynamic_lookup")
else()
  target_link_libraries(paxformat_test PRIVATE paxformat ${pax_target_link_libs} postgres)
endif()
