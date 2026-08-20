# contrib/datalake_fdw/Makefile

MODULE_big = datalake_fdw

OBJS = 	src/datalake_fdw.o \
		src/datalake_def.o \
		src/datalake_fragment.o \
		src/datalake_option.o \
		src/iceberg_volume_fdw/iceberg_volume_option.o \
		src/iceberg_volume_fdw/iceberg_volume_fdw.o \
		src/iceberg_volume_fdw/iceberg_toolkit_volume_fdw.o \
		src/iceberg_catalog_fdw/iceberg_catalog_fdw.o \
		src/iceberg_catalog_fdw/iceberg_catalog_option.o \
		src/iceberg_catalog_fdw/iceberg_toolkit_catalog_fdw.o \
		src/common/iceberg_toolkit_common.o \
		src/common/fdwFunction.o \
		src/common/dataBufferArray.o \
		src/common/dataBufferArray_c.o \
		src/common/datalakeBuffer.o \
		src/common/fileSystemWrapper.o \
		src/common/logicalType.o \
		src/common/readPolicy.o \
		src/common/rewrLogical.o \
		src/common/makeHash.o \
		src/common/util.o \
		src/common/partition_selector.o \
		src/common/random_segment.o \
		src/common/grammar_convert.o \
		src/common/exttable.o \
		src/common/datalake_resowner.o \
		src/common/parser_option.o \
		src/dlproxy/deparse.o \
		src/dlproxy/filters.o \
		src/dlproxy/headers.o \
		src/dlproxy/hive.o \
		src/dlproxy/hudi.o \
		src/dlproxy/iceberg.o \
		src/dlproxy/iceberg_fragment_cache.o \
		src/dlproxy/libchurl.o \
		src/dlproxy/protocol.o \
		src/dlproxy/uriparser.o \
		src/dlproxy/utils.o \
		src/dlproxy/icebergOptions.o \
		src/dlproxy/icebergConfig.o \
		src/provider/parquet/read/parquetRead.o \
		src/provider/parquet/read/parquetInputStream.o \
		src/provider/parquet/read/parquetFileReader.o \
		src/provider/parquet/write/parquetWrite.o \
		src/provider/parquet/write/parquetOutputStream.o \
		src/provider/parquet/write/parquetFileWriter.o \
		src/provider/orc/read/orcInputStream.o \
		src/provider/orc/read/orcReadDeltaFile.o \
		src/provider/orc/read/orcReadInterface.o \
		src/provider/orc/read/orcFileReader.o \
		src/provider/orc/read/orcRead.o \
		src/provider/orc/read/orc_stripe_filter.o \
		src/provider/orc/read/orcReadRecordBatch.o \
		src/provider/orc/write/orcWriter.o \
		src/provider/archive/read/archiveFileRead.o \
		src/provider/archive/read/archiveRead.o \
		src/provider/archive/read/lineRecordReader.o \
		src/provider/archive/read/textFileArchiveRead.o \
		src/provider/archive/read/textFileDeflateRead.o \
		src/provider/archive/read/textFileInput.o \
		src/provider/archive/read/textFileRead.o \
		src/provider/archive/read/textFileReadPolicy.o \
		src/provider/archive/read/textFileSimpleRead.o \
		src/provider/archive/read/textFileSnappyRead.o \
		src/provider/archive/write/archiveFileWrite.o \
		src/provider/archive/write/archiveWrite.o \
		src/provider/common/config.o \
		src/provider/common/datalake_random_file.o \
		src/provider/common/base_reader.o \
		src/provider/common/common.o \
		src/provider/common/utils.o \
		src/provider/common/parquet_reader_c.o \
		src/provider/common/parquet_reader.o \
		src/provider/common/rowgroup_filter.o \
		src/provider/common/avro_block_reader.o \
		src/provider/common/avro_block_reader_c.o \
		src/provider/common/file_reader.o \
		src/provider/common/delete_bitmap.o \
		src/provider/common/delete_bitmap_c.o \
		src/provider/common/sorted_merge.o \
		src/provider/common/sorted_merge_c.o \
		src/provider/common/kryo.o \
		src/provider/common/kryo_input.o \
		src/provider/common/row_reader.o \
		src/provider/common/mdb.o \
		src/provider/common/midl.o \
		src/provider/common/datalake_numeric.o \
		src/provider/iceberg/iceberg_equality_filter.o \
		src/provider/iceberg/iceberg_task_reader.o \
		src/provider/iceberg/iceberg_position_filter.o \
		src/provider/iceberg/iceberg_delete_index.o \
		src/provider/iceberg/iceberg_read.o \
		src/provider/iceberg/iceberg_write.o \
		src/provider/iceberg/iceberg_posdel_write.o \
		src/provider/iceberg/iceberg_file_index.o \
		src/provider/hudi/hudi_btree_merger.o \
		src/provider/hudi/hudi_deltalog_filter.o \
		src/provider/hudi/hudi_hashtab_merger.o \
		src/provider/hudi/hudi_logfile_block_reader.o \
		src/provider/hudi/hudi_merged_logfile_record_reader.o \
		src/provider/hudi/hudi_task_reader.o \
		src/provider/hudi/hudi_read.o \
		src/provider/avro/read/avroBlockReader.o \
		src/provider/avro/read/avroRead.o \
		src/provider/avro/write/avroWriter.o \
		src/provider/avro/write/avroWrite.o \
		src/provider/providerWrapper.o \
		src/provider/provider.o \
		src/hdw_gopher_cache/hdw_gopher_cache.o \
		src/am_iceberg/pg_iceberg_guc.o \
		src/am_iceberg/pg_iceberg_ddl.o \
		src/am_iceberg/pg_iceberg_am.o \
		src/am_iceberg/pg_iceberg_am_handler.o \
		src/am_iceberg/pg_iceberg_catalog.o \
		src/am_iceberg/pg_iceberg_metadata.o \
		src/am_iceberg/pg_iceberg_deletion_queue.o \
		src/am_iceberg/pg_iceberg_catalog_helper.o \
		src/am_iceberg/pg_iceberg_metadata_reader.o \
		src/am_iceberg/pg_iceberg_catalog_utils.o \
		src/am_iceberg/pg_iceberg_options.o \
		src/am_iceberg/pg_iceberg_metadata_tracker.o \
		src/am_iceberg/pg_iceberg_rewrite_plan.o \
		src/am_iceberg/pg_iceberg_extensible.o \
		src/am_iceberg/pg_iceberg_custom_scan.o \
		src/am_iceberg/pg_iceberg_pending_delete.o \
		src/am_iceberg/pg_iceberg_av_consumer.o \
		src/common/fileMetadata.o \
		src/components/agent_cli/src/agent_c_api.o \
		src/components/agent_cli/src/agent_client.o \
		src/components/agent_cli/src/http_client.o \
		src/components/agent_cli/src/agent_types.o \
		src/components/agent_cli/src/json_builder.o \
		src/components/agent_cli/src/agent_cjson_builder.o \
		src/components/agent_cli/src/logger.o

# Avoid building LLVM Bitcode for datalake_fdw module.
with_llvm = no

SHLIB_LINK += -lparquet -lorc -larchive -lavrocpp -larrow -lavro -lcurl -lyaml -lboost_iostreams -lsnappy -lprotobuf -lstdc++ -lm -lssl -lcrypto

ifneq ($(filter distclean clean,$(MAKECMDGOALS)),)
JANSSON_STATIC_LIBS = dummy
else
JANSSON_STATIC_PATH ?= /usr/lib64 /usr/local/lib /usr/lib /opt/local/lib
JANSSON_STATIC_LIBS = $(wildcard $(addsuffix /libjansson.a,$(JANSSON_STATIC_PATH)))
endif

ifneq ($(JANSSON_STATIC_LIBS),)
    $(info Found JANSSON_STATIC_LIBS: $(JANSSON_STATIC_LIBS))
    $(info Using JANSSON_STATIC_LIB: $(firstword $(JANSSON_STATIC_LIBS)))
    SHLIB_LINK += -Wl,--exclude-libs=libjansson.a -Wl,-Bstatic $(firstword $(JANSSON_STATIC_LIBS)) -Wl,-Bdynamic
else
    $(error FATAL: No JANSSON_STATIC_LIBS found in paths: $(JANSSON_STATIC_PATH))
    $(info Please specify JANSSON_STATIC_PATH with: make JANSSON_STATIC_PATH="/path/to/lib")
endif

PG_CPPFLAGS = -DSNAPPY_CODEC_AVAILABLE -Isrc/iceberg_catalog_fdw -Isrc/components/agent_cli/c_interface -Isrc/components/agent_cli/include

EXTENSION = datalake_fdw
DATA = datalake_fdw--1.0.sql

ifdef ICEBERG_HUDI_TEST
REGRESS = setup_hudi iceberg_read hudi_read
else
REGRESS = setup \
        hive_connecter_orc_read hive_connecter_orc_vec_read hive_connecter_parquet_read hive_connecter_avro_read hive_connector \
        hive_connecter_avro_read_limit_segment hive_connecter_orc_read_limit_segment hive_connecter_parquet_read_limit_segment \
        hive_connector_limit_segment \
        hive_connecter_orc_read_s3 hive_connecter_orc_vec_read_s3 hive_connecter_parquet_read_s3 hive_connecter_avro_read_s3 \
        hive_connector_s3 hive_connecter_avro_read_limit_segment_s3 hive_connecter_orc_read_limit_segment_s3 \
        hive_connecter_parquet_read_limit_segment_s3 hive_connector_limit_segment_s3 \
        hive_connecter_read_new_deflate_text hive_connecter_read_new_snappy_text \
        hive_connecter_read_new_text hive_connecter_read_text \
        hive_connecter_read_new_deflate_text_s3 hive_connecter_read_new_snappy_text_s3 \
        hive_connecter_read_new_text_s3 hive_connecter_read_text_s3 \
        orc_read parquet_read avro_read iresearch_orc_test iresearch_orc_vec_test \
        oss_read_new_text oss_read_text grammar_convert gphdfs_ignore_file gphdfs_custom \
        oss_read_new_text_limit_segment oss_read_orc_limit_segment oss_read_parquet_limit_segment oss_read_text_limit_segment \
		iceberg_test iceberg_no_extension iceberg_am_misuse
endif
REGRESS_OPTS += --init-file=test/init_file

arrow_dist_dir = $(abspath $(top_builddir)/contrib/apache-arrow/dist)

PG_CPPFLAGS += -fPIC -I$(arrow_dist_dir)/include -I../../src/interfaces

# Override -O0 from the global build for this performance-critical module.
# The datalake_fdw hot paths (numeric conversion, row population, vector access)
# benefit significantly from inlining and loop optimizations.
PG_CFLAGS += -O2
PG_CXXFLAGS += -O2

PG_CFLAGS += -Wno-error=stringop-overflow
PG_CFLAGS += -I $(shell pg_config --includedir) -I../../src/interfaces
PG_LDFLAGS += $(shell pg_config --ldflags) -L$(arrow_dist_dir)/lib

ifdef USE_PGXS
PG_CONFIG = pg_config

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/datalake_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Install the iceberg AM cdb_init.d script so initdb registers
# pg_iceberg_tableam_handler (OID 8321) and the iceberg access method
# (OID 8320) in template1. See iceberg-cdbinit--1.0.sql.
.PHONY: install-data uninstall-data
install-data:
	$(INSTALL_DATA) $(srcdir)/iceberg-cdbinit--1.0.sql '$(DESTDIR)$(datadir)/cdb_init.d/iceberg-cdbinit--1.0.sql'

uninstall-data:
	$(RM) '$(DESTDIR)$(datadir)/cdb_init.d/iceberg-cdbinit--1.0.sql'

install: install-data
uninstall: uninstall-data

# ---- Storage backend selection (after PGXS include for variable access) ----
# Default-commercial: build the open-source backend (S3 SDK + libhdfs3) ONLY
# when with_gopher is explicitly "no" (configure --without-gopher). Anything
# else -- "yes" OR unset/empty -- builds the Gopher backend. An unset value
# happens when Makefile.global comes from a mainline base predating this
# switch; it must default to Gopher to match historical behavior (and so CI
# images without the AWS SDK don't try to compile the S3 backend).
ifneq ($(with_gopher),no)
override CPPFLAGS += -DUSE_GOPHER
OBJS += src/common/gopherFileSystem.o
SHLIB_LINK += -lgopher
STORAGE_BACKEND_OBJ = src/common/gopherFileSystem.o
else
OBJS += src/common/s3FileSystem.o
OBJS += src/common/backendRegistry.o
OBJS += src/common/hdfsFileSystem.o
# AWS SDK C++ (static): high-level S3 → core → CRT C++ → C libs → crypto
SHLIB_LINK += \
  -Wl,--start-group \
  -laws-cpp-sdk-s3 -laws-cpp-sdk-core -laws-cpp-sdk-sts \
  -laws-crt-cpp -laws-c-s3 -laws-c-auth -laws-c-http \
  -laws-c-io -laws-c-cal -laws-c-compression -laws-c-mqtt \
  -laws-c-event-stream -laws-c-sdkutils -laws-c-common \
  -laws-checksums -ls2n \
  -Wl,--end-group -lpthread
# HDFS native backend via libhdfs3
SHLIB_LINK += -lhdfs3
# The dlproxy/libchurl HTTP client (to the dlagent) uses libcurl, and Iceberg
# data-file naming uses uuid_unparse from system libuuid.  The commercial build
# resolves both transitively through -lgopher; the open-source build has no
# gopher chain, so link them directly to keep datalake_fdw.so self-contained.
SHLIB_LINK += -lcurl -luuid
STORAGE_BACKEND_OBJS = src/common/s3FileSystem.o \
                      src/common/backendRegistry.o \
                      src/common/hdfsFileSystem.o
endif

# PGXS snapshots OBJS at include time for the .so target's prerequisites.
# The conditional OBJS += above happens after include, so the storage backend
# .o files are NOT in the .so's dependency list - under parallel make the
# linker can start before they compile. Fix: explicit .so dependency AND an
# "all" prereq (belt and suspenders).

# Gopher branch sets STORAGE_BACKEND_OBJ (single file); non-Gopher branch
# sets STORAGE_BACKEND_OBJS (list). Normalize to one variable for the
# compile rule.
STORAGE_BACKEND_OBJS ?= $(STORAGE_BACKEND_OBJ)

datalake_fdw.so: $(STORAGE_BACKEND_OBJS)

all: $(STORAGE_BACKEND_OBJS)

$(STORAGE_BACKEND_OBJS): %.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CFLAGS_SL) $(CPPFLAGS) -c -o $@ $<
