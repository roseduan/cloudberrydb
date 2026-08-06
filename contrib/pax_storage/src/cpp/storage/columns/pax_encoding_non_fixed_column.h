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
 * pax_encoding_non_fixed_column.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_encoding_non_fixed_column.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "storage/columns/pax_columns.h"
#include "storage/columns/pax_compress.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"

namespace pax {
class PaxNonFixedEncodingColumn : public PaxNonFixedColumn {
 public:
  PaxNonFixedEncodingColumn(uint32 data_capacity, uint32 offsets_capacity,
                            const PaxEncoder::EncodingOption &encoder_options);

  PaxNonFixedEncodingColumn(uint32 data_capacity, uint32 offsets_capacity,
                            const PaxDecoder::DecodingOption &decoding_option);

  ~PaxNonFixedEncodingColumn() override;

  void Set(std::unique_ptr<DataBuffer<char>> data,
           std::unique_ptr<DataBuffer<int32>> offsets,
           size_t total_size) override;

  std::pair<char *, size_t> GetBuffer() override;

  // Per-position and range accessors used by the vectorized read path read
  // data_ directly, so a lazily chunk-indexed DATA stream must be materialized
  // first (GetDatum() is the only accessor that decodes chunks on the fly).
  std::pair<char *, size_t> GetBuffer(size_t position) override;

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  std::pair<char *, size_t> GetOffsetBuffer(bool append_last) override;

  // Lazy per-chunk decode of the DATA stream (chunk index prototype). The
  // OFFSETS stream is kept whole; only the (large) varlena data is chunked.
  Datum GetDatum(size_t position, int null_counts) override;

  int64 GetOriginLength() const override;

  size_t GetAlignSize() const override;

#ifdef BUILD_RB_RET_DICT
  inline std::shared_ptr<DataBuffer<char>> GetUndecodedBuffer() {
    return shared_data_;
  }
#endif

  // `GetNonNullRows` is not overridden because `PaxNonFixedEncodingColumn`
  // has no streaming encoding and `shared_data_` shares its buffer with
  // `PaxNonFixedColumn::data_`. `GetBuffer(position)`/`GetRangeBuffer` are
  // overridden only to materialize a lazily chunk-indexed DATA stream.

 protected:
  void InitEncoder();
  void InitOffsetStreamCompressor();
  void InitDecoder();
  void InitOffsetStreamDecompressor();

  // Decompress DATA chunk chunk_no into chunk_buf_.
  void DecodeChunk(uint32 chunk_no);
  // Fallback: decompress all DATA chunks into data_ and leave chunked mode.
  void MaterializeAll();

 protected:
  PaxEncoder::EncodingOption encoder_options_;
  PaxDecoder::DecodingOption decoder_options_;

  std::shared_ptr<PaxEncoder> encoder_;
  std::shared_ptr<PaxDecoder> decoder_;

  std::shared_ptr<PaxCompressor> compressor_;
  bool compress_route_;
  std::shared_ptr<DataBuffer<char>> shared_data_;

  std::shared_ptr<PaxCompressor> offsets_compressor_;
  // Optional encoder/decoder for offsets stream (alternative to compression)
  std::shared_ptr<PaxEncoder> offsets_encoder_;
  std::shared_ptr<PaxDecoder> offsets_decoder_;
  std::shared_ptr<DataBuffer<char>> shared_offsets_data_;

  // --- chunk index (prototype), read side; DATA stream only ---
  bool chunked_ = false;
  uint32 chunk_rows_ = 0;         // non-null values per chunk
  uint32 chunk_total_rows_ = 0;   // total non-null values
  std::vector<uint64> chunk_ulen_;   // uncompressed byte length of each chunk
  std::vector<uint64> chunk_clen_;   // compressed byte length of each chunk
  std::vector<size_t> chunk_coff_;   // compressed offset (into chunk_src_)
  std::vector<size_t> chunk_ustart_; // uncompressed byte start of each chunk
  std::unique_ptr<DataBuffer<char>> chunk_src_;   // kept compressed DATA bytes
  std::shared_ptr<DataBuffer<char>> chunk_buf_;   // currently decoded chunk
  int cached_chunk_ = -1;
};

}  // namespace pax
