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
 * pax_encoding_column.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_encoding_column.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "storage/columns/pax_columns.h"
#include "storage/columns/pax_compress.h"
#include "storage/columns/pax_decoding.h"
#include "storage/columns/pax_encoding.h"

namespace pax {

template <typename T>
class PaxEncodingColumn : public PaxCommColumn<T> {
 public:
  PaxEncodingColumn(uint32 capacity,
                    const PaxEncoder::EncodingOption &encoding_option);

  PaxEncodingColumn(uint32 capacity,
                    const PaxDecoder::DecodingOption &decoding_option);

  ~PaxEncodingColumn() override;

  void Set(std::unique_ptr<DataBuffer<T>> data) override;

  std::pair<char *, size_t> GetBuffer() override;

  // Per-position and range accessors used by the vectorized read path read
  // data_ directly, so a lazily chunk-indexed DATA stream must be materialized
  // first (GetDatum() is the only accessor that decodes chunks on the fly).
  std::pair<char *, size_t> GetBuffer(size_t position) override;

  std::pair<char *, size_t> GetRangeBuffer(size_t start_pos,
                                           size_t len) override;

  // Lazy per-chunk decode when the DATA stream is chunk-indexed (prototype).
  Datum GetDatum(size_t position, int null_counts) override;

  int64 GetOriginLength() const override;

  size_t PhysicalSize() const override;

  size_t GetAlignSize() const override;

 protected:
  void InitEncoder();

  void InitDecoder();

  virtual ColumnEncoding_Kind GetDefaultColumnType();

  // Decompress a single chunk into chunk_buf_ (chunk index prototype).
  void DecodeChunk(uint32 chunk_no);
  // Fallback: decompress all chunks into data_ and leave the chunked mode.
  void MaterializeAll();

 protected:
  PaxEncoder::EncodingOption encoder_options_;
  std::shared_ptr<PaxEncoder> encoder_;

  PaxDecoder::DecodingOption decoder_options_;
  std::shared_ptr<PaxDecoder> decoder_;
  std::shared_ptr<DataBuffer<char>> shared_data_;

  std::shared_ptr<PaxCompressor> compressor_;
  bool compress_route_;

  // --- chunk index (prototype), read side only ---
  bool chunked_ = false;
  uint32 chunk_rows_ = 0;         // non-null values per chunk
  uint32 chunk_total_rows_ = 0;   // total non-null values in the stream
  std::vector<uint64> chunk_clen_;  // compressed length of each chunk
  std::vector<size_t> chunk_coff_;  // compressed offset of each chunk
  std::unique_ptr<DataBuffer<T>> chunk_src_;    // kept compressed bytes
  std::shared_ptr<DataBuffer<char>> chunk_buf_;  // currently decoded chunk
  int cached_chunk_ = -1;
};

extern template class PaxEncodingColumn<int8>;
extern template class PaxEncodingColumn<int16>;
extern template class PaxEncodingColumn<int32>;
extern template class PaxEncodingColumn<int64>;

}  // namespace pax
