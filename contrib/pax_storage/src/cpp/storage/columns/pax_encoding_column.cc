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
 * pax_encoding_column.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_encoding_column.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_encoding_column.h"

#include "comm/fmt.h"
#include "comm/guc.h"
#include "storage/pax_defined.h"
#include "storage/proto/proto_wrappers.h"

namespace pax {
// Self-describing chunk-index header prepended to a fixed-length column's
// DATA stream when pax.enable_chunk_index is on. Layout:
//   [uint32 magic][uint32 n_chunks][uint32 chunk_rows][uint32 total_rows]
//   [uint64 clen_0 .. clen_{n-1}][compressed chunk 0][chunk 1]...
// clen is uint64 because a column's DATA stream is bounded only by the tuple
// count (PAX_MAX_NUM_TUPLES_PER_FILE), not by any byte cap, so a chunk's
// compressed length can exceed 4GB for wide values.
// The magic differs from the ZSTD frame magic (0xFD2FB528) so a plain stream
// (old tables / non-chunked) is detected and decoded the whole-frame way.
static const uint32 kPaxChunkMagic = 0x50414331;  // "PAC1"
}  // namespace pax
namespace pax {

template <typename T>
PaxEncodingColumn<T>::PaxEncodingColumn(
    uint32 capacity, const PaxEncoder::EncodingOption &encoding_option)
    : PaxCommColumn<T>(capacity),
      encoder_options_(encoding_option),
      encoder_(nullptr),
      decoder_(nullptr),
      shared_data_(nullptr),
      compressor_(nullptr),
      compress_route_(true) {
  InitEncoder();
}

template <typename T>
PaxEncodingColumn<T>::PaxEncodingColumn(
    uint32 capacity, const PaxDecoder::DecodingOption &decoding_option)
    : PaxCommColumn<T>(capacity),
      encoder_(nullptr),
      decoder_options_{decoding_option},
      decoder_(nullptr),
      shared_data_(nullptr),
      compressor_(nullptr),
      compress_route_(false) {
  InitDecoder();
}

template <typename T>
PaxEncodingColumn<T>::~PaxEncodingColumn() { }

template <typename T>
void PaxEncodingColumn<T>::InitEncoder() {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED) {
    encoder_options_.column_encode_type = GetDefaultColumnType();
  }

  PaxColumn::SetEncodeType(encoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(encoder_options_.compress_level);

  // Create a streaming encoder
  // If current `encoded_type_` can not create a streaming encoder,
  // `CreateStreamingEncoder` will return a nullptr. This may be
  // caused by three scenarios:
  //   - `encoded_type_` is not a encoding type.
  //   - `encoded_type_` is a encoding type, but not support it yet.
  //   - `encoded_type_` is no_encoding type.
  //
  // Not allow pass `default`type` of `encoded_type_` into
  // `CreateStreamingEncoder`, caller should change it before create a encoder.
  encoder_ = PaxEncoder::CreateStreamingEncoder(encoder_options_);
  if (encoder_) {
    return;
  }

  // Create a block compressor
  // Compressor have a different interface with pax encoder
  // If no pax encoder no provided, then try to create a compressor.
  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
  if (compressor_) {
    return;
  }

  // can't find any encoder or compressor
  // then should reset encode type
  // or will got origin length is -1 but still have encode type
  PaxColumn::SetEncodeType(ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED);
  PaxColumn::SetCompressLevel(0);
}

template <typename T>
void PaxEncodingColumn<T>::InitDecoder() {
  Assert(decoder_options_.column_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);

  PaxColumn::SetEncodeType(decoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(decoder_options_.compress_level);

  decoder_ = PaxDecoder::CreateDecoder<T>(decoder_options_);
  if (decoder_) {
    // init the shared_data_ with the buffer from PaxCommColumn<T>::data_
    // cause decoder_ need a DataBuffer<char> * as dst buffer
    shared_data_ = std::make_shared<DataBuffer<char>>(*PaxCommColumn<T>::data_);
    decoder_->SetDataBuffer(shared_data_);
    return;
  }

  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
}

template <typename T>
void PaxEncodingColumn<T>::Set(std::unique_ptr<DataBuffer<T>> data) {
  if (decoder_) {
    // should not decoding null
    if (data->Used() != 0) {
      Assert(shared_data_);
      decoder_->SetSrcBuffer(data->Start(), data->Used());
      decoder_->Decoding();

      // `data_` have the same buffer with `shared_data_`
      PaxCommColumn<T>::data_->Brush(shared_data_->Used());
    }

    Assert(!data->IsMemTakeOver());
  } else if (compressor_) {
    if (data->Used() != 0) {
      // should not init `shared_data_`, direct uncompress to `data_`
      Assert(!shared_data_);

      // Chunk-indexed stream? Keep the compressed bytes and decode lazily
      // per chunk in GetDatum(); do not decompress the whole group here.
      if (data->Used() >= 4 * sizeof(uint32) &&
          *reinterpret_cast<const uint32 *>(data->Start()) == kPaxChunkMagic) {
        const uint32 *h = reinterpret_cast<const uint32 *>(data->Start());
        uint32 n_chunks = h[1];
        chunk_rows_ = h[2];
        chunk_total_rows_ = h[3];
        const uint64 *clen_tab = reinterpret_cast<const uint64 *>(h + 4);
        // n_chunks comes from the (untrusted) stream header. The magic cannot
        // legitimately collide with a zstd/lz4 frame header, so a stream that
        // matches the magic but whose declared table/chunks do not fit the
        // received buffer is corrupt/truncated: fall through to whole-frame
        // decompression (which then rejects it) rather than reading past the
        // buffer. All size math is size_t; the multiply cannot overflow because
        // n_chunks is uint32.
        size_t hdr_need = 4 * sizeof(uint32) + (size_t)n_chunks * sizeof(uint64);
        // Reject a degenerate/corrupt header as well: n_chunks or chunk_rows_
        // == 0 would divide by zero / index empty chunk tables in GetDatum().
        // A valid stream always satisfies these (the writer only chunks when
        // nrows > chunk_rows >= 1024).
        if (n_chunks > 0 && chunk_rows_ > 0 && data->Used() >= hdr_need) {
          chunk_clen_.resize(n_chunks);
          chunk_coff_.resize(n_chunks);
          size_t off = hdr_need;
          for (uint32 c = 0; c < n_chunks; c++) {
            chunk_clen_[c] = clen_tab[c];
            chunk_coff_[c] = off;
            off += clen_tab[c];
          }
          // Only adopt the chunked layout if every chunk's compressed bytes are
          // present; otherwise a later DecodeChunk()/MaterializeAll() would read
          // past chunk_src_.
          if (off <= data->Used()) {
            chunk_buf_ = std::make_shared<DataBuffer<char>>(
                (size_t)chunk_rows_ * sizeof(T));
            cached_chunk_ = -1;
            chunk_src_ = std::move(data);
            chunked_ = true;
            return;
          }
        }
        // Not a usable chunk stream -- fall through to whole decompression.
      }

      size_t d_size = compressor_->Decompress(
          PaxCommColumn<T>::data_->Start(), PaxCommColumn<T>::data_->Capacity(),
          data->Start(), data->Used());
      if (compressor_->IsError(d_size)) {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeCompressError,
            fmt("Decompress failed, %s", compressor_->ErrorName(d_size)));
      }

      PaxCommColumn<T>::data_->Brush(d_size);
    }

    Assert(!data->IsMemTakeOver());
  } else {
    PaxCommColumn<T>::Set(std::move(data));
  }
}

template <typename T>
std::pair<char *, size_t> PaxEncodingColumn<T>::GetBuffer() {
  // Whole-buffer accessors (stats/vec/analyze) need the full column; if we are
  // in lazy chunk mode, decompress everything into data_ first.
  if (!compress_route_ && chunked_) MaterializeAll();
  if (compress_route_) {
    // already done with decoding/compress
    if (shared_data_) {
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    // no data for encoding
    if (PaxCommColumn<T>::data_->Used() == 0) {
      return PaxCommColumn<T>::GetBuffer();
    }

    if (encoder_) {
      // changed streaming encode to blocking encode
      // because we still need store a origin data in `PaxCommColumn<T>`
      auto origin_data_buffer = PaxCommColumn<T>::data_.get();

      shared_data_ = std::make_shared<DataBuffer<char>>(origin_data_buffer->Used());
      encoder_->SetDataBuffer(shared_data_);
      for (size_t i = 0; i < origin_data_buffer->GetSize(); i++) {
        encoder_->Append((char *)(origin_data_buffer->GetBuffer() + i),
                         sizeof(T));
      }
      encoder_->Flush();
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    } else if (compressor_) {
      char *src = PaxCommColumn<T>::data_->Start();
      size_t used = PaxCommColumn<T>::data_->Used();
      size_t nrows = used / sizeof(T);

      // Chunk-indexed layout: split the (non-null) values into fixed-row
      // chunks, compress each independently, and prepend an offset table so a
      // random fetch can decompress just one chunk.
      if (pax_enable_chunk_index && used > 0 &&
          nrows > (size_t)pax_chunk_index_rows) {
        uint32 chunk_rows = (uint32)pax_chunk_index_rows;
        uint32 n_chunks = (uint32)((nrows + chunk_rows - 1) / chunk_rows);
        size_t hdr = 4 * sizeof(uint32) + (size_t)n_chunks * sizeof(uint64);
        // Worst case: each chunk's bound + header.
        size_t cap = hdr + compressor_->GetCompressBound(used) +
                     (size_t)n_chunks * 128;
        shared_data_ = std::make_shared<DataBuffer<char>>(cap);
        char *base = shared_data_->Start();
        uint32 h[4] = {kPaxChunkMagic, n_chunks, chunk_rows, (uint32)nrows};
        memcpy(base, h, sizeof(h));
        uint64 *clen_tab = reinterpret_cast<uint64 *>(base + sizeof(h));
        size_t off = hdr;
        for (uint32 c = 0; c < n_chunks; c++) {
          size_t crows =
              (c + 1 < n_chunks) ? chunk_rows : (nrows - (size_t)c * chunk_rows);
          size_t clen = compressor_->Compress(
              base + off, cap - off, src + (size_t)c * chunk_rows * sizeof(T),
              crows * sizeof(T), encoder_options_.compress_level);
          if (compressor_->IsError(clen)) {
            CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                       fmt("Compress failed, %s", compressor_->ErrorName(clen)));
          }
          clen_tab[c] = clen;
          off += clen;
        }
        shared_data_->Brush(off);
        return std::make_pair(shared_data_->Start(), shared_data_->Used());
      }

      size_t bound_size = compressor_->GetCompressBound(used);
      shared_data_ = std::make_shared<DataBuffer<char>>(bound_size);

      size_t c_size =
          compressor_->Compress(shared_data_->Start(), shared_data_->Capacity(),
                                src, used, encoder_options_.compress_level);

      if (compressor_->IsError(c_size)) {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                   fmt("Compress failed, %s", compressor_->ErrorName(c_size)));
      }

      shared_data_->Brush(c_size);
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    // no encoding here, fall through
  }

  return PaxCommColumn<T>::GetBuffer();
}

template <typename T>
int64 PaxEncodingColumn<T>::GetOriginLength() const {
  return PaxCommColumn<T>::data_->Used();
}

template <typename T>
size_t PaxEncodingColumn<T>::PhysicalSize() const {
  if (shared_data_) {
    return shared_data_->Used();
  }

  return PaxCommColumn<T>::PhysicalSize();
}

template <typename T>
size_t PaxEncodingColumn<T>::GetAlignSize() const {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    return PaxColumn::GetAlignSize();
  }

  return PAX_DATA_NO_ALIGN;
}

template <typename T>
ColumnEncoding_Kind PaxEncodingColumn<T>::GetDefaultColumnType() {
  return ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED;
}

template <typename T>
void PaxEncodingColumn<T>::DecodeChunk(uint32 chunk_no) {
  Assert(chunk_no < chunk_clen_.size());
  size_t d = compressor_->Decompress(
      chunk_buf_->Start(), chunk_buf_->Capacity(),
      reinterpret_cast<char *>(chunk_src_->Start()) + chunk_coff_[chunk_no],
      chunk_clen_[chunk_no]);
  if (compressor_->IsError(d)) {
    CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
               fmt("Decompress failed, %s", compressor_->ErrorName(d)));
  }
  cached_chunk_ = (int)chunk_no;
}

template <typename T>
void PaxEncodingColumn<T>::MaterializeAll() {
  if (!chunked_) return;
  size_t n = chunk_clen_.size();
  for (size_t c = 0; c < n; c++) {
    size_t d = compressor_->Decompress(
        PaxCommColumn<T>::data_->Start() + (size_t)c * chunk_rows_ * sizeof(T),
        PaxCommColumn<T>::data_->Capacity() -
            (size_t)c * chunk_rows_ * sizeof(T),
        reinterpret_cast<char *>(chunk_src_->Start()) + chunk_coff_[c],
        chunk_clen_[c]);
    if (compressor_->IsError(d)) {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                 fmt("Decompress failed, %s", compressor_->ErrorName(d)));
    }
  }
  PaxCommColumn<T>::data_->Brush((size_t)chunk_total_rows_ * sizeof(T));
  chunked_ = false;
  chunk_src_ = nullptr;
  chunk_buf_ = nullptr;
  cached_chunk_ = -1;
}

template <typename T>
Datum PaxEncodingColumn<T>::GetDatum(size_t position, int null_counts) {
  if (!chunked_) return PaxCommColumn<T>::GetDatum(position, null_counts);
  size_t data_idx = (null_counts >= 0) ? (position - null_counts) : position;
  uint32 c = (uint32)(data_idx / chunk_rows_);
  if ((int)c != cached_chunk_) DecodeChunk(c);
  size_t local = data_idx - (size_t)c * chunk_rows_;
  auto ptr = chunk_buf_->Start() + sizeof(T) * local;
  return (Datum)(*reinterpret_cast<T *>(ptr));
}

template <typename T>
std::pair<char *, size_t> PaxEncodingColumn<T>::GetBuffer(size_t position) {
  // The vectorized read path fetches values through this per-position accessor,
  // which reads data_ directly. Materialize a lazily chunk-indexed DATA stream
  // first; otherwise data_ is empty and the read returns garbage.
  if (!compress_route_ && chunked_) MaterializeAll();
  return PaxCommColumn<T>::GetBuffer(position);
}

template <typename T>
std::pair<char *, size_t> PaxEncodingColumn<T>::GetRangeBuffer(size_t start_pos,
                                                               size_t len) {
  // Same lazy-chunk contract as GetBuffer(position): the vec adapter sizes its
  // output buffer from this range and reads data_ directly.
  if (!compress_route_ && chunked_) MaterializeAll();
  return PaxCommColumn<T>::GetRangeBuffer(start_pos, len);
}

template class PaxEncodingColumn<int8>;
template class PaxEncodingColumn<int16>;
template class PaxEncodingColumn<int32>;
template class PaxEncodingColumn<int64>;

}  // namespace pax
