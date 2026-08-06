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
 * pax_encoding_non_fixed_column.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_encoding_non_fixed_column.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/columns/pax_encoding_non_fixed_column.h"

#include "comm/fmt.h"
#include "comm/guc.h"
#include "comm/pax_memory.h"
#include "storage/pax_defined.h"

namespace pax {
// Chunk-index magic for a non-fixed column's DATA stream (distinct from the
// fixed-length magic and from the ZSTD frame magic). Layout:
//   [uint32 magic][uint32 n_chunks][uint32 chunk_rows][uint32 total_rows]
//   [per chunk: uint64 ulen, uint64 clen][compressed chunk 0][chunk 1]...
// ulen/clen are uint64: a varlena column's DATA stream is bounded only by the
// tuple count (PAX_MAX_NUM_TUPLES_PER_FILE), not by any byte cap, so a single
// chunk's (un)compressed length can exceed 4GB for wide values.
// Only the DATA (varlena) stream is chunked; the OFFSETS stream is unchanged.
static const uint32 kPaxNfChunkMagic = 0x50414332;  // "PAC2"

void PaxNonFixedEncodingColumn::InitEncoder() {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED) {
    encoder_options_.column_encode_type = ColumnEncoding_Kind_COMPRESS_ZSTD;
    encoder_options_.compress_level = 5;
  }

  PaxColumn::SetEncodeType(encoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(encoder_options_.compress_level);

  encoder_ = PaxEncoder::CreateStreamingEncoder(encoder_options_, true);
  if (encoder_) {
    return;
  }

  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
  if (compressor_) {
    return;
  }

  PaxColumn::SetEncodeType(ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED);
  PaxColumn::SetCompressLevel(0);
}

void PaxNonFixedEncodingColumn::InitOffsetStreamCompressor() {
  Assert(encoder_options_.offsets_encode_type ==
         ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA);

  SetOffsetsEncodeType(encoder_options_.offsets_encode_type);
  SetOffsetsCompressLevel(encoder_options_.offsets_compress_level);

  PaxEncoder::EncodingOption opt = encoder_options_;
  opt.column_encode_type =
      ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA;
  opt.is_sign = false;
  // offsets are fixed-width, do not enable non_fixed streaming restriction
  offsets_encoder_ = PaxEncoder::CreateStreamingEncoder(opt, false);
}

void PaxNonFixedEncodingColumn::InitOffsetStreamDecompressor() {
  Assert(decoder_options_.offsets_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);
  SetOffsetsEncodeType(decoder_options_.offsets_encode_type);
  SetOffsetsCompressLevel(decoder_options_.offsets_compress_level);

  if (decoder_options_.offsets_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA) {
    PaxDecoder::DecodingOption temp_opt = decoder_options_;
    temp_opt.column_encode_type =
        ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA;
    temp_opt.is_sign = false;
    offsets_decoder_ = PaxDecoder::CreateDecoder<int32>(temp_opt);
  } else {
    offsets_compressor_ = PaxCompressor::CreateBlockCompressor(
        decoder_options_.offsets_encode_type);
  }
}

void PaxNonFixedEncodingColumn::InitDecoder() {
  Assert(decoder_options_.column_encode_type !=
         ColumnEncoding_Kind::ColumnEncoding_Kind_DEF_ENCODED);

  PaxColumn::SetEncodeType(decoder_options_.column_encode_type);
  PaxColumn::SetCompressLevel(decoder_options_.compress_level);

  decoder_ = PaxDecoder::CreateDecoder<int8>(decoder_options_);
  if (decoder_) {
    shared_data_ =
        std::make_shared<DataBuffer<char>>(*PaxNonFixedColumn::data_);
    decoder_->SetDataBuffer(shared_data_);
    return;
  }

  compressor_ =
      PaxCompressor::CreateBlockCompressor(PaxColumn::GetEncodingType());
}

PaxNonFixedEncodingColumn::PaxNonFixedEncodingColumn(
    uint32 data_capacity, uint32 offsets_capacity,
    const PaxEncoder::EncodingOption &encoder_options)
    : PaxNonFixedColumn(data_capacity, offsets_capacity),
      encoder_options_(encoder_options),
      encoder_(nullptr),
      decoder_(nullptr),
      compressor_(nullptr),
      compress_route_(true),
      shared_data_(nullptr),
      offsets_compressor_(nullptr),
      shared_offsets_data_(nullptr) {
  InitEncoder();
  InitOffsetStreamCompressor();
}

PaxNonFixedEncodingColumn::PaxNonFixedEncodingColumn(
    uint32 data_capacity, uint32 offsets_capacity,
    const PaxDecoder::DecodingOption &decoding_option)
    : PaxNonFixedColumn(data_capacity, offsets_capacity),
      decoder_options_(decoding_option),
      encoder_(nullptr),
      decoder_(nullptr),
      compressor_(nullptr),
      compress_route_(false),
      shared_data_(nullptr),
      offsets_compressor_(nullptr),
      shared_offsets_data_(nullptr) {
  InitDecoder();
  InitOffsetStreamDecompressor();
}

PaxNonFixedEncodingColumn::~PaxNonFixedEncodingColumn() {}

void PaxNonFixedEncodingColumn::Set(std::unique_ptr<DataBuffer<char>> data,
                                    std::unique_ptr<DataBuffer<int32>> offsets,
                                    size_t total_size) {
  bool exist_decoder;
  Assert(data && offsets);

  auto data_decompress = [&]() {
    Assert(!compress_route_);
    Assert(bool(compressor_) != bool(decoder_));

    if (data->Used() == 0) {
      return;
    }

    // Chunk-indexed DATA stream? Keep the compressed bytes and decode lazily
    // per chunk in GetDatum(); the OFFSETS stream is still decoded whole below.
    if (compressor_ && data->Used() >= 4 * sizeof(uint32) &&
        *reinterpret_cast<const uint32 *>(data->Start()) == kPaxNfChunkMagic) {
      const uint32 *h = reinterpret_cast<const uint32 *>(data->Start());
      uint32 n_chunks = h[1];
      chunk_rows_ = h[2];
      chunk_total_rows_ = h[3];
      const uint64 *tab = reinterpret_cast<const uint64 *>(h + 4);
      // n_chunks comes from the (untrusted) stream header; each chunk has an
      // [ulen,clen] pair (2 uint64). The magic cannot legitimately collide with
      // a zstd/lz4 frame header, so a stream that matches the magic but whose
      // declared table/chunks do not fit the received buffer is
      // corrupt/truncated: fall through to whole-frame decompression (which
      // then rejects it) rather than reading past the buffer. All size math is
      // size_t; the multiply cannot overflow because n_chunks is uint32.
      size_t hdr_need = 4 * sizeof(uint32) + (size_t)n_chunks * 2 * sizeof(uint64);
      // Reject a degenerate/corrupt header as well: n_chunks or chunk_rows_ == 0
      // would divide by zero / index empty chunk tables in GetDatum(). A valid
      // stream always satisfies these (the writer only chunks when
      // nrows > chunk_rows >= 1024).
      if (n_chunks > 0 && chunk_rows_ > 0 && data->Used() >= hdr_need) {
        chunk_ulen_.resize(n_chunks);
        chunk_clen_.resize(n_chunks);
        chunk_coff_.resize(n_chunks);
        chunk_ustart_.resize(n_chunks);
        size_t off = hdr_need, ustart = 0, maxu = 0;
        for (uint32 c = 0; c < n_chunks; c++) {
          chunk_ulen_[c] = tab[2 * c];
          chunk_clen_[c] = tab[2 * c + 1];
          chunk_coff_[c] = off;
          chunk_ustart_[c] = ustart;
          off += chunk_clen_[c];
          ustart += chunk_ulen_[c];
          if (chunk_ulen_[c] > maxu) maxu = chunk_ulen_[c];
        }
        // Only adopt the chunked layout if every chunk's compressed bytes are
        // present; otherwise a later DecodeChunk()/MaterializeAll() would read
        // past chunk_src_.
        if (off <= data->Used()) {
          chunk_buf_ = std::make_shared<DataBuffer<char>>(maxu);
          cached_chunk_ = -1;
          chunk_src_ = std::move(data);
          chunked_ = true;
          return;
        }
      }
      // Not a usable chunk stream -- fall through to whole decompression.
    }

    if (compressor_) {
      auto d_size = compressor_->Decompress(
          PaxNonFixedColumn::data_->Start(),
          PaxNonFixedColumn::data_->Capacity(), data->Start(), data->Used());
      if (compressor_->IsError(d_size)) {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeCompressError,
            fmt("Decompress failed, %s", compressor_->ErrorName(d_size)));
      }
      PaxNonFixedColumn::data_->Brush(d_size);
    }

    if (decoder_) {
      Assert(shared_data_);
      decoder_->SetSrcBuffer(data->Start(), data->Used());
      decoder_->Decoding();

      // `data_` have the same buffer with `shared_data_`
      PaxNonFixedColumn::data_->Brush(shared_data_->Used());
      // no delete the origin data
      shared_data_ = std::move(data);
    }
  };

  auto offsets_decompress = [&]() {
    Assert(!compress_route_);
    Assert(offsets_compressor_ || offsets_decoder_);

    if (offsets->Used() == 0) {
      return;
    }

    if (offsets_compressor_) {
      auto d_size = offsets_compressor_->Decompress(
          PaxNonFixedColumn::offsets_->Start(),
          PaxNonFixedColumn::offsets_->Capacity(), offsets->Start(),
          offsets->Used());
      if (offsets_compressor_->IsError(d_size)) {
        CBDB_RAISE(
            cbdb::CException::ExType::kExTypeCompressError,
            fmt("Decompress failed, %s", compressor_->ErrorName(d_size)));
      }
      PaxNonFixedColumn::offsets_->Brush(d_size);
      return;
    }

    if (offsets_decoder_) {
      // Decode offsets using encoder for int32 stream
      shared_offsets_data_ = std::make_shared<DataBuffer<char>>(
          PaxNonFixedColumn::offsets_->Start(),
          PaxNonFixedColumn::offsets_->Capacity(), false, false);
      offsets_decoder_->SetDataBuffer(shared_offsets_data_);
      offsets_decoder_->SetSrcBuffer(offsets->Start(), offsets->Used());
      offsets_decoder_->Decoding();
      PaxNonFixedColumn::offsets_->Brush(shared_offsets_data_->Used());
      return;
    }
  };

  exist_decoder = compressor_ || decoder_;
  bool has_offsets_processor = offsets_compressor_ || offsets_decoder_;

  if (exist_decoder && has_offsets_processor) {
    data_decompress();
    offsets_decompress();
    PaxNonFixedColumn::estimated_size_ = total_size;
    PaxNonFixedColumn::next_offsets_ = -1;
  } else if (exist_decoder && !has_offsets_processor) {
    data_decompress();
    PaxNonFixedColumn::offsets_ = std::move(offsets);
    PaxNonFixedColumn::estimated_size_ = total_size;
    PaxNonFixedColumn::next_offsets_ = -1;
  } else if (!exist_decoder && has_offsets_processor) {
    PaxNonFixedColumn::data_ = std::move(data);
    offsets_decompress();
    PaxNonFixedColumn::estimated_size_ = total_size;
    PaxNonFixedColumn::next_offsets_ = -1;
  } else {  // (!compressor_ && !offsets_compressor_)
    PaxNonFixedColumn::Set(std::move(data), std::move(offsets), total_size);
  }
}

std::pair<char *, size_t> PaxNonFixedEncodingColumn::GetBuffer() {
  // Whole-buffer accessors need the full column; materialize lazy chunks first.
  if (!compress_route_ && chunked_) MaterializeAll();

  bool exist_encoder;
  exist_encoder = compressor_ || encoder_;

  if (exist_encoder && compress_route_) {
    Assert(!compressor_ || !encoder_);

    // already compressed
    if (shared_data_) {
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    if (PaxNonFixedColumn::data_->Used() == 0) {
      return PaxNonFixedColumn::GetBuffer();
    }

    // do compressed
    if (compressor_) {
      char *src = PaxNonFixedColumn::data_->Start();
      size_t total_bytes = PaxNonFixedColumn::data_->Used();
      // Use the real non-null value count. offsets_->GetSize() carries an extra
      // trailing offset (== total_bytes) when next_offsets_ == -1, so using it
      // directly adds a phantom final value: when the count is an exact
      // multiple of chunk_rows (e.g. a full 131072-row group with the default
      // 8192 chunk size) it produces a trailing zero-length chunk whose
      // Compress(src_len=0) trips an assertion / writes a bogus chunk.
      size_t nrows = GetNonNullRows();

      // Chunk-indexed DATA layout: split the varlena bytes at row boundaries
      // (chunk_rows values per chunk), compress each independently, and prepend
      // an [ulen,clen] table so a random fetch decompresses just one chunk.
      if (pax_enable_chunk_index && total_bytes > 0 &&
          nrows > (size_t)pax_chunk_index_rows) {
        uint32 chunk_rows = (uint32)pax_chunk_index_rows;
        uint32 n_chunks = (uint32)((nrows + chunk_rows - 1) / chunk_rows);
        size_t hdr = 4 * sizeof(uint32) + (size_t)n_chunks * 2 * sizeof(uint64);
        size_t cap =
            hdr + compressor_->GetCompressBound(total_bytes) + n_chunks * 128;
        shared_data_ = std::make_shared<DataBuffer<char>>(cap);
        char *base = shared_data_->Start();
        uint32 h[4] = {kPaxNfChunkMagic, n_chunks, chunk_rows, (uint32)nrows};
        memcpy(base, h, sizeof(h));
        uint64 *tab = reinterpret_cast<uint64 *>(base + sizeof(h));
        size_t off = hdr;
        for (uint32 c = 0; c < n_chunks; c++) {
          size_t rs = (size_t)c * chunk_rows;
          size_t re = rs + chunk_rows < nrows ? rs + chunk_rows : nrows;
          size_t bstart = (*offsets_)[rs];
          size_t bend = (re < nrows) ? (size_t)(*offsets_)[re] : total_bytes;
          size_t ulen = bend - bstart;
          size_t clen = compressor_->Compress(base + off, cap - off,
                                              src + bstart, ulen,
                                              encoder_options_.compress_level);
          if (compressor_->IsError(clen)) {
            CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                       fmt("Compress failed, %s", compressor_->ErrorName(clen)));
          }
          tab[2 * c] = ulen;
          tab[2 * c + 1] = clen;
          off += clen;
        }
        shared_data_->Brush(off);
        return std::make_pair(shared_data_->Start(), shared_data_->Used());
      }

      size_t bound_size = compressor_->GetCompressBound(total_bytes);
      shared_data_ = std::make_shared<DataBuffer<char>>(bound_size);

      auto c_size = compressor_->Compress(
          shared_data_->Start(), shared_data_->Capacity(), src, total_bytes,
          encoder_options_.compress_level);

      if (compressor_->IsError(c_size)) {
        // log error with `compressor_->ErrorName(d_size)`
        CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                   fmt("Compress failed, %s", compressor_->ErrorName(c_size)));
      }

      shared_data_->Brush(c_size);
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    if (encoder_) {
      shared_data_ =
          std::make_shared<DataBuffer<char>>(PaxNonFixedColumn::data_->Used());
      encoder_->SetDataBuffer(shared_data_);

      char *data_buffer = PaxNonFixedColumn::data_->GetBuffer();

      for (size_t i = 0; i < offsets_->GetSize() - 1; i++) {
        encoder_->Append(data_buffer, (*offsets_)[i + 1] - (*offsets_)[i]);
        data_buffer += (*offsets_)[i + 1] - (*offsets_)[i];
      }

      if (next_offsets_ != -1) {
        encoder_->Append(data_buffer,
                         next_offsets_ - (*offsets_)[offsets_->GetSize() - 1]);
      }

      encoder_->Flush();
      return std::make_pair(shared_data_->Start(), shared_data_->Used());
    }

    // no encoding here, fall through
  }

  // no compress or uncompressed
  return PaxNonFixedColumn::GetBuffer();
}

std::pair<char *, size_t> PaxNonFixedEncodingColumn::GetBuffer(size_t position) {
  // The vectorized read path fetches varlena values through this per-position
  // accessor, which reads data_ directly. Materialize a lazily chunk-indexed
  // DATA stream first; otherwise data_ is empty and the read returns garbage.
  if (!compress_route_ && chunked_) MaterializeAll();
  return PaxNonFixedColumn::GetBuffer(position);
}

std::pair<char *, size_t> PaxNonFixedEncodingColumn::GetRangeBuffer(
    size_t start_pos, size_t len) {
  // Same lazy-chunk contract as GetBuffer(position): the vec adapter sizes its
  // output buffer from this range and reads data_ directly.
  if (!compress_route_ && chunked_) MaterializeAll();
  return PaxNonFixedColumn::GetRangeBuffer(start_pos, len);
}

std::pair<char *, size_t> PaxNonFixedEncodingColumn::GetOffsetBuffer(
    bool append_last) {
  if (append_last) {
    AppendLastOffset();
  }

  if (shared_offsets_data_) {
    return std::make_pair(shared_offsets_data_->Start(),
                          shared_offsets_data_->Used());
  }

  if (PaxNonFixedColumn::offsets_->Used() == 0) {
    // should never append last offset again
    return PaxNonFixedColumn::GetOffsetBuffer(false);
  }

  if (offsets_compressor_ && compress_route_) {
    size_t bound_size = offsets_compressor_->GetCompressBound(
        PaxNonFixedColumn::offsets_->Used());
    shared_offsets_data_ = std::make_shared<DataBuffer<char>>(bound_size);

    auto c_size = offsets_compressor_->Compress(
        shared_offsets_data_->Start(), shared_offsets_data_->Capacity(),
        PaxNonFixedColumn::offsets_->Start(),
        PaxNonFixedColumn::offsets_->Used(), encoder_options_.compress_level);

    if (offsets_compressor_->IsError(c_size)) {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                 fmt("Compress failed, %s", compressor_->ErrorName(c_size)));
    }

    shared_offsets_data_->Brush(c_size);
    return std::make_pair(shared_offsets_data_->Start(),
                          shared_offsets_data_->Used());
  }

  if (offsets_encoder_ && compress_route_) {
    // For delta encoder, allocate a buffer sized by raw bytes for safety
    size_t bound_size = offsets_encoder_->GetBoundSize(offsets_->Used());
    shared_offsets_data_ = std::make_shared<DataBuffer<char>>(bound_size);
    offsets_encoder_->SetDataBuffer(shared_offsets_data_);

    // Encode entire offsets buffer as a single stream
    offsets_encoder_->Append(offsets_->Start(), offsets_->Used());
    offsets_encoder_->Flush();

    return std::make_pair(shared_offsets_data_->Start(),
                          shared_offsets_data_->Used());
  }

  // no compress or uncompressed
  // should never append last offset again
  return PaxNonFixedColumn::GetOffsetBuffer(false);
}

int64 PaxNonFixedEncodingColumn::GetOriginLength() const {
  return PaxNonFixedColumn::data_->Used();
}

size_t PaxNonFixedEncodingColumn::GetAlignSize() const {
  if (encoder_options_.column_encode_type ==
      ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED) {
    return PaxColumn::GetAlignSize();
  }

  return PAX_DATA_NO_ALIGN;
}

void PaxNonFixedEncodingColumn::DecodeChunk(uint32 chunk_no) {
  Assert(chunk_no < chunk_clen_.size());
  size_t d = compressor_->Decompress(chunk_buf_->Start(), chunk_buf_->Capacity(),
                                     chunk_src_->Start() + chunk_coff_[chunk_no],
                                     chunk_clen_[chunk_no]);
  if (compressor_->IsError(d)) {
    CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
               fmt("Decompress failed, %s", compressor_->ErrorName(d)));
  }
  cached_chunk_ = (int)chunk_no;
}

void PaxNonFixedEncodingColumn::MaterializeAll() {
  if (!chunked_) return;
  size_t total_u = 0;
  for (size_t c = 0; c < chunk_clen_.size(); c++) {
    size_t d = compressor_->Decompress(
        PaxNonFixedColumn::data_->Start() + chunk_ustart_[c],
        PaxNonFixedColumn::data_->Capacity() - chunk_ustart_[c],
        chunk_src_->Start() + chunk_coff_[c], chunk_clen_[c]);
    if (compressor_->IsError(d)) {
      CBDB_RAISE(cbdb::CException::ExType::kExTypeCompressError,
                 fmt("Decompress failed, %s", compressor_->ErrorName(d)));
    }
    total_u += chunk_ulen_[c];
  }
  PaxNonFixedColumn::data_->Brush(total_u);
  chunked_ = false;
  chunk_src_ = nullptr;
  chunk_buf_ = nullptr;
  cached_chunk_ = -1;
}

Datum PaxNonFixedEncodingColumn::GetDatum(size_t position, int null_counts) {
  if (!chunked_) return PaxNonFixedColumn::GetDatum(position, null_counts);

  // Toast values need the full detoast path; fall back for them (rare).
  if (unlikely(IsToast(position))) {
    MaterializeAll();
    return PaxNonFixedColumn::GetDatum(position, null_counts);
  }

  size_t data_idx = (null_counts >= 0) ? (position - null_counts) : position;
  uint32 c = (uint32)(data_idx / chunk_rows_);
  if ((int)c != cached_chunk_) DecodeChunk(c);
  size_t local = (size_t)(*offsets_)[data_idx] - chunk_ustart_[c];
  return PointerGetDatum(chunk_buf_->Start() + local);
}

}  // namespace pax
