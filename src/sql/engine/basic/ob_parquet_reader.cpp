/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_parquet_reader.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <new>

namespace oceanbase
{
namespace sql
{

namespace
{

// ------------------------- Thrift compact protocol -------------------------

enum ThriftType
{
  TT_STOP = 0,
  TT_BOOLEAN_TRUE = 1,
  TT_BOOLEAN_FALSE = 2,
  TT_BYTE = 3,
  TT_I16 = 4,
  TT_I32 = 5,
  TT_I64 = 6,
  TT_DOUBLE = 7,
  TT_BINARY = 8,
  TT_LIST = 9,
  TT_SET = 10,
  TT_MAP = 11,
  TT_STRUCT = 12,
};

class ThriftReader
{
public:
  ThriftReader() : buf_(nullptr), pos_(0), len_(0) {}
  void init(const char *buf, int64_t len)
  {
    buf_ = buf;
    pos_ = 0;
    len_ = len;
  }
  bool ok() const { return OB_NOT_NULL(buf_) && pos_ <= len_; }
  int64_t pos() const { return pos_; }
  bool eof() const { return pos_ >= len_; }

  int read_byte(uint8_t &v)
  {
    int ret = OB_SUCCESS;
    if (pos_ >= len_) {
      ret = OB_INVALID_DATA;
    } else {
      v = static_cast<uint8_t>(buf_[pos_++]);
    }
    return ret;
  }

  int read_varint(uint64_t &v)
  {
    int ret = OB_SUCCESS;
    v = 0;
    int shift = 0;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      uint8_t b = 0;
      if (OB_FAIL(read_byte(b))) {
      } else if (shift > 63) {
        ret = OB_INVALID_DATA;
      } else {
        v |= (static_cast<uint64_t>(b & 0x7f) << shift);
        if (0 == (b & 0x80)) {
          done = true;
        }
        shift += 7;
      }
    }
    return ret;
  }

  int read_zigzag(int64_t &v)
  {
    int ret = OB_SUCCESS;
    uint64_t u = 0;
    if (OB_FAIL(read_varint(u))) {
    } else {
      v = static_cast<int64_t>((u >> 1) ^ (~(u & 1) + 1));
    }
    return ret;
  }

  int read_binary(const char *&ptr, int32_t &len)
  {
    int ret = OB_SUCCESS;
    uint64_t n = 0;
    if (OB_FAIL(read_varint(n))) {
    } else if (n > static_cast<uint64_t>(len_ - pos_)) {
      ret = OB_INVALID_DATA;
    } else {
      ptr = buf_ + pos_;
      len = static_cast<int32_t>(n);
      pos_ += n;
    }
    return ret;
  }

  int skip(uint8_t type)
  {
    int ret = OB_SUCCESS;
    switch (type) {
      case TT_BOOLEAN_TRUE:
      case TT_BOOLEAN_FALSE:
        break;
      case TT_BYTE: {
        uint8_t b = 0;
        ret = read_byte(b);
        break;
      }
      case TT_I16:
      case TT_I32:
      case TT_I64: {
        uint64_t v = 0;
        ret = read_varint(v);
        break;
      }
      case TT_DOUBLE: {
        if (pos_ + 8 > len_) {
          ret = OB_INVALID_DATA;
        } else {
          pos_ += 8;
        }
        break;
      }
      case TT_BINARY: {
        const char *p = nullptr;
        int32_t l = 0;
        ret = read_binary(p, l);
        break;
      }
      case TT_LIST:
      case TT_SET: {
        uint8_t header = 0;
        uint64_t size = 0;
        if (OB_FAIL(read_byte(header))) {
        } else {
          size = header >> 4;
          const uint8_t elem_type = header & 0x0f;
          if (15 == size && OB_FAIL(read_varint(size))) {
          } else {
            for (uint64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
              ret = skip(elem_type);
            }
          }
        }
        break;
      }
      case TT_MAP: {
        uint64_t size = 0;
        if (OB_FAIL(read_varint(size))) {
        } else if (0 == size) {
          // empty map has no type byte
        } else {
          uint8_t kv = 0;
          if (OB_FAIL(read_byte(kv))) {
          } else {
            for (uint64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
              ret = skip(kv >> 4);
              if (OB_SUCC(ret)) {
                ret = skip(kv & 0x0f);
              }
            }
          }
        }
        break;
      }
      case TT_STRUCT: {
        int16_t last_id = 0;
        bool done = false;
        while (OB_SUCC(ret) && !done) {
          uint8_t header = 0;
          if (OB_FAIL(read_byte(header))) {
          } else if (TT_STOP == header) {
            done = true;
          } else {
            int16_t id = static_cast<int16_t>(header >> 4);
            const uint8_t type = header & 0x0f;
            if (0 == id) {
              int64_t explicit_id = 0;
              if (OB_FAIL(read_zigzag(explicit_id))) {
              } else {
                id = static_cast<int16_t>(explicit_id);
              }
            } else {
              id = static_cast<int16_t>(last_id + id);
            }
            last_id = id;
            if (OB_SUCC(ret)) {
              ret = skip(type);
            }
          }
        }
        break;
      }
      default:
        ret = OB_INVALID_DATA;
        break;
    }
    return ret;
  }

private:
  const char *buf_;
  int64_t pos_;
  int64_t len_;
};

// Iterates the fields of one struct.
class ThriftStructReader
{
public:
  explicit ThriftStructReader(ThriftReader &r) : r_(r), last_id_(0) {}
  // Returns OB_SUCCESS with type == TT_STOP at the end of the struct.
  int next(int16_t &field_id, uint8_t &type)
  {
    int ret = OB_SUCCESS;
    uint8_t header = 0;
    if (OB_FAIL(r_.read_byte(header))) {
    } else if (TT_STOP == header) {
      field_id = 0;
      type = TT_STOP;
    } else {
      int16_t id = static_cast<int16_t>(header >> 4);
      type = header & 0x0f;
      if (0 == id) {
        int64_t explicit_id = 0;
        if (OB_FAIL(r_.read_zigzag(explicit_id))) {
        } else {
          id = static_cast<int16_t>(explicit_id);
        }
      } else {
        id = static_cast<int16_t>(last_id_ + id);
      }
      last_id_ = id;
      field_id = id;
    }
    return ret;
  }
private:
  ThriftReader &r_;
  int16_t last_id_;
};

int read_list_header(ThriftReader &r, uint8_t &elem_type, uint64_t &size)
{
  int ret = OB_SUCCESS;
  uint8_t header = 0;
  if (OB_FAIL(r.read_byte(header))) {
  } else {
    size = header >> 4;
    elem_type = header & 0x0f;
    if (15 == size) {
      ret = r.read_varint(size);
    }
  }
  return ret;
}

int read_bool(ThriftReader &r, uint8_t type, bool &v)
{
  int ret = OB_SUCCESS;
  if (TT_BOOLEAN_TRUE == type) {
    v = true;
  } else if (TT_BOOLEAN_FALSE == type) {
    v = false;
  } else if (TT_BYTE == type) {
    uint8_t b = 0;
    if (OB_FAIL(r.read_byte(b))) {
    } else {
      v = (0 != b);
    }
  } else {
    ret = OB_INVALID_DATA;
  }
  return ret;
}

// ------------------------- Parquet metadata -------------------------

struct ParquetSchemaElement
{
  ParquetSchemaElement()
    : type_(-1), repetition_(-1), converted_type_(-1), num_children_(0), max_def_level_(0) {}
  int32_t type_;
  int32_t repetition_;
  int32_t converted_type_;
  int32_t num_children_;
  int32_t max_def_level_;
  common::ObString name_;
};

struct ParquetColumnChunk
{
  ParquetColumnChunk()
    : type_(-1), codec_(0), num_values_(0), data_page_offset_(0),
      dictionary_page_offset_(-1), total_compressed_size_(0), file_offset_(0),
      schema_index_(-1) {}
  int32_t type_;
  int32_t codec_;
  int64_t num_values_;
  int64_t data_page_offset_;
  int64_t dictionary_page_offset_;
  int64_t total_compressed_size_;
  int64_t file_offset_;
  int32_t schema_index_;
};

struct ParquetRowGroup
{
  ParquetRowGroup() : num_rows_(0) {}
  int64_t num_rows_;
  common::ObSEArray<ParquetColumnChunk, 8> columns_;
};

struct ParquetFileMeta
{
  ParquetFileMeta() : num_rows_(0) {}
  int64_t num_rows_;
  common::ObSEArray<ParquetSchemaElement, 16> schema_;
  common::ObSEArray<ParquetRowGroup, 4> row_groups_;
};

int parse_schema_element(ThriftReader &r, ParquetSchemaElement &elem)
{
  int ret = OB_SUCCESS;
  ThriftStructReader sr(r);
  int16_t field_id = 0;
  uint8_t type = 0;
  while (OB_SUCC(ret) && OB_SUCC(sr.next(field_id, type)) && TT_STOP != type) {
    switch (field_id) {
      case 1: {
        int64_t v = 0;
        ret = r.read_zigzag(v);
        elem.type_ = static_cast<int32_t>(v);
        break;
      }
      case 2: {
        int64_t v = 0;
        ret = r.read_zigzag(v);
        break;
      }
      case 3: {
        int64_t v = 0;
        ret = r.read_zigzag(v);
        elem.repetition_ = static_cast<int32_t>(v);
        break;
      }
      case 4: {
        const char *p = nullptr;
        int32_t l = 0;
        if (OB_FAIL(r.read_binary(p, l))) {
        } else {
          elem.name_.assign_ptr(p, l);
        }
        break;
      }
      case 5: {
        int64_t v = 0;
        ret = r.read_zigzag(v);
        elem.num_children_ = static_cast<int32_t>(v);
        break;
      }
      case 6: {
        int64_t v = 0;
        ret = r.read_zigzag(v);
        elem.converted_type_ = static_cast<int32_t>(v);
        break;
      }
      default:
        ret = r.skip(type);
        break;
    }
  }
  return ret;
}

int parse_column_meta_data(ThriftReader &r, ParquetColumnChunk &chunk)
{
  int ret = OB_SUCCESS;
  ThriftStructReader sr(r);
  int16_t field_id = 0;
  uint8_t type = 0;
  while (OB_SUCC(ret) && OB_SUCC(sr.next(field_id, type)) && TT_STOP != type) {
    switch (field_id) {
      case 1: {
        int64_t v = 0;
        if (OB_FAIL(r.read_zigzag(v))) {
        } else {
          chunk.type_ = static_cast<int32_t>(v);
        }
        break;
      }
      case 4: {
        int64_t v = 0;
        if (OB_FAIL(r.read_zigzag(v))) {
        } else {
          chunk.codec_ = static_cast<int32_t>(v);
        }
        break;
      }
      case 5: {
        int64_t v = 0;
        if (OB_FAIL(r.read_zigzag(v))) {
        } else {
          chunk.num_values_ = v;
        }
        break;
      }
      case 7: {
        int64_t v = 0;
        if (OB_FAIL(r.read_zigzag(v))) {
        } else {
          chunk.total_compressed_size_ = v;
        }
        break;
      }
      case 9: {
        int64_t v = 0;
        if (OB_FAIL(r.read_zigzag(v))) {
        } else {
          chunk.data_page_offset_ = v;
        }
        break;
      }
      case 11: {
        int64_t v = 0;
        if (OB_FAIL(r.read_zigzag(v))) {
        } else {
          chunk.dictionary_page_offset_ = v;
        }
        break;
      }
      default:
        ret = r.skip(type);
        break;
    }
  }
  return ret;
}

int parse_column_chunk(ThriftReader &r, ParquetColumnChunk &chunk)
{
  int ret = OB_SUCCESS;
  ThriftStructReader sr(r);
  int16_t field_id = 0;
  uint8_t type = 0;
  while (OB_SUCC(ret) && OB_SUCC(sr.next(field_id, type)) && TT_STOP != type) {
    switch (field_id) {
      case 2: {
        int64_t v = 0;
        if (OB_FAIL(r.read_zigzag(v))) {
        } else {
          chunk.file_offset_ = v;
        }
        break;
      }
      case 3: {
        ret = parse_column_meta_data(r, chunk);
        break;
      }
      default:
        ret = r.skip(type);
        break;
    }
  }
  return ret;
}

int parse_row_group(ThriftReader &r, ParquetRowGroup &row_group)
{
  int ret = OB_SUCCESS;
  ThriftStructReader sr(r);
  int16_t field_id = 0;
  uint8_t type = 0;
  while (OB_SUCC(ret) && OB_SUCC(sr.next(field_id, type)) && TT_STOP != type) {
    if (1 == field_id) {
      uint8_t elem_type = 0;
      uint64_t size = 0;
      if (OB_FAIL(read_list_header(r, elem_type, size))) {
      } else {
        for (uint64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          ParquetColumnChunk chunk;
          if (OB_FAIL(parse_column_chunk(r, chunk))) {
          } else if (OB_FAIL(row_group.columns_.push_back(chunk))) {
          }
        }
      }
    } else if (3 == field_id) {
      int64_t v = 0;
      if (OB_FAIL(r.read_zigzag(v))) {
      } else {
        row_group.num_rows_ = v;
      }
    } else {
      ret = r.skip(type);
    }
  }
  return ret;
}

int parse_file_meta_data(const char *buf, int64_t len, ParquetFileMeta &meta)
{
  int ret = OB_SUCCESS;
  ThriftReader r;
  r.init(buf, len);
  ThriftStructReader sr(r);
  int16_t field_id = 0;
  uint8_t type = 0;
  while (OB_SUCC(ret) && OB_SUCC(sr.next(field_id, type)) && TT_STOP != type) {
    if (2 == field_id) {
      uint8_t elem_type = 0;
      uint64_t size = 0;
      if (OB_FAIL(read_list_header(r, elem_type, size))) {
      } else {
        for (uint64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          ParquetSchemaElement elem;
          if (OB_FAIL(parse_schema_element(r, elem))) {
          } else if (OB_FAIL(meta.schema_.push_back(elem))) {
          }
        }
      }
    } else if (3 == field_id) {
      int64_t v = 0;
      if (OB_FAIL(r.read_zigzag(v))) {
      } else {
        meta.num_rows_ = v;
      }
    } else if (4 == field_id) {
      uint8_t elem_type = 0;
      uint64_t size = 0;
      if (OB_FAIL(read_list_header(r, elem_type, size))) {
      } else {
        for (uint64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          ParquetRowGroup rg;
          if (OB_FAIL(parse_row_group(r, rg))) {
          } else if (OB_FAIL(meta.row_groups_.push_back(rg))) {
          }
        }
      }
    } else {
      ret = r.skip(type);
    }
  }
  return ret;
}

// Compute max definition levels and map leaves to their schema position.
int build_leaf_index(ParquetFileMeta &meta, common::ObIArray<int32_t> &leaves)
{
  int ret = OB_SUCCESS;
  int32_t max_def = 0;
  int64_t idx = 0;
  if (meta.schema_.empty()) {
    ret = OB_INVALID_DATA;
  } else {
    // schema_[0] is the root; the leaves are the elements with type_ >= 0,
    // in depth-first order, which matches the column chunk order.
    for (int64_t i = 0; OB_SUCC(ret) && i < meta.schema_.count(); ++i) {
      ParquetSchemaElement &elem = meta.schema_.at(i);
      if (elem.type_ >= 0) {
        if (elem.repetition_ == 1) {
          ++max_def;
        }
        elem.max_def_level_ = max_def--;
        // NOTE: nested schemas are not handled; all supported files are flat.
        if (OB_FAIL(leaves.push_back(static_cast<int32_t>(i)))) {
        }
      }
    }
    idx = 0;
    UNUSED(idx);
  }
  return ret;
}

bool is_leaf(const ParquetSchemaElement &elem) { return elem.type_ >= 0; }

int map_parquet_type(const ParquetSchemaElement &elem, FileColumnType &type)
{
  int ret = OB_SUCCESS;
  switch (elem.type_) {
    case 0: // BOOLEAN
      type = FILE_COL_BOOLEAN;
      break;
    case 1: // INT32
      type = (6 == elem.converted_type_) ? FILE_COL_DATE : FILE_COL_BIGINT;
      break;
    case 2: // INT64
      type = (9 == elem.converted_type_ || 10 == elem.converted_type_)
                 ? FILE_COL_DATETIME : FILE_COL_BIGINT;
      break;
    case 3: // INT96
      type = FILE_COL_DATETIME;
      break;
    case 4: // FLOAT
    case 5: // DOUBLE
      type = FILE_COL_DOUBLE;
      break;
    case 6: // BYTE_ARRAY
    case 7: // FIXED_LEN_BYTE_ARRAY
      type = FILE_COL_VARCHAR;
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported parquet physical type", K(elem.type_), K(ret));
      break;
  }
  return ret;
}

// ------------------------- snappy decompression -------------------------

int snappy_decompress(const char *src, int64_t src_len,
                      char *dst, int64_t dst_len, int64_t &written)
{
  int ret = OB_SUCCESS;
  int64_t s = 0;
  int64_t d = 0;
  // preamble: uncompressed length (varint)
  uint64_t expected = 0;
  int shift = 0;
  bool done = false;
  while (OB_SUCC(ret) && !done && s < src_len) {
    const uint8_t b = static_cast<uint8_t>(src[s++]);
    expected |= (static_cast<uint64_t>(b & 0x7f) << shift);
    done = (0 == (b & 0x80));
    shift += 7;
  }
  if (OB_SUCC(ret) && expected > static_cast<uint64_t>(dst_len)) {
    ret = OB_INVALID_DATA;
  }
  while (OB_SUCC(ret) && s < src_len) {
    const uint8_t tag = static_cast<uint8_t>(src[s++]);
    const int type = tag & 0x03;
    if (0 == type) {
      int64_t len = (tag >> 2) + 1;
      if (len > 60) {
        const int extra = static_cast<int>(len) - 60;
        len = 0;
        for (int i = 0; OB_SUCC(ret) && i < extra; ++i) {
          if (s >= src_len) {
            ret = OB_INVALID_DATA;
          } else {
            len |= (static_cast<int64_t>(static_cast<uint8_t>(src[s++])) << (8 * i));
          }
        }
        len += 1;
      }
      if (OB_SUCC(ret)) {
        if (s + len > src_len || d + len > dst_len) {
          ret = OB_INVALID_DATA;
        } else {
          MEMCPY(dst + d, src + s, len);
          s += len;
          d += len;
        }
      }
    } else {
      int64_t len = 0;
      int64_t offset = 0;
      if (1 == type) {
        if (s >= src_len) {
          ret = OB_INVALID_DATA;
        } else {
          len = ((tag >> 2) & 0x07) + 4;
          offset = ((static_cast<int64_t>(tag >> 5) & 0x07) << 8) |
                   static_cast<uint8_t>(src[s++]);
        }
      } else if (2 == type) {
        if (s + 2 > src_len) {
          ret = OB_INVALID_DATA;
        } else {
          len = (tag >> 2) + 1;
          offset = static_cast<uint8_t>(src[s]) |
                   (static_cast<int64_t>(static_cast<uint8_t>(src[s + 1])) << 8);
          s += 2;
        }
      } else {
        if (s + 4 > src_len) {
          ret = OB_INVALID_DATA;
        } else {
          len = (tag >> 2) + 1;
          offset = static_cast<uint8_t>(src[s]) |
                   (static_cast<int64_t>(static_cast<uint8_t>(src[s + 1])) << 8) |
                   (static_cast<int64_t>(static_cast<uint8_t>(src[s + 2])) << 16) |
                   (static_cast<int64_t>(static_cast<uint8_t>(src[s + 3])) << 24);
          s += 4;
        }
      }
      if (OB_SUCC(ret)) {
        if (offset <= 0 || offset > d || d + len > dst_len) {
          ret = OB_INVALID_DATA;
        } else {
          for (int64_t i = 0; i < len; ++i) {
            dst[d + i] = dst[d - offset + i];
          }
          d += len;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    written = d;
  }
  return ret;
}

// ------------------------- RLE / bit-packed hybrid -------------------------

class RleBitPackedDecoder
{
public:
  RleBitPackedDecoder() : ptr_(nullptr), end_(nullptr), bit_width_(0), cur_run_len_(0) {}
  void init(const char *ptr, int64_t len, int bit_width)
  {
    ptr_ = ptr;
    end_ = ptr + len;
    bit_width_ = bit_width;
    cur_run_len_ = 0;
  }
  int get(uint64_t &value)
  {
    int ret = OB_SUCCESS;
    if (0 == cur_run_len_) {
      ret = read_run();
    }
    if (OB_SUCC(ret)) {
      if (cur_run_len_ <= 0) {
        ret = OB_ITER_END;
      } else {
        if (is_packed_) {
          const int bit_pos = (packed_index_ * bit_width_) & 7;
          const int64_t byte_pos = (packed_index_ * bit_width_) >> 3;
          const char *p = packed_base_ + byte_pos;
          const char *pend = end_;
          uint64_t v = 0;
          int bits = 0;
          int shift = 0;
          while (bits < bit_width_ && p < pend) {
            v |= (static_cast<uint64_t>(static_cast<uint8_t>(*p++)) << shift);
            shift += 8;
            bits += 8;
          }
          value = (v >> bit_pos) & ((bit_width_ >= 64) ? ~0ULL : ((1ULL << bit_width_) - 1));
          ++packed_index_;
        } else {
          value = rle_value_;
        }
        --cur_run_len_;
      }
    }
    return ret;
  }
private:
  int read_run()
  {
    int ret = OB_SUCCESS;
    // read uvarint header
    uint64_t header = 0;
    int shift = 0;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      if (ptr_ >= end_) {
        ret = OB_ITER_END;
      } else {
        const uint8_t b = static_cast<uint8_t>(*ptr_++);
        header |= (static_cast<uint64_t>(b & 0x7f) << shift);
        done = (0 == (b & 0x80));
        shift += 7;
      }
    }
    if (OB_SUCC(ret)) {
      if (0 == (header & 1)) {
        is_packed_ = false;
        cur_run_len_ = static_cast<int64_t>(header >> 1);
        const int64_t bytes = (bit_width_ + 7) / 8;
        if (ptr_ + bytes > end_) {
          ret = OB_INVALID_DATA;
        } else {
          rle_value_ = 0;
          for (int64_t i = 0; i < bytes; ++i) {
            rle_value_ |= (static_cast<uint64_t>(static_cast<uint8_t>(ptr_[i])) << (8 * i));
          }
          ptr_ += bytes;
        }
      } else {
        is_packed_ = true;
        const int64_t groups = static_cast<int64_t>(header >> 1);
        cur_run_len_ = groups * 8;
        packed_base_ = ptr_;
        packed_index_ = 0;
        const int64_t bytes = groups * bit_width_;
        if (ptr_ + bytes > end_) {
          ret = OB_INVALID_DATA;
        } else {
          ptr_ += bytes;
        }
      }
    }
    return ret;
  }

  const char *ptr_;
  const char *end_;
  const char *packed_base_ = nullptr;
  int bit_width_;
  bool is_packed_ = false;
  uint64_t rle_value_ = 0;
  int64_t cur_run_len_;
  int64_t packed_index_ = 0;
};

int civil_from_days(int64_t z, int64_t &y, int64_t &m, int64_t &d)
{
  int ret = OB_SUCCESS;
  if (z < -1000000000LL || z > 1000000000LL) {
    ret = OB_INVALID_DATE_VALUE;
  } else {
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const int64_t doe = z - era * 146097;
    const int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    y = yoe + era * 400;
    const int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const int64_t mp = (5 * doy + 2) / 153;
    d = doy - (153 * mp + 2) / 5 + 1;
    m = mp + (mp < 10 ? 3 : -9);
    y += (m <= 2 ? 1 : 0);
  }
  return ret;
}

int format_date(int32_t days, char *buf, int64_t size, int64_t &len)
{
  int ret = OB_SUCCESS;
  int64_t y = 0, m = 0, d = 0;
  if (OB_FAIL(civil_from_days(days, y, m, d))) {
  } else {
    len = snprintf(buf, size, "%04ld-%02ld-%02ld",
                   static_cast<long>(y), static_cast<long>(m), static_cast<long>(d));
  }
  return ret;
}

int format_datetime(int64_t micros, char *buf, int64_t size, int64_t &len)
{
  int ret = OB_SUCCESS;
  int64_t days = micros / 86400000000LL;
  int64_t rem = micros % 86400000000LL;
  if (rem < 0) {
    rem += 86400000000LL;
    --days;
  }
  int64_t y = 0, m = 0, d = 0;
  if (OB_FAIL(civil_from_days(days, y, m, d))) {
  } else {
    const int64_t hh = rem / 3600000000LL;
    const int64_t mm = (rem % 3600000000LL) / 60000000LL;
    const int64_t ss = (rem % 60000000LL) / 1000000LL;
    const int64_t us = rem % 1000000LL;
    if (0 == us) {
      len = snprintf(buf, size, "%04ld-%02ld-%02ld %02ld:%02ld:%02ld",
                     static_cast<long>(y), static_cast<long>(m), static_cast<long>(d),
                     static_cast<long>(hh), static_cast<long>(mm), static_cast<long>(ss));
    } else {
      len = snprintf(buf, size, "%04ld-%02ld-%02ld %02ld:%02ld:%02ld.%06ld",
                     static_cast<long>(y), static_cast<long>(m), static_cast<long>(d),
                     static_cast<long>(hh), static_cast<long>(mm), static_cast<long>(ss),
                     static_cast<long>(us));
    }
  }
  return ret;
}

} // anonymous namespace

struct ObParquetReader::Impl
{
  Impl()
    : fd_(-1), meta_alloc_("ParquetMeta"), row_alloc_("ParquetRow"),
      row_group_idx_(-1), row_in_group_(0), eof_(false) {}
  ~Impl() { close(); }

  void close()
  {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  int read_at(int64_t offset, int64_t size, char *buf)
  {
    int ret = OB_SUCCESS;
    int64_t done = 0;
    while (OB_SUCC(ret) && done < size) {
      const ssize_t n = ::pread(fd_, buf + done, size - done, offset + done);
      if (n < 0) {
        if (EINTR == errno) {
          continue;
        }
        ret = OB_IO_ERROR;
      } else if (0 == n) {
        ret = OB_INVALID_DATA;
      } else {
        done += n;
      }
    }
    return ret;
  }

  int load_metadata(const common::ObString &filename)
  {
    int ret = OB_SUCCESS;
    char path[4097];
    if (filename.empty() || filename.length() > 4096) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      MEMCPY(path, filename.ptr(), filename.length());
      path[filename.length()] = '\0';
      fd_ = ::open(path, O_RDONLY);
      struct stat st;
      if (fd_ < 0) {
        ret = OB_FILE_NOT_EXIST;
        LOG_WARN("failed to open parquet file", K(filename), K(errno), K(ret));
      } else if (0 != ::fstat(fd_, &st) || st.st_size < 12) {
        ret = OB_INVALID_DATA;
      } else {
        char tail[8];
        if (OB_FAIL(read_at(st.st_size - 8, 8, tail))) {
        } else if (0 != MEMCMP(tail + 4, "PAR1", 4)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid parquet file magic", K(ret));
        } else {
          uint32_t meta_len = static_cast<uint8_t>(tail[0]) |
                              (static_cast<uint32_t>(static_cast<uint8_t>(tail[1])) << 8) |
                              (static_cast<uint32_t>(static_cast<uint8_t>(tail[2])) << 16) |
                              (static_cast<uint32_t>(static_cast<uint8_t>(tail[3])) << 24);
          if (meta_len <= 0 || static_cast<int64_t>(meta_len) > st.st_size - 12) {
            ret = OB_INVALID_DATA;
          } else {
            char *buf = static_cast<char *>(meta_alloc_.alloc(meta_len));
            if (OB_ISNULL(buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else if (OB_FAIL(read_at(st.st_size - 8 - meta_len, meta_len, buf))) {
            } else if (OB_FAIL(parse_file_meta_data(buf, meta_len, meta_))) {
            } else if (OB_FAIL(build_leaf_index(meta_, leaves_))) {
            }
          }
        }
      }
    }
    return ret;
  }

  int load_row_group(int64_t rg_idx)
  {
    int ret = OB_SUCCESS;
    row_alloc_.reset();
    col_values_.reuse();
    col_nulls_.reuse();
    if (rg_idx < 0 || rg_idx >= meta_.row_groups_.count()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ParquetRowGroup &rg = meta_.row_groups_.at(rg_idx);
      const int64_t col_count = static_cast<int64_t>(projection_.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        common::ObSEArray<common::ObString, 64> value_array;
        common::ObSEArray<bool, 64> null_array;
        if (OB_FAIL(col_values_.push_back(value_array))) {
        } else if (OB_FAIL(col_nulls_.push_back(null_array))) {
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        const int32_t schema_idx = projection_.at(i);
        int64_t chunk_idx = -1;
        for (int64_t j = 0; j < rg.columns_.count(); ++j) {
          // Column chunks are in leaf order for flat schemas.
          if (j < leaves_.count() && leaves_.at(j) == schema_idx) {
            chunk_idx = j;
          }
        }
        if (chunk_idx < 0) {
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(decode_chunk(rg.columns_.at(chunk_idx),
                                        meta_.schema_.at(schema_idx),
                                        col_values_.at(i), col_nulls_.at(i)))) {
        }
      }
      if (OB_SUCC(ret)) {
        row_in_group_ = 0;
      }
    }
    return ret;
  }

  int decode_chunk(const ParquetColumnChunk &chunk,
                   const ParquetSchemaElement &schema,
                   common::ObSEArray<common::ObString, 64> &values,
                   common::ObSEArray<bool, 64> &nulls)
  {
    int ret = OB_SUCCESS;
    int64_t start = chunk.data_page_offset_;
    if (chunk.dictionary_page_offset_ > 0 && chunk.dictionary_page_offset_ < start) {
      start = chunk.dictionary_page_offset_;
    }
    const int64_t size = chunk.total_compressed_size_;
    current_codec_ = chunk.codec_;
    char *chunk_buf = static_cast<char *>(row_alloc_.alloc(size + 1));
    if (size <= 0 || start < 0) {
      ret = OB_INVALID_DATA;
    } else if (OB_ISNULL(chunk_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(read_at(start, size, chunk_buf))) {
    } else {
      const char *p = chunk_buf;
      const char *end = chunk_buf + size;
      common::ObSEArray<common::ObString, 64> dict;
      while (OB_SUCC(ret) && p < end) {
        int64_t header_len = 0;
        PageHeaderInfo ph;
        if (OB_FAIL(parse_page_header(p, end - p, ph, header_len))) {
        } else if (ph.compressed_size_ < 0 || p + header_len + ph.compressed_size_ > end) {
          ret = OB_INVALID_DATA;
        } else {
          const char *page = p + header_len;
          p += header_len + ph.compressed_size_;
          if (PAGE_DICTIONARY == ph.type_) {
            if (OB_FAIL(parse_dictionary_page(page, ph, schema, dict))) {
            }
          } else if (PAGE_DATA == ph.type_) {
            if (OB_FAIL(parse_data_page(page, ph, schema, dict, values, nulls))) {
            }
          } else if (PAGE_DATA_V2 == ph.type_) {
            if (OB_FAIL(parse_data_page_v2(page, ph, schema, dict, values, nulls))) {
            }
          }
        }
      }
    }
    return ret;
  }

  enum PageType { PAGE_DATA = 0, PAGE_INDEX = 1, PAGE_DICTIONARY = 2, PAGE_DATA_V2 = 3 };

  struct PageHeaderInfo
  {
    PageHeaderInfo()
      : type_(-1), uncompressed_size_(0), compressed_size_(0),
        num_values_(0), encoding_(0), def_level_encoding_(0),
        rep_level_encoding_(0), num_nulls_(0), num_rows_(0),
        def_levels_byte_length_(-1), is_v2_(false) {}
    int32_t type_;
    int32_t uncompressed_size_;
    int32_t compressed_size_;
    int32_t num_values_;
    int32_t encoding_;
    int32_t def_level_encoding_;
    int32_t rep_level_encoding_;
    int32_t num_nulls_;
    int32_t num_rows_;
    int32_t def_levels_byte_length_;
    bool is_v2_;
  };

  int parse_data_page_header(ThriftReader &r, PageHeaderInfo &ph)
  {
    int ret = OB_SUCCESS;
    ThriftStructReader sr(r);
    int16_t fid = 0;
    uint8_t type = 0;
    while (OB_SUCC(ret) && OB_SUCC(sr.next(fid, type)) && TT_STOP != type) {
      int64_t v = 0;
      if (1 == fid || 2 == fid || 3 == fid || 4 == fid) {
        if (OB_FAIL(r.read_zigzag(v))) {
        } else if (1 == fid) {
          ph.num_values_ = static_cast<int32_t>(v);
        } else if (2 == fid) {
          ph.encoding_ = static_cast<int32_t>(v);
        } else if (3 == fid) {
          ph.def_level_encoding_ = static_cast<int32_t>(v);
        } else {
          ph.rep_level_encoding_ = static_cast<int32_t>(v);
        }
      } else {
        ret = r.skip(type);
      }
    }
    return ret;
  }

  int parse_data_page_header_v2(ThriftReader &r, PageHeaderInfo &ph)
  {
    int ret = OB_SUCCESS;
    ThriftStructReader sr(r);
    int16_t fid = 0;
    uint8_t type = 0;
    while (OB_SUCC(ret) && OB_SUCC(sr.next(fid, type)) && TT_STOP != type) {
      if (1 == fid || 2 == fid || 3 == fid || 4 == fid ||
          5 == fid || 6 == fid) {
        int64_t v = 0;
        if (OB_FAIL(r.read_zigzag(v))) {
        } else if (1 == fid) {
          ph.num_values_ = static_cast<int32_t>(v);
        } else if (2 == fid) {
          ph.num_nulls_ = static_cast<int32_t>(v);
        } else if (3 == fid) {
          ph.num_rows_ = static_cast<int32_t>(v);
        } else if (4 == fid) {
          ph.encoding_ = static_cast<int32_t>(v);
        } else if (5 == fid) {
          ph.def_levels_byte_length_ = static_cast<int32_t>(v);
        } else {
          // repetition levels byte length
        }
      } else if (7 == fid) {
        bool b = false;
        if (OB_FAIL(read_bool(r, type, b))) {
        }
      } else {
        ret = r.skip(type);
      }
    }
    return ret;
  }

  int parse_page_header(const char *buf, int64_t len,
                        PageHeaderInfo &ph, int64_t &consumed)
  {
    int ret = OB_SUCCESS;
    ThriftReader r;
    r.init(buf, len);
    ThriftStructReader sr(r);
    int16_t fid = 0;
    uint8_t type = 0;
    while (OB_SUCC(ret) && OB_SUCC(sr.next(fid, type)) && TT_STOP != type) {
      if (1 == fid || 2 == fid || 3 == fid) {
        int64_t v = 0;
        if (OB_FAIL(r.read_zigzag(v))) {
        } else if (1 == fid) {
          ph.type_ = static_cast<int32_t>(v);
        } else if (2 == fid) {
          ph.uncompressed_size_ = static_cast<int32_t>(v);
        } else {
          ph.compressed_size_ = static_cast<int32_t>(v);
        }
      } else if (5 == fid) {
        if (OB_FAIL(parse_data_page_header(r, ph))) {
        }
      } else if (7 == fid) {
        ph.is_v2_ = false;
        int64_t count = 0;
        ThriftStructReader dsr(r);
        int16_t dfid = 0;
        uint8_t dtype = 0;
        while (OB_SUCC(ret) && OB_SUCC(dsr.next(dfid, dtype)) && TT_STOP != dtype) {
          if (1 == dfid) {
            int64_t v = 0;
            if (OB_FAIL(r.read_zigzag(v))) {
            } else {
              count = v;
            }
          } else {
            ret = r.skip(dtype);
          }
        }
        ph.num_values_ = static_cast<int32_t>(count);
      } else if (8 == fid) {
        ph.is_v2_ = true;
        if (OB_FAIL(parse_data_page_header_v2(r, ph))) {
        }
      } else {
        ret = r.skip(type);
      }
    }
    if (OB_SUCC(ret)) {
      consumed = r.pos();
    }
    return ret;
  }

  int decompress_page(const char *page, int32_t compressed, int32_t uncompressed,
                      const char *&out, char *&tmp)
  {
    int ret = OB_SUCCESS;
    out = page;
    tmp = nullptr;
    if (0 == current_codec_ || compressed == uncompressed) {
      out = page;
    } else if (1 == current_codec_) {
      tmp = static_cast<char *>(row_alloc_.alloc(uncompressed + 1));
      int64_t written = 0;
      if (OB_ISNULL(tmp)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(snappy_decompress(page, compressed, tmp, uncompressed, written))) {
      } else if (written != uncompressed) {
        ret = OB_INVALID_DATA;
      } else {
        out = tmp;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported parquet compression codec", K(current_codec_), K(ret));
    }
    return ret;
  }

  int decode_plain_values(const char *data, int64_t len, int64_t count,
                          const ParquetSchemaElement &schema,
                          common::ObSEArray<common::ObString, 64> &values)
  {
    int ret = OB_SUCCESS;
    const char *p = data;
    const char *end = data + len;
    char tmp[64];
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      common::ObString value;
      int64_t out_len = 0;
      switch (schema.type_) {
        case 0: { // BOOLEAN
          if (p >= end) {
            ret = OB_INVALID_DATA;
          } else {
            const uint8_t b = static_cast<uint8_t>(*p++);
            const bool v = (b >> (i % 8)) & 1;
            value.assign_ptr(v ? "1" : "0", 1);
          }
          break;
        }
        case 1: { // INT32
          if (p + 4 > end) {
            ret = OB_INVALID_DATA;
          } else {
            int32_t v = 0;
            MEMCPY(&v, p, 4);
            p += 4;
            if (6 == schema.converted_type_) {
              char *copy = static_cast<char *>(row_alloc_.alloc(16));
              if (OB_ISNULL(copy)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else if (OB_FAIL(format_date(v, copy, 16, out_len))) {
              } else {
                value.assign_ptr(copy, static_cast<int32_t>(out_len));
              }
            } else {
              out_len = snprintf(tmp, sizeof(tmp), "%d", v);
              char *copy = static_cast<char *>(row_alloc_.alloc(out_len));
              if (OB_ISNULL(copy)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                MEMCPY(copy, tmp, out_len);
                value.assign_ptr(copy, static_cast<int32_t>(out_len));
              }
            }
          }
          break;
        }
        case 2: { // INT64
          if (p + 8 > end) {
            ret = OB_INVALID_DATA;
          } else {
            int64_t v = 0;
            MEMCPY(&v, p, 8);
            p += 8;
            if (9 == schema.converted_type_) {
              v *= 1000;
            }
            if (9 == schema.converted_type_ || 10 == schema.converted_type_) {
              char *copy = static_cast<char *>(row_alloc_.alloc(32));
              if (OB_ISNULL(copy)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else if (OB_FAIL(format_datetime(v, copy, 32, out_len))) {
              } else {
                value.assign_ptr(copy, static_cast<int32_t>(out_len));
              }
            } else {
              out_len = snprintf(tmp, sizeof(tmp), "%ld", static_cast<long>(v));
              char *copy = static_cast<char *>(row_alloc_.alloc(out_len));
              if (OB_ISNULL(copy)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                MEMCPY(copy, tmp, out_len);
                value.assign_ptr(copy, static_cast<int32_t>(out_len));
              }
            }
          }
          break;
        }
        case 3: { // INT96
          if (p + 12 > end) {
            ret = OB_INVALID_DATA;
          } else {
            int64_t nanos = 0;
            int32_t julian = 0;
            MEMCPY(&nanos, p, 8);
            MEMCPY(&julian, p + 8, 4);
            p += 12;
            const int64_t micros = (static_cast<int64_t>(julian) - 2440588LL) * 86400000000LL +
                                   nanos / 1000;
            char *copy = static_cast<char *>(row_alloc_.alloc(32));
            if (OB_ISNULL(copy)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else if (OB_FAIL(format_datetime(micros, copy, 32, out_len))) {
            } else {
              value.assign_ptr(copy, static_cast<int32_t>(out_len));
            }
          }
          break;
        }
        case 4: { // FLOAT
          if (p + 4 > end) {
            ret = OB_INVALID_DATA;
          } else {
            float v = 0;
            MEMCPY(&v, p, 4);
            p += 4;
            out_len = snprintf(tmp, sizeof(tmp), "%.9g", v);
            char *copy = static_cast<char *>(row_alloc_.alloc(out_len));
            if (OB_ISNULL(copy)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              MEMCPY(copy, tmp, out_len);
              value.assign_ptr(copy, static_cast<int32_t>(out_len));
            }
          }
          break;
        }
        case 5: { // DOUBLE
          if (p + 8 > end) {
            ret = OB_INVALID_DATA;
          } else {
            double v = 0;
            MEMCPY(&v, p, 8);
            p += 8;
            out_len = snprintf(tmp, sizeof(tmp), "%.17g", v);
            char *copy = static_cast<char *>(row_alloc_.alloc(out_len));
            if (OB_ISNULL(copy)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              MEMCPY(copy, tmp, out_len);
              value.assign_ptr(copy, static_cast<int32_t>(out_len));
            }
          }
          break;
        }
        case 6:
        case 7: { // BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY
          int32_t n = 0;
          if (6 == schema.type_) {
            if (p + 4 > end) {
              ret = OB_INVALID_DATA;
            } else {
              MEMCPY(&n, p, 4);
              p += 4;
            }
          } else {
            n = 0; // FIXED_LEN_BYTE_ARRAY size is not in the schema subset we keep
            ret = OB_NOT_SUPPORTED;
          }
          if (OB_SUCC(ret)) {
            if (n < 0 || p + n > end) {
              ret = OB_INVALID_DATA;
            } else {
              char *copy = static_cast<char *>(row_alloc_.alloc(n + 1));
              if (OB_ISNULL(copy)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                MEMCPY(copy, p, n);
                copy[n] = '\0';
                value.assign_ptr(copy, n);
                p += n;
              }
            }
          }
          break;
        }
        default:
          ret = OB_NOT_SUPPORTED;
          break;
      }
      if (OB_SUCC(ret) && OB_FAIL(values.push_back(value))) {
        LOG_WARN("failed to push back parquet value", K(ret));
      }
    }
    return ret;
  }

  int parse_dictionary_page(const char *page, const PageHeaderInfo &ph,
                            const ParquetSchemaElement &schema,
                            common::ObSEArray<common::ObString, 64> &dict)
  {
    int ret = OB_SUCCESS;
    const char *data = nullptr;
    char *tmp = nullptr;
    if (OB_FAIL(decompress_page(page, ph.compressed_size_, ph.uncompressed_size_, data, tmp))) {
    } else if (OB_FAIL(decode_plain_values(data, ph.uncompressed_size_, ph.num_values_,
                                           schema, dict))) {
    }
    return ret;
  }

  int decode_levels(const char *data, int64_t len, int max_level,
                    int64_t count, common::ObSEArray<int32_t, 64> &levels)
  {
    int ret = OB_SUCCESS;
    if (max_level <= 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        if (OB_FAIL(levels.push_back(0))) {
        }
      }
    } else {
      RleBitPackedDecoder decoder;
      decoder.init(data, len, max_level > 1 ? 2 : 1);
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        uint64_t v = 0;
        if (OB_FAIL(decoder.get(v))) {
          if (OB_ITER_END == ret) {
            ret = OB_INVALID_DATA;
          }
        } else if (OB_FAIL(levels.push_back(static_cast<int32_t>(v)))) {
        }
      }
    }
    return ret;
  }

  int parse_data_page(const char *page, const PageHeaderInfo &ph,
                      const ParquetSchemaElement &schema,
                      common::ObSEArray<common::ObString, 64> &dict,
                      common::ObSEArray<common::ObString, 64> &values,
                      common::ObSEArray<bool, 64> &nulls)
  {
    int ret = OB_SUCCESS;
    const char *data = nullptr;
    char *tmp = nullptr;
    if (OB_FAIL(decompress_page(page, ph.compressed_size_, ph.uncompressed_size_, data, tmp))) {
    } else {
      int64_t pos = 0;
      const int64_t total = ph.uncompressed_size_;
      int32_t max_def = (schema.repetition_ == 1) ? 1 : 0;
      common::ObSEArray<int32_t, 64> levels;
      if (max_def > 0) {
        // repetition levels: none for flat columns
        // definition levels: 4 byte length prefix + RLE
        if (pos + 4 > total) {
          ret = OB_INVALID_DATA;
        } else if (ph.def_level_encoding_ != 3) {
          ret = OB_NOT_SUPPORTED;
        } else {
          int32_t lvl_len = 0;
          MEMCPY(&lvl_len, data + pos, 4);
          pos += 4;
          if (lvl_len < 0 || pos + lvl_len > total) {
            ret = OB_INVALID_DATA;
          } else if (OB_FAIL(decode_levels(data + pos, lvl_len, max_def, ph.num_values_, levels))) {
          } else {
            pos += lvl_len;
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ph.num_values_; ++i) {
          if (OB_FAIL(levels.push_back(0))) {
          }
        }
      }
      if (OB_SUCC(ret)) {
        int64_t non_null = 0;
        for (int64_t i = 0; i < levels.count(); ++i) {
          if (levels.at(i) == max_def) {
            ++non_null;
          }
        }
        common::ObSEArray<common::ObString, 64> decoded;
        if (OB_FAIL(decode_values(data + pos, total - pos, non_null, ph.encoding_,
                                  schema, dict, decoded))) {
        } else {
          int64_t vi = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < levels.count(); ++i) {
            if (levels.at(i) != max_def) {
              common::ObString empty;
              if (OB_FAIL(values.push_back(empty)) || OB_FAIL(nulls.push_back(true))) {
              }
            } else {
              if (vi >= decoded.count()) {
                ret = OB_INVALID_DATA;
              } else if (OB_FAIL(values.push_back(decoded.at(vi++)))) {
              } else if (OB_FAIL(nulls.push_back(false))) {
              }
            }
          }
        }
      }
    }
    return ret;
  }

  int parse_data_page_v2(const char *page, const PageHeaderInfo &ph,
                         const ParquetSchemaElement &schema,
                         common::ObSEArray<common::ObString, 64> &dict,
                         common::ObSEArray<common::ObString, 64> &values,
                         common::ObSEArray<bool, 64> &nulls)
  {
    int ret = OB_SUCCESS;
    const int32_t rep_len = 0;
    const int32_t def_len = ph.def_levels_byte_length_ > 0 ? ph.def_levels_byte_length_ : 0;
    const char *levels = page;
    const char *data = page + rep_len + def_len;
    const int64_t data_len = ph.uncompressed_size_ - rep_len - def_len;
    char *tmp = nullptr;
    const char *decoded_data = data;
    int32_t max_def = (schema.repetition_ == 1) ? 1 : 0;
    common::ObSEArray<int32_t, 64> levels_arr;
    if (ph.compressed_size_ != ph.uncompressed_size_) {
      tmp = static_cast<char *>(row_alloc_.alloc(data_len + 1));
      int64_t written = 0;
      if (OB_ISNULL(tmp)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(snappy_decompress(data, ph.compressed_size_ - rep_len - def_len,
                                           tmp, data_len, written))) {
      } else if (written != data_len) {
        ret = OB_INVALID_DATA;
      } else {
        decoded_data = tmp;
      }
    }
    if (OB_SUCC(ret)) {
      if (max_def > 0) {
        if (OB_FAIL(decode_levels(levels + rep_len, def_len, max_def, ph.num_values_, levels_arr))) {
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ph.num_values_; ++i) {
          if (OB_FAIL(levels_arr.push_back(0))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t non_null = 0;
      for (int64_t i = 0; i < levels_arr.count(); ++i) {
        if (levels_arr.at(i) == max_def) {
          ++non_null;
        }
      }
      common::ObSEArray<common::ObString, 64> decoded;
      if (OB_FAIL(decode_values(decoded_data, data_len, non_null, ph.encoding_,
                                schema, dict, decoded))) {
      } else {
        int64_t vi = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < levels_arr.count(); ++i) {
          if (levels_arr.at(i) != max_def) {
            common::ObString empty;
            if (OB_FAIL(values.push_back(empty)) || OB_FAIL(nulls.push_back(true))) {
            }
          } else {
            if (vi >= decoded.count()) {
              ret = OB_INVALID_DATA;
            } else if (OB_FAIL(values.push_back(decoded.at(vi++)))) {
            } else if (OB_FAIL(nulls.push_back(false))) {
            }
          }
        }
      }
    }
    return ret;
  }

  int decode_values(const char *data, int64_t len, int64_t count, int32_t encoding,
                    const ParquetSchemaElement &schema,
                    common::ObSEArray<common::ObString, 64> &dict,
                    common::ObSEArray<common::ObString, 64> &values)
  {
    int ret = OB_SUCCESS;
    if (0 == encoding) { // PLAIN
      ret = decode_plain_values(data, len, count, schema, values);
    } else if (2 == encoding || 8 == encoding) { // PLAIN_DICTIONARY / RLE_DICTIONARY
      if (len < 1 || dict.empty()) {
        ret = OB_INVALID_DATA;
      } else {
        const int bit_width = static_cast<uint8_t>(data[0]);
        RleBitPackedDecoder decoder;
        decoder.init(data + 1, len - 1, bit_width);
        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          uint64_t idx = 0;
          if (OB_FAIL(decoder.get(idx))) {
            if (OB_ITER_END == ret) {
              ret = OB_INVALID_DATA;
            }
          } else if (idx >= static_cast<uint64_t>(dict.count())) {
            ret = OB_INVALID_DATA;
          } else if (OB_FAIL(values.push_back(dict.at(idx)))) {
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported parquet encoding", K(encoding), K(ret));
    }
    return ret;
  }

  int fd_;
  common::ObArenaAllocator meta_alloc_;
  common::ObArenaAllocator row_alloc_;
  ParquetFileMeta meta_;
  common::ObSEArray<int32_t, 8> leaves_;
  common::ObSEArray<int32_t, 8> projection_;  // leaf schema index per projected column
  common::ObSEArray<common::ObSEArray<common::ObString, 64>, 8> col_values_;
  common::ObSEArray<common::ObSEArray<bool, 64>, 8> col_nulls_;
  int64_t row_group_idx_;
  int64_t row_in_group_;
  bool eof_;
  int32_t current_codec_ = 0;
};

ObParquetReader::ObParquetReader() : impl_(nullptr) {}
ObParquetReader::~ObParquetReader() { close(); }

int ObParquetReader::open(const common::ObString &filename,
                          const common::ObIArray<FileColumnMeta> &columns,
                          common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  (void)alloc;
  if (OB_ISNULL(impl_ = new (std::nothrow) Impl())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(impl_->load_metadata(filename))) {
  } else {
    impl_->projection_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      // The requested columns follow the parquet leaf order, so map each of
      // them to its schema index (schema_[0] is the root element).
      if (i >= impl_->leaves_.count()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("projected column exceeds parquet leaf count",
                 K(i), K(impl_->leaves_.count()), K(ret));
      } else if (OB_FAIL(impl_->projection_.push_back(impl_->leaves_.at(i)))) {
      }
    }
    impl_->row_group_idx_ = -1;
    impl_->row_in_group_ = 0;
  }
  return ret;
}

int ObParquetReader::next_row(common::ObIArray<common::ObString> &values,
                              common::ObIArray<bool> &nulls)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(impl_)) {
    ret = OB_NOT_INIT;
  } else {
    bool ready = false;
    while (OB_SUCC(ret) && !ready) {
      if (impl_->row_group_idx_ < 0 ||
          impl_->row_in_group_ >= impl_->meta_.row_groups_.at(impl_->row_group_idx_).num_rows_) {
        ++impl_->row_group_idx_;
        if (impl_->row_group_idx_ >= impl_->meta_.row_groups_.count()) {
          ret = OB_ITER_END;
        } else if (OB_FAIL(impl_->load_row_group(impl_->row_group_idx_))) {
        }
      } else {
        ready = true;
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t col_count = impl_->projection_.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        if (impl_->row_in_group_ >= impl_->col_values_.at(i).count()) {
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(values.push_back(impl_->col_values_.at(i).at(impl_->row_in_group_)))) {
        } else if (OB_FAIL(nulls.push_back(impl_->col_nulls_.at(i).at(impl_->row_in_group_)))) {
        }
      }
      if (OB_SUCC(ret)) {
        ++impl_->row_in_group_;
      }
    }
  }
  return ret;
}

void ObParquetReader::close()
{
  if (OB_NOT_NULL(impl_)) {
    impl_->close();
    impl_ = nullptr;
  }
}

int infer_parquet_schema(const common::ObString &filename,
                         common::ObIAllocator &alloc,
                         common::ObIArray<FileColumnMeta> &columns,
                         FileStatInfo &stat_info)
{
  int ret = OB_SUCCESS;
  ObParquetReader::Impl *impl = nullptr;
  if (OB_FAIL(get_file_stat(filename, stat_info))) {
  } else if (OB_ISNULL(impl = new (std::nothrow) ObParquetReader::Impl())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    if (OB_FAIL(impl->load_metadata(filename))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < impl->meta_.schema_.count(); ++i) {
        const ParquetSchemaElement &elem = impl->meta_.schema_.at(i);
        if (!is_leaf(elem)) {
          continue;
        }
        FileColumnMeta meta;
        char *name = static_cast<char *>(alloc.alloc(elem.name_.length() + 1));
        if (OB_ISNULL(name)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL(map_parquet_type(elem, meta.column_type_))) {
        } else {
          MEMCPY(name, elem.name_.ptr(), elem.name_.length());
          name[elem.name_.length()] = '\0';
          meta.column_name_.assign_ptr(name, elem.name_.length());
          meta.nullable_ = (1 == elem.repetition_);
          meta.source_index_ = columns.count();
          if (OB_FAIL(columns.push_back(meta))) {
          }
        }
      }
    }
    delete impl;
    impl = nullptr;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
