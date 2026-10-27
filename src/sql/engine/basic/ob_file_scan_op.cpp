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

#include "sql/engine/basic/ob_file_scan_op.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

namespace
{

int encode_ob_string(char *buf, const int64_t buf_len, int64_t &pos, const ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(common::serialization::encode_vi32(buf, buf_len, pos, str.length()))) {
  } else if (OB_UNLIKELY(pos + str.length() > buf_len)) {
    ret = OB_SIZE_OVERFLOW;
  } else if (str.length() > 0) {
    MEMCPY(buf + pos, str.ptr(), str.length());
    pos += str.length();
  }
  return ret;
}

int decode_ob_string(const char *buf, const int64_t data_len, int64_t &pos,
                     ObIAllocator &alloc, ObString &str)
{
  int ret = OB_SUCCESS;
  int32_t len = 0;
  if (OB_FAIL(common::serialization::decode_vi32(buf, data_len, pos, &len))) {
  } else if (len < 0 || pos + len > data_len) {
    ret = OB_DESERIALIZE_ERROR;
  } else if (0 == len) {
    str.reset();
  } else {
    char *copy = static_cast<char *>(alloc.alloc(len));
    if (OB_ISNULL(copy)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(copy, buf + pos, len);
      pos += len;
      str.assign_ptr(copy, len);
    }
  }
  return ret;
}

int64_t encoded_ob_string_length(const ObString &str)
{
  return common::serialization::encoded_length_vi32(str.length()) + str.length();
}

} // anonymous namespace

OB_DEF_SERIALIZE(ObFileScanSpec)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObFileScanSpec, ObOpSpec));
  OB_UNIS_ENCODE(function_type_);
  OB_UNIS_ENCODE(has_header_);
  OB_UNIS_ENCODE(delimiter_);
  OB_UNIS_ENCODE(quote_);
  OB_UNIS_ENCODE(escape_);
  OB_UNIS_ENCODE(file_size_);
  OB_UNIS_ENCODE(file_mtime_);
  OB_UNIS_ENCODE(file_ino_);
  OB_UNIS_ENCODE(table_id_);
  if (OB_FAIL(encode_ob_string(buf, buf_len, pos, file_path_))) {
  } else if (OB_FAIL(encode_ob_string(buf, buf_len, pos, null_string_))) {
  } else if (OB_FAIL(encode_ob_string(buf, buf_len, pos, table_name_))) {
  } else {
    int64_t column_count = column_names_.count();
    OB_UNIS_ENCODE(column_count);
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      if (OB_FAIL(encode_ob_string(buf, buf_len, pos, column_names_.at(i)))) {
      } else {
        OB_UNIS_ENCODE(column_types_.at(i));
        OB_UNIS_ENCODE(column_nullables_.at(i));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObFileScanSpec)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObFileScanSpec, ObOpSpec));
  OB_UNIS_ADD_LEN(function_type_);
  OB_UNIS_ADD_LEN(has_header_);
  OB_UNIS_ADD_LEN(delimiter_);
  OB_UNIS_ADD_LEN(quote_);
  OB_UNIS_ADD_LEN(escape_);
  OB_UNIS_ADD_LEN(file_size_);
  OB_UNIS_ADD_LEN(file_mtime_);
  OB_UNIS_ADD_LEN(file_ino_);
  OB_UNIS_ADD_LEN(table_id_);
  len += encoded_ob_string_length(file_path_);
  len += encoded_ob_string_length(null_string_);
  len += encoded_ob_string_length(table_name_);
  const int64_t column_count = column_names_.count();
  len += common::serialization::encoded_length_vi64(column_count);
  for (int64_t i = 0; i < column_count; ++i) {
    len += encoded_ob_string_length(column_names_.at(i));
    len += common::serialization::encoded_length_vi64(column_types_.at(i));
    len += common::serialization::encoded_length_vi64(column_nullables_.at(i));
  }
  return len;
}

OB_DEF_DESERIALIZE(ObFileScanSpec)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObFileScanSpec, ObOpSpec));
  OB_UNIS_DECODE(function_type_);
  OB_UNIS_DECODE(has_header_);
  OB_UNIS_DECODE(delimiter_);
  OB_UNIS_DECODE(quote_);
  OB_UNIS_DECODE(escape_);
  OB_UNIS_DECODE(file_size_);
  OB_UNIS_DECODE(file_mtime_);
  OB_UNIS_DECODE(file_ino_);
  OB_UNIS_DECODE(table_id_);
  if (OB_FAIL(decode_ob_string(buf, data_len, pos, *alloc_, file_path_))) {
  } else if (OB_FAIL(decode_ob_string(buf, data_len, pos, *alloc_, null_string_))) {
  } else if (OB_FAIL(decode_ob_string(buf, data_len, pos, *alloc_, table_name_))) {
  } else {
    int64_t column_count = 0;
    OB_UNIS_DECODE(column_count);
    if (OB_SUCC(ret) && OB_FAIL(column_names_.init(column_count))) {
    } else if (OB_SUCC(ret) && OB_FAIL(column_types_.init(column_count))) {
    } else if (OB_SUCC(ret) && OB_FAIL(column_nullables_.init(column_count))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      ObString name;
      int64_t column_type = 0;
      int64_t nullable = 0;
      if (OB_FAIL(decode_ob_string(buf, data_len, pos, *alloc_, name))) {
      } else {
        OB_UNIS_DECODE(column_type);
        OB_UNIS_DECODE(nullable);
        if (OB_SUCC(ret) && OB_FAIL(column_names_.push_back(name))) {
        } else if (OB_SUCC(ret) && OB_FAIL(column_types_.push_back(column_type))) {
        } else if (OB_SUCC(ret) && OB_FAIL(column_nullables_.push_back(nullable))) {
        }
      }
    }
  }
  return ret;
}

int ObFileScanOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(MY_SPEC.column_names_.count() != MY_SPEC.column_types_.count() ||
                  MY_SPEC.column_names_.count() != MY_SPEC.column_nullables_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected file scan spec", K(ret),
             K(MY_SPEC.column_names_.count()), K(MY_SPEC.column_types_.count()));
  } else if (OB_FAIL(check_file_unchanged())) {
  } else {
    options_.format_ = static_cast<FileTableFormat>(MY_SPEC.function_type_ == 1
                                                    ? FILE_TABLE_JSONL
                                                    : (MY_SPEC.function_type_ == 2
                                                       ? FILE_TABLE_PARQUET : FILE_TABLE_CSV));
    options_.has_header_ = MY_SPEC.has_header_;
    options_.delimiter_ = static_cast<char>(MY_SPEC.delimiter_);
    options_.quote_ = static_cast<char>(MY_SPEC.quote_);
    options_.escape_ = static_cast<char>(MY_SPEC.escape_);
    options_.null_string_ = MY_SPEC.null_string_;
    options_.sample_size_ = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_names_.count(); ++i) {
      FileColumnMeta meta;
      meta.column_name_ = MY_SPEC.column_names_.at(i);
      meta.column_type_ = static_cast<FileColumnType>(MY_SPEC.column_types_.at(i));
      meta.nullable_ = (0 != MY_SPEC.column_nullables_.at(i));
      meta.source_index_ = i;
      if (OB_FAIL(columns_.push_back(meta))) {
        LOG_WARN("failed to push back column", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (3 == MY_SPEC.function_type_) {
      glob_idx_ = 0;
      if (OB_FAIL(file_sql_glob(MY_SPEC.file_path_, ctx_.get_allocator(), glob_files_))) {
        LOG_WARN("failed to expand glob pattern", K(ret), K(MY_SPEC.file_path_));
      }
    } else if (OB_FAIL(scanner_.open(MY_SPEC.file_path_, options_, columns_,
                                     ctx_.get_allocator()))) {
      LOG_WARN("failed to open file scanner", K(ret), K(MY_SPEC.file_path_));
    }
    if (OB_SUCC(ret)) {
      opened_ = true;
    }
  }
  return ret;
}

int ObFileScanOp::check_file_unchanged()
{
  int ret = OB_SUCCESS;
  if (3 == MY_SPEC.function_type_) {
    // glob results are produced from the current directory state.
  } else if (MY_SPEC.file_size_ >= 0) {
    FileStatInfo stat_info;
    if (OB_FAIL(get_file_stat(MY_SPEC.file_path_, stat_info))) {
    } else if (stat_info.size_ != MY_SPEC.file_size_ ||
               stat_info.mtime_ != MY_SPEC.file_mtime_ ||
               (MY_SPEC.file_ino_ != 0 && stat_info.ino_ != MY_SPEC.file_ino_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("file changed after the query was resolved", K(ret),
               K(MY_SPEC.file_path_), K(MY_SPEC.file_size_), K(stat_info.size_),
               K(MY_SPEC.file_mtime_), K(stat_info.mtime_));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file changed after the query was resolved");
    }
  }
  return ret;
}

int ObFileScanOp::fill_datum(ObExpr *expr, ObDatum &datum, int64_t column_idx,
                             int64_t datum_idx)
{
  int ret = OB_SUCCESS;
  if (3 == MY_SPEC.function_type_) {
    // handled by the caller
    ret = OB_ERR_UNEXPECTED;
  } else if (scanner_.is_null(column_idx)) {
    datum.set_null();
  } else {
    const ObString &text = scanner_.get_field(column_idx);
    switch (expr->datum_meta_.type_) {
      case ObIntType:
      case ObInt32Type:
      case ObUInt64Type: {
        char buf[32];
        const int64_t len = text.length() < 31 ? text.length() : 31;
        MEMCPY(buf, text.ptr(), len);
        buf[len] = '\0';
        char *end = NULL;
        const int64_t value = strtoll(buf, &end, 10);
        if (NULL == end || '\0' != *end) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid integer in file", K(text), K(ret));
        } else {
          datum.set_int(value);
        }
        break;
      }
      case ObDoubleType:
      case ObFloatType:
      case ObNumberType: {
        char buf[64];
        const int64_t len = text.length() < 63 ? text.length() : 63;
        MEMCPY(buf, text.ptr(), len);
        buf[len] = '\0';
        char *end = NULL;
        const double value = strtod(buf, &end);
        if (NULL == end || '\0' != *end) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid double in file", K(text), K(ret));
        } else {
          datum.set_double(value);
        }
        break;
      }
      case ObTinyIntType: {
        int64_t value = 0;
        if (text.length() == 4 && 0 == strncasecmp(text.ptr(), "true", 4)) {
          value = 1;
        } else if (text.length() == 5 && 0 == strncasecmp(text.ptr(), "false", 5)) {
          value = 0;
        } else if (text.length() == 1 && ('0' == text.ptr()[0] || '1' == text.ptr()[0])) {
          value = text.ptr()[0] - '0';
        } else {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid boolean in file", K(text), K(ret));
        }
        if (OB_SUCC(ret)) {
          datum.set_int(value);
        }
        break;
      }
      case ObDateType: {
        int32_t value = 0;
        if (OB_FAIL(file_sql_text_to_date(text.ptr(), text.length(), value))) {
          LOG_WARN("invalid date in file", K(text), K(ret));
        } else {
          datum.set_date(value);
        }
        break;
      }
      case ObDateTimeType:
      case ObTimestampType: {
        int64_t value = 0;
        if (OB_FAIL(file_sql_text_to_datetime(text.ptr(), text.length(), value))) {
          LOG_WARN("invalid datetime in file", K(text), K(ret));
        } else {
          datum.set_datetime(value);
        }
        break;
      }
      default: {
        char *buf = expr->get_str_res_mem(eval_ctx_, text.length(), datum_idx);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMCPY(buf, text.ptr(), text.length());
          datum.set_string(buf, text.length());
        }
        break;
      }
    }
  }
  return ret;
}

int ObFileScanOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (!opened_) {
    ret = OB_NOT_INIT;
  } else if (3 == MY_SPEC.function_type_) {
    if (glob_idx_ >= glob_files_.count()) {
      ret = OB_ITER_END;
    } else if (MY_SPEC.output_.empty()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObExpr *expr = MY_SPEC.output_.at(0);
      ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
      const ObString &file = glob_files_.at(glob_idx_++);
      char *copy = expr->get_str_res_mem(eval_ctx_, file.length());
      if (OB_ISNULL(copy)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(copy, file.ptr(), file.length());
        datum.set_string(copy, file.length());
        expr->set_evaluated_projected(eval_ctx_);
      }
    }
  } else if (OB_FAIL(scanner_.next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to read next file row", K(ret));
    }
  } else {
    const int64_t output_count = MY_SPEC.output_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < output_count; ++i) {
      ObExpr *expr = MY_SPEC.output_.at(i);
      ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
      const int64_t datum_idx = expr->is_batch_result() ? eval_ctx_.get_batch_idx() : 0;
      const int64_t src_idx = i < MY_SPEC.output_column_indexes_.count()
                              ? MY_SPEC.output_column_indexes_.at(i) : i;
      if (src_idx < 0 || src_idx >= scanner_.get_column_count()) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(fill_datum(expr, datum, src_idx, datum_idx))) {
      } else {
        expr->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

int ObFileScanOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t batch_size = min(max_row_cnt, MY_SPEC.max_batch_size_);
  int64_t row_cnt = 0;
  const int64_t output_count = MY_SPEC.output_.count();
  clear_evaluated_flag();
  brs_.size_ = 0;
  brs_.end_ = false;
  while (OB_SUCC(ret) && row_cnt < batch_size) {
    if (3 == MY_SPEC.function_type_) {
      if (glob_idx_ >= glob_files_.count()) {
        brs_.end_ = true;
        break;
      } else if (MY_SPEC.output_.empty()) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObExpr *expr = MY_SPEC.output_.at(0);
        ObDatum *datums = expr->locate_datums_for_update(eval_ctx_, batch_size);
        const ObString &file = glob_files_.at(glob_idx_++);
        char *copy = expr->get_str_res_mem(eval_ctx_, file.length(), row_cnt);
        if (OB_ISNULL(copy)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMCPY(copy, file.ptr(), file.length());
          datums[row_cnt].set_string(copy, file.length());
          ++row_cnt;
        }
      }
    } else if (OB_FAIL(scanner_.next_row())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        brs_.end_ = true;
        break;
      }
      LOG_WARN("failed to read next file row", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < output_count; ++i) {
        ObExpr *expr = MY_SPEC.output_.at(i);
        ObDatum *datums = expr->locate_datums_for_update(eval_ctx_, batch_size);
        const int64_t src_idx = i < MY_SPEC.output_column_indexes_.count()
                                ? MY_SPEC.output_column_indexes_.at(i) : i;
        if (src_idx < 0 || src_idx >= scanner_.get_column_count()) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(fill_datum(expr, datums[row_cnt], src_idx, row_cnt))) {
        }
      }
      ++row_cnt;
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < output_count; ++i) {
      MY_SPEC.output_.at(i)->set_evaluated_flag(eval_ctx_);
    }
    brs_.size_ = row_cnt;
  }
  return ret;
}

int ObFileScanOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else if (opened_ && OB_FAIL(scanner_.open(MY_SPEC.file_path_, options_, columns_,
                                              ctx_.get_allocator()))) {
    LOG_WARN("failed to rescan file", K(ret));
  }
  return ret;
}

int ObFileScanOp::inner_close()
{
  int ret = OB_SUCCESS;
  scanner_.close();
  opened_ = false;
  return ret;
}

void ObFileScanOp::destroy()
{
  scanner_.close();
  opened_ = false;
  ObOperator::destroy();
}

} // namespace sql
} // namespace oceanbase
