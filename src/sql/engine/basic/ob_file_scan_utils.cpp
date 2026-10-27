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

#include "sql/engine/basic/ob_file_scan_utils.h"
#include "sql/engine/basic/ob_parquet_reader.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <glob.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

namespace oceanbase
{
namespace sql
{

namespace
{

const int64_t FILE_SQL_READ_BUFFER_SIZE = 64 * 1024;

bool is_digit(char c) { return c >= '0' && c <= '9'; }

int days_in_month(int64_t year, int64_t month)
{
  static const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int ret = 0;
  if (month < 1 || month > 12) {
    ret = 0;
  } else if (2 == month) {
    const bool leap = (0 == year % 4 && 0 != year % 100) || 0 == year % 400;
    ret = leap ? 29 : 28;
  } else {
    ret = days[month - 1];
  }
  return ret;
}

// Days since 1970-01-01, proleptic Gregorian calendar.
int64_t days_from_civil(int64_t y, int64_t m, int64_t d)
{
  y -= m <= 2 ? 1 : 0;
  const int64_t era = (y >= 0 ? y : y - 399) / 400;
  const int64_t yoe = y - era * 400;
  const int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe - 719468;
}

bool parse_fixed_digits(const char *ptr, int64_t len, int64_t pos, int64_t n, int64_t &value)
{
  bool ok = pos + n <= len;
  int64_t v = 0;
  for (int64_t i = 0; ok && i < n; ++i) {
    const char c = ptr[pos + i];
    if (!is_digit(c)) {
      ok = false;
    } else {
      v = v * 10 + (c - '0');
    }
  }
  if (ok) {
    value = v;
  }
  return ok;
}

bool parse_time_part(const char *ptr, int64_t len, int64_t pos,
                     int64_t &hour, int64_t &minute, int64_t &second, int64_t &frac_us)
{
  bool ok = parse_fixed_digits(ptr, len, pos, 2, hour) &&
            pos + 5 <= len && ':' == ptr[pos + 2] &&
            parse_fixed_digits(ptr, len, pos + 3, 2, minute) &&
            ':' == ptr[pos + 5] &&
            parse_fixed_digits(ptr, len, pos + 6, 2, second);
  frac_us = 0;
  if (ok) {
    pos += 8;
    if (pos < len && '.' == ptr[pos]) {
      ++pos;
      int64_t scale = 100000;
      bool has_digit = false;
      while (pos < len && is_digit(ptr[pos]) && scale > 0) {
        frac_us += (ptr[pos] - '0') * scale;
        scale /= 10;
        ++pos;
        has_digit = true;
      }
      while (pos < len && is_digit(ptr[pos])) {
        ++pos;
      }
      ok = has_digit;
    }
    ok = ok && pos == len;
  }
  return ok && hour < 24 && minute < 60 && second < 60;
}

} // anonymous namespace

int file_sql_text_to_date(const char *ptr, int64_t len, int32_t &value)
{
  int ret = OB_SUCCESS;
  int64_t year = 0;
  int64_t month = 0;
  int64_t day = 0;
  if (OB_ISNULL(ptr) || OB_UNLIKELY(10 != len) || '-' != ptr[4] || '-' != ptr[7] ||
      !parse_fixed_digits(ptr, len, 0, 4, year) ||
      !parse_fixed_digits(ptr, len, 5, 2, month) ||
      !parse_fixed_digits(ptr, len, 8, 2, day) ||
      month < 1 || month > 12 || day < 1 || day > days_in_month(year, month)) {
    ret = OB_INVALID_DATE_VALUE;
  } else {
    value = static_cast<int32_t>(days_from_civil(year, month, day));
  }
  return ret;
}

int file_sql_text_to_datetime(const char *ptr, int64_t len, int64_t &value)
{
  int ret = OB_SUCCESS;
  int64_t year = 0;
  int64_t month = 0;
  int64_t day = 0;
  int64_t hour = 0;
  int64_t minute = 0;
  int64_t second = 0;
  int64_t frac_us = 0;
  if (OB_ISNULL(ptr) || OB_UNLIKELY(len < 19) || '-' != ptr[4] || '-' != ptr[7] ||
      (' ' != ptr[10] && 'T' != ptr[10] && 't' != ptr[10]) ||
      !parse_fixed_digits(ptr, len, 0, 4, year) ||
      !parse_fixed_digits(ptr, len, 5, 2, month) ||
      !parse_fixed_digits(ptr, len, 8, 2, day) ||
      month < 1 || month > 12 || day < 1 || day > days_in_month(year, month) ||
      !parse_time_part(ptr, len, 11, hour, minute, second, frac_us)) {
    ret = OB_INVALID_DATE_VALUE;
  } else {
    const int64_t days = days_from_civil(year, month, day);
    value = ((days * 24 + hour) * 60 + minute) * 60 * 1000000L +
            second * 1000000L + frac_us;
  }
  return ret;
}

namespace
{

bool is_int_text(const char *ptr, int64_t len)
{
  if (len <= 0 || len > 20) {
    return false;
  }
  int64_t i = 0;
  if ('-' == ptr[0] || '+' == ptr[0]) {
    i = 1;
  }
  if (i >= len) {
    return false;
  }
  for (; i < len; ++i) {
    if (!is_digit(ptr[i])) {
      return false;
    }
  }
  errno = 0;
  char buf[32];
  MEMCPY(buf, ptr, len);
  buf[len] = '\0';
  char *end = NULL;
  (void)strtoll(buf, &end, 10);
  return 0 == errno && NULL != end && '\0' == *end;
}

bool is_double_text(const char *ptr, int64_t len)
{
  if (len <= 0 || len > 63) {
    return false;
  }
  bool has_digit = false;
  for (int64_t i = 0; i < len; ++i) {
    const char c = ptr[i];
    if (is_digit(c)) {
      has_digit = true;
    } else if ('.' != c && 'e' != c && 'E' != c && '+' != c && '-' != c) {
      return false;
    }
  }
  if (!has_digit) {
    return false;
  }
  char buf[64];
  MEMCPY(buf, ptr, len);
  buf[len] = '\0';
  char *end = NULL;
  errno = 0;
  (void)strtod(buf, &end);
  return 0 == errno && NULL != end && '\0' == *end;
}

bool is_bool_text(const char *ptr, int64_t len)
{
  return (4 == len && 0 == strncasecmp(ptr, "true", 4)) ||
         (5 == len && 0 == strncasecmp(ptr, "false", 5));
}

bool is_date_text(const char *ptr, int64_t len)
{
  int32_t value = 0;
  return OB_SUCCESS == file_sql_text_to_date(ptr, len, value);
}

bool is_datetime_text(const char *ptr, int64_t len)
{
  int64_t value = 0;
  return OB_SUCCESS == file_sql_text_to_datetime(ptr, len, value);
}

struct ColumnTypeFlags
{
  ColumnTypeFlags()
    : has_value_(false), has_int_(false), has_double_(false), has_bool_(false),
      has_date_(false), has_datetime_(false), has_string_(false), nullable_(false) {}
  void reset()
  {
    has_value_ = false;
    has_int_ = false;
    has_double_ = false;
    has_bool_ = false;
    has_date_ = false;
    has_datetime_ = false;
    has_string_ = false;
    nullable_ = false;
  }
  void add(bool is_null, const char *ptr, int64_t len)
  {
    if (is_null) {
      nullable_ = true;
    } else {
      has_value_ = true;
      if (is_int_text(ptr, len)) {
        has_int_ = true;
      } else if (is_double_text(ptr, len)) {
        has_double_ = true;
      } else if (is_bool_text(ptr, len)) {
        has_bool_ = true;
      } else if (is_date_text(ptr, len)) {
        has_date_ = true;
      } else if (is_datetime_text(ptr, len)) {
        has_datetime_ = true;
      } else {
        has_string_ = true;
      }
    }
  }
  FileColumnType get_type() const
  {
    FileColumnType type = FILE_COL_NULL;
    if (!has_value_) {
      type = FILE_COL_NULL;
    } else if (has_string_) {
      type = FILE_COL_VARCHAR;
    } else if (has_bool_ && (has_int_ || has_double_ || has_date_ || has_datetime_)) {
      type = FILE_COL_VARCHAR;
    } else if ((has_int_ || has_double_ || has_bool_) &&
               (has_date_ || has_datetime_)) {
      type = FILE_COL_VARCHAR;
    } else if (has_int_ && has_double_) {
      type = FILE_COL_DOUBLE;
    } else if (has_double_) {
      type = FILE_COL_DOUBLE;
    } else if (has_int_) {
      type = FILE_COL_BIGINT;
    } else if (has_bool_) {
      type = FILE_COL_BOOLEAN;
    } else if (has_date_ && has_datetime_) {
      type = FILE_COL_DATETIME;
    } else if (has_date_) {
      type = FILE_COL_DATE;
    } else if (has_datetime_) {
      type = FILE_COL_DATETIME;
    }
    return type;
  }
  bool has_value_;
  bool has_int_;
  bool has_double_;
  bool has_bool_;
  bool has_date_;
  bool has_datetime_;
  bool has_string_;
  bool nullable_;
};

// Buffered, forward-only file reader. The whole file is never materialized.
class FileBufferReader
{
public:
  FileBufferReader() : fd_(-1), pos_(0), len_(0), eof_(false), file_size_(0) {}
  ~FileBufferReader() { close(); }

  int open(const common::ObString &filename)
  {
    int ret = OB_SUCCESS;
    char path[MAX_PATH_SIZE + 1];
    if (filename.empty() || filename.length() > MAX_PATH_SIZE) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid file name", K(filename), K(ret));
    } else {
      MEMCPY(path, filename.ptr(), filename.length());
      path[filename.length()] = '\0';
      fd_ = ::open(path, O_RDONLY);
      if (fd_ < 0) {
        ret = OB_FILE_NOT_EXIST;
        LOG_WARN("failed to open file", K(filename), K(errno), K(ret));
        LOG_USER_ERROR(OB_FILE_NOT_EXIST, path);
      } else {
        struct stat st;
        if (0 == ::fstat(fd_, &st)) {
          file_size_ = st.st_size;
        }
      }
    }
    return ret;
  }

  void close()
  {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
    pos_ = 0;
    len_ = 0;
    eof_ = false;
  }

  // Returns -1 on EOF, otherwise the byte value [0, 255].
  int peek()
  {
    int c = -1;
    if (pos_ < len_) {
      c = static_cast<unsigned char>(buf_[pos_]);
    } else if (!eof_ && OB_SUCCESS == fill()) {
      if (pos_ < len_) {
        c = static_cast<unsigned char>(buf_[pos_]);
      }
    }
    return c;
  }

  int get()
  {
    const int c = peek();
    if (c >= 0) {
      ++pos_;
    }
    return c;
  }

  bool eof() { return peek() < 0; }
  int64_t file_size() const { return file_size_; }

private:
  int fill()
  {
    int ret = OB_SUCCESS;
    if (eof_) {
      ret = OB_ITER_END;
    } else {
      ssize_t n = 0;
      do {
        n = ::read(fd_, buf_, FILE_SQL_READ_BUFFER_SIZE);
      } while (n < 0 && EINTR == errno);
      if (n < 0) {
        ret = OB_IO_ERROR;
        LOG_WARN("failed to read file", K(errno), K(ret));
      } else {
        pos_ = 0;
        len_ = n;
        eof_ = (0 == n);
      }
    }
    return ret;
  }

  static const int64_t MAX_PATH_SIZE = 4096;
  int fd_;
  char buf_[FILE_SQL_READ_BUFFER_SIZE];
  int64_t pos_;
  int64_t len_;
  bool eof_;
  int64_t file_size_;
};

// A growable character buffer owned by the scanner.
class RecordBuffer
{
public:
  int init(common::ObIAllocator &alloc)
  {
    alloc_ = &alloc;
    return OB_SUCCESS;
  }
  void reset() { len_ = 0; }
  int append(char c)
  {
    int ret = OB_SUCCESS;
    if (len_ + 1 > cap_ && OB_FAIL(reserve(len_ + 1))) {
    } else {
      buf_[len_++] = c;
    }
    return ret;
  }
  int append(const char *ptr, int64_t len)
  {
    int ret = OB_SUCCESS;
    if (len_ + len > cap_ && OB_FAIL(reserve(len_ + len))) {
    } else if (len > 0) {
      MEMCPY(buf_ + len_, ptr, len);
      len_ += len;
    }
    return ret;
  }
  int reserve(int64_t need)
  {
    int ret = OB_SUCCESS;
    if (need > cap_) {
      int64_t new_cap = cap_ * 2;
      if (new_cap < need) {
        new_cap = need;
      }
      char *new_buf = static_cast<char *>(alloc_->alloc(new_cap));
      if (OB_ISNULL(new_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        if (len_ > 0) {
          MEMCPY(new_buf, buf_, len_);
        }
        buf_ = new_buf;
        cap_ = new_cap;
      }
    }
    return ret;
  }
  char *ptr() { return buf_; }
  int64_t length() const { return len_; }
  bool empty() const { return 0 == len_; }
private:
  common::ObIAllocator *alloc_ = nullptr;
  char *buf_ = nullptr;
  int64_t len_ = 0;
  int64_t cap_ = 0;
};

} // anonymous namespace

common::ObObjType FileColumnMeta::get_obj_type() const
{
  common::ObObjType type = common::ObVarcharType;
  switch (column_type_) {
    case FILE_COL_BIGINT:
      type = common::ObIntType;
      break;
    case FILE_COL_DOUBLE:
      type = common::ObDoubleType;
      break;
    case FILE_COL_BOOLEAN:
      type = common::ObTinyIntType;
      break;
    case FILE_COL_DATE:
      type = common::ObDateType;
      break;
    case FILE_COL_DATETIME:
      type = common::ObDateTimeType;
      break;
    case FILE_COL_VARCHAR:
    case FILE_COL_NULL:
    default:
      type = common::ObVarcharType;
      break;
  }
  return type;
}

const char *FileColumnMeta::get_type_name() const
{
  const char *name = "VARCHAR";
  switch (column_type_) {
    case FILE_COL_BIGINT:
      name = "BIGINT";
      break;
    case FILE_COL_DOUBLE:
      name = "DOUBLE";
      break;
    case FILE_COL_BOOLEAN:
      name = "BOOLEAN";
      break;
    case FILE_COL_DATE:
      name = "DATE";
      break;
    case FILE_COL_DATETIME:
      name = "DATETIME";
      break;
    case FILE_COL_VARCHAR:
    case FILE_COL_NULL:
    default:
      name = "VARCHAR";
      break;
  }
  return name;
}

int get_file_stat(const common::ObString &filename, FileStatInfo &stat_info)
{
  int ret = OB_SUCCESS;
  char path[4097];
  struct stat st;
  if (filename.empty() || filename.length() > 4096) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(path, filename.ptr(), filename.length());
    path[filename.length()] = '\0';
    if (0 != ::stat(path, &st)) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("failed to stat file", K(filename), K(errno), K(ret));
    } else if (!S_ISREG(st.st_mode)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("file is not a regular file", K(filename), K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file is not a regular file");
    } else {
      stat_info.size_ = st.st_size;
      stat_info.mtime_ = static_cast<int64_t>(st.st_mtime);
      stat_info.ino_ = static_cast<int64_t>(st.st_ino);
    }
  }
  return ret;
}

int file_sql_glob(const common::ObString &pattern,
                  common::ObIAllocator &alloc,
                  common::ObIArray<common::ObString> &files)
{
  int ret = OB_SUCCESS;
  char buf[4097];
  glob_t glob_buf;
  MEMSET(&glob_buf, 0, sizeof(glob_buf));
  if (pattern.empty() || pattern.length() > 4096) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid glob pattern", K(pattern), K(ret));
  } else {
    MEMCPY(buf, pattern.ptr(), pattern.length());
    buf[pattern.length()] = '\0';
    const int glob_ret = ::glob(buf, 0, NULL, &glob_buf);
    if (0 != glob_ret && GLOB_NOMATCH != glob_ret) {
      ret = OB_IO_ERROR;
      LOG_WARN("glob failed", K(pattern), K(glob_ret), K(ret));
    } else if (GLOB_NOMATCH != glob_ret) {
      for (size_t i = 0; OB_SUCC(ret) && i < glob_buf.gl_pathc; ++i) {
        const char *path = glob_buf.gl_pathv[i];
        const int64_t len = static_cast<int64_t>(strlen(path));
        char *copy = static_cast<char *>(alloc.alloc(len + 1));
        if (OB_ISNULL(copy)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMCPY(copy, path, len);
          copy[len] = '\0';
          common::ObString value(static_cast<int32_t>(len), copy);
          if (OB_FAIL(files.push_back(value))) {
            LOG_WARN("failed to push back file", K(ret));
          }
        }
      }
    }
    ::globfree(&glob_buf);
  }
  return ret;
}

namespace
{

// Parses one CSV record at a time. Fields reference the internal record buffer
// and stay valid until the next call to next_record().
class CsvRecordParser
{
public:
  CsvRecordParser() : reader_(nullptr), opts_(nullptr) {}
  int init(FileBufferReader *reader, common::ObIAllocator &alloc,
           const FileScanOptions *opts)
  {
    int ret = OB_SUCCESS;
    reader_ = reader;
    opts_ = opts;
    if (OB_FAIL(record_.init(alloc))) {
    }
    return ret;
  }

  int next_record(bool &eof)
  {
    int ret = OB_SUCCESS;
    eof = false;
    record_.reset();
    fields_.reuse();
    nulls_.reuse();
    if (nullptr == reader_ || nullptr == opts_) {
      ret = OB_NOT_INIT;
    } else if (reader_->peek() < 0) {
      eof = true;
    } else {
      bool in_quote = false;
      bool field_quoted = false;
      bool field_started = false;
      int64_t field_offset = record_.length();
      bool finished = false;
      while (OB_SUCC(ret) && !finished) {
        const int c = reader_->get();
        if (c < 0) {
          finished = true;
        } else if (in_quote) {
          if (c == static_cast<unsigned char>(opts_->quote_)) {
            const int next = reader_->peek();
            if (next == static_cast<unsigned char>(opts_->quote_)) {
              reader_->get();
              ret = record_.append(static_cast<char>(c));
            } else {
              in_quote = false;
            }
          } else if (opts_->escape_ != opts_->quote_ &&
                     c == static_cast<unsigned char>(opts_->escape_)) {
            const int next = reader_->get();
            if (next >= 0) {
              ret = record_.append(static_cast<char>(next));
            } else {
              ret = record_.append(static_cast<char>(c));
            }
          } else {
            ret = record_.append(static_cast<char>(c));
          }
        } else if (!field_started && c == static_cast<unsigned char>(opts_->quote_)) {
          in_quote = true;
          field_quoted = true;
          field_started = true;
        } else if (c == static_cast<unsigned char>(opts_->delimiter_)) {
          if (OB_FAIL(end_field(field_offset, field_quoted))) {
          } else {
            field_offset = record_.length();
            field_quoted = false;
            field_started = false;
          }
        } else if ('\n' == c || '\r' == c) {
          if ('\r' == c && '\n' == reader_->peek()) {
            reader_->get();
          }
          if (OB_FAIL(end_field(field_offset, field_quoted))) {
          } else {
            finished = true;
          }
        } else {
          field_started = true;
          ret = record_.append(static_cast<char>(c));
        }
      }
      if (OB_SUCC(ret) && !finished) {
        // EOF without a trailing newline.
        if (OB_FAIL(end_field(field_offset, field_quoted))) {
        }
      }
      if (OB_SUCC(ret) && 1 == fields_.count() && nulls_.at(0)) {
        // Skip blank lines.
        if (reader_->peek() < 0) {
          eof = true;
        } else {
          ret = next_record(eof);
        }
      }
    }
    return ret;
  }

  common::ObIArray<common::ObString> &get_fields() { return fields_; }
  common::ObIArray<bool> &get_nulls() { return nulls_; }

private:
  int end_field(int64_t offset, bool quoted)
  {
    int ret = OB_SUCCESS;
    const int64_t len = record_.length() - offset;
    common::ObString value(static_cast<int32_t>(len), record_.ptr() + offset);
    if (OB_FAIL(fields_.push_back(value))) {
    } else if (OB_FAIL(nulls_.push_back(!quoted && 0 == len))) {
    }
    return ret;
  }

  FileBufferReader *reader_;
  const FileScanOptions *opts_;
  RecordBuffer record_;
  common::ObSEArray<common::ObString, 16> fields_;
  common::ObSEArray<bool, 16> nulls_;
};

// Minimal flat JSON parser: objects of scalar values only. Nested objects and
// arrays are rejected as required by the file SQL specification.
class JsonLineParser
{
public:
  JsonLineParser() : ptr_(nullptr), end_(nullptr) {}
  int init(common::ObIAllocator &alloc)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(value_buf_.init(alloc))) {
    } else if (OB_FAIL(key_buf_.init(alloc))) {
    }
    return ret;
  }

  int parse(const char *data, int64_t len)
  {
    int ret = OB_SUCCESS;
    ptr_ = data;
    end_ = data + len;
    keys_.reuse();
    values_.reuse();
    nulls_.reuse();
    value_buf_.reset();
    key_buf_.reset();
    skip_space();
    if (ptr_ >= end_ || '{' != *ptr_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("json line is not an object", K(ret));
    } else {
      ++ptr_;
      skip_space();
      if (ptr_ < end_ && '}' == *ptr_) {
        ++ptr_;
      } else {
        bool done = false;
        while (OB_SUCC(ret) && !done) {
          common::ObString key;
          if (OB_FAIL(parse_string(key_buf_, key))) {
          } else {
            skip_space();
            if (ptr_ >= end_ || ':' != *ptr_) {
              ret = OB_INVALID_ARGUMENT;
            } else {
              ++ptr_;
              ObString value;
              bool is_null = false;
              if (OB_FAIL(parse_value(value, is_null))) {
              } else if (OB_FAIL(keys_.push_back(key))) {
              } else if (OB_FAIL(values_.push_back(value))) {
              } else if (OB_FAIL(nulls_.push_back(is_null))) {
              } else {
                skip_space();
                if (ptr_ < end_ && ',' == *ptr_) {
                  ++ptr_;
                  skip_space();
                } else if (ptr_ < end_ && '}' == *ptr_) {
                  ++ptr_;
                  done = true;
                } else {
                  ret = OB_INVALID_ARGUMENT;
                }
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        skip_space();
        if (ptr_ != end_) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("trailing characters after json object", K(ret));
        }
      }
    }
    return ret;
  }

  common::ObIArray<common::ObString> &get_keys() { return keys_; }
  common::ObIArray<common::ObString> &get_values() { return values_; }
  common::ObIArray<bool> &get_nulls() { return nulls_; }

private:
  void skip_space()
  {
    while (ptr_ < end_ && (' ' == *ptr_ || '\t' == *ptr_ || '\r' == *ptr_ || '\n' == *ptr_)) {
      ++ptr_;
    }
  }

  int parse_hex4(uint32_t &value)
  {
    int ret = OB_SUCCESS;
    if (ptr_ + 4 > end_) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      value = 0;
      for (int i = 0; OB_SUCC(ret) && i < 4; ++i) {
        const char c = ptr_[i];
        value <<= 4;
        if (c >= '0' && c <= '9') {
          value |= (c - '0');
        } else if (c >= 'a' && c <= 'f') {
          value |= (c - 'a' + 10);
        } else if (c >= 'A' && c <= 'F') {
          value |= (c - 'A' + 10);
        } else {
          ret = OB_INVALID_ARGUMENT;
        }
      }
      if (OB_SUCC(ret)) {
        ptr_ += 4;
      }
    }
    return ret;
  }

  int append_utf8(RecordBuffer &buf, uint32_t cp)
  {
    int ret = OB_SUCCESS;
    if (cp < 0x80) {
      ret = buf.append(static_cast<char>(cp));
    } else if (cp < 0x800) {
      ret = buf.append(static_cast<char>(0xC0 | (cp >> 6)));
      if (OB_SUCC(ret)) {
        ret = buf.append(static_cast<char>(0x80 | (cp & 0x3F)));
      }
    } else if (cp < 0x10000) {
      ret = buf.append(static_cast<char>(0xE0 | (cp >> 12)));
      if (OB_SUCC(ret)) {
        ret = buf.append(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
      }
      if (OB_SUCC(ret)) {
        ret = buf.append(static_cast<char>(0x80 | (cp & 0x3F)));
      }
    } else {
      ret = buf.append(static_cast<char>(0xF0 | (cp >> 18)));
      if (OB_SUCC(ret)) {
        ret = buf.append(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
      }
      if (OB_SUCC(ret)) {
        ret = buf.append(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
      }
      if (OB_SUCC(ret)) {
        ret = buf.append(static_cast<char>(0x80 | (cp & 0x3F)));
      }
    }
    return ret;
  }

  int parse_string(RecordBuffer &buf, common::ObString &value)
  {
    int ret = OB_SUCCESS;
    const int64_t start = buf.length();
    if (ptr_ >= end_ || '"' != *ptr_) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ++ptr_;
      bool closed = false;
      while (OB_SUCC(ret) && ptr_ < end_ && !closed) {
        const char c = *ptr_++;
        if ('"' == c) {
          closed = true;
        } else if ('\\' == c) {
          if (ptr_ >= end_) {
            ret = OB_INVALID_ARGUMENT;
          } else {
            const char esc = *ptr_++;
            switch (esc) {
              case '"': ret = buf.append('"'); break;
              case '\\': ret = buf.append('\\'); break;
              case '/': ret = buf.append('/'); break;
              case 'b': ret = buf.append('\b'); break;
              case 'f': ret = buf.append('\f'); break;
              case 'n': ret = buf.append('\n'); break;
              case 'r': ret = buf.append('\r'); break;
              case 't': ret = buf.append('\t'); break;
              case 'u': {
                uint32_t cp = 0;
                if (OB_FAIL(parse_hex4(cp))) {
                } else if (cp >= 0xD800 && cp <= 0xDBFF && ptr_ + 6 <= end_ &&
                           '\\' == ptr_[0] && 'u' == ptr_[1]) {
                  ptr_ += 2;
                  uint32_t low = 0;
                  if (OB_FAIL(parse_hex4(low))) {
                  } else if (low < 0xDC00 || low > 0xDFFF) {
                    ret = OB_INVALID_ARGUMENT;
                  } else {
                    cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                    ret = append_utf8(buf, cp);
                  }
                } else {
                  ret = append_utf8(buf, cp);
                }
                break;
              }
              default:
                ret = OB_INVALID_ARGUMENT;
                break;
            }
          }
        } else {
          ret = buf.append(c);
        }
      }
      if (OB_SUCC(ret) && !closed) {
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCC(ret)) {
        value.assign_ptr(buf.ptr() + start, static_cast<int32_t>(buf.length() - start));
      }
    }
    return ret;
  }

  int parse_value(common::ObString &value, bool &is_null)
  {
    int ret = OB_SUCCESS;
    is_null = false;
    skip_space();
    if (ptr_ >= end_) {
      ret = OB_INVALID_ARGUMENT;
    } else if ('"' == *ptr_) {
      ret = parse_string(value_buf_, value);
    } else if ('{' == *ptr_ || '[' == *ptr_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("nested json value is not supported", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "nested object or array in read_json");
    } else {
      const char *start = ptr_;
      while (ptr_ < end_ && ',' != *ptr_ && '}' != *ptr_ &&
             ' ' != *ptr_ && '\t' != *ptr_ && '\r' != *ptr_ && '\n' != *ptr_) {
        ++ptr_;
      }
      const int64_t len = ptr_ - start;
      if (0 == len) {
        ret = OB_INVALID_ARGUMENT;
      } else if (4 == len && 0 == strncasecmp(start, "null", 4)) {
        is_null = true;
      } else {
        value.assign_ptr(start, static_cast<int32_t>(len));
      }
    }
    return ret;
  }

  const char *ptr_;
  const char *end_;
  RecordBuffer value_buf_;
  RecordBuffer key_buf_;
  common::ObSEArray<common::ObString, 16> keys_;
  common::ObSEArray<common::ObString, 16> values_;
  common::ObSEArray<bool, 16> nulls_;
};

int read_line(FileBufferReader &reader, RecordBuffer &line, bool &eof)
{
  int ret = OB_SUCCESS;
  eof = false;
  line.reset();
  int c = reader.peek();
  if (c < 0) {
    eof = true;
  } else {
    while (OB_SUCC(ret) && (c = reader.get()) >= 0) {
      if ('\n' == c) {
        break;
      } else if ('\r' == c) {
        if ('\n' == reader.peek()) {
          reader.get();
        }
        break;
      } else if (OB_FAIL(line.append(static_cast<char>(c)))) {
        break;
      }
    }
  }
  return ret;
}

int append_column(common::ObIAllocator &alloc,
                  common::ObIArray<FileColumnMeta> &columns,
                  const char *name, int64_t len)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    if (0 == strncasecmp(columns.at(i).column_name_.ptr(), name,
                         static_cast<int32_t>(len)) &&
        static_cast<int32_t>(len) == columns.at(i).column_name_.length()) {
      ret = OB_ERR_COLUMN_DUPLICATE;
      LOG_WARN("duplicate column name", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t alloc_len = len + 1;
    char *copy = static_cast<char *>(alloc.alloc(alloc_len));
    FileColumnMeta meta;
    if (OB_ISNULL(copy)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(copy, name, len);
      copy[len] = '\0';
      meta.column_name_.assign_ptr(copy, static_cast<int32_t>(len));
      meta.column_type_ = FILE_COL_NULL;
      meta.nullable_ = false;
      meta.source_index_ = columns.count();
      if (OB_FAIL(columns.push_back(meta))) {
        LOG_WARN("failed to push back column", K(ret));
      }
    }
  }
  return ret;
}

// Generate a unique name for duplicate/empty CSV headers: id, id__2, id__3...
int make_unique_column_name(common::ObIAllocator &alloc,
                            common::ObIArray<FileColumnMeta> &columns,
                            const common::ObString &raw_name,
                            int64_t ordinal,
                            common::ObString &name)
{
  int ret = OB_SUCCESS;
  char base[OB_MAX_COLUMN_NAME_LENGTH + 32];
  int64_t base_len = raw_name.length();
  if (base_len <= 0) {
    base_len = snprintf(base, sizeof(base), "column%ld", static_cast<long>(ordinal));
  } else {
    if (base_len > OB_MAX_COLUMN_NAME_LENGTH) {
      base_len = OB_MAX_COLUMN_NAME_LENGTH;
    }
    MEMCPY(base, raw_name.ptr(), base_len);
  }
  char candidate[OB_MAX_COLUMN_NAME_LENGTH + 64];
  bool conflict = true;
  int64_t candidate_len = base_len;
  for (int64_t suffix = 1; OB_SUCC(ret) && conflict; ++suffix) {
    MEMCPY(candidate, base, base_len);
    candidate_len = base_len;
    if (suffix > 1) {
      candidate_len += snprintf(candidate + base_len, sizeof(candidate) - base_len,
                                "__%ld", static_cast<long>(suffix));
    }
    conflict = false;
    for (int64_t i = 0; !conflict && i < columns.count(); ++i) {
      if (columns.at(i).column_name_.length() == candidate_len &&
          0 == strncasecmp(columns.at(i).column_name_.ptr(), candidate,
                           static_cast<int32_t>(candidate_len))) {
        conflict = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    char *copy = static_cast<char *>(alloc.alloc(candidate_len + 1));
    if (OB_ISNULL(copy)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(copy, candidate, candidate_len);
      copy[candidate_len] = '\0';
      name.assign_ptr(copy, static_cast<int32_t>(candidate_len));
    }
  }
  return ret;
}

int infer_csv_schema(const common::ObString &filename,
                     const FileScanOptions &options,
                     common::ObIAllocator &alloc,
                     common::ObIArray<FileColumnMeta> &columns,
                     FileStatInfo &stat_info)
{
  int ret = OB_SUCCESS;
  FileBufferReader reader;
  CsvRecordParser parser;
  common::ObArenaAllocator tmp_alloc;
  common::ObSEArray<ColumnTypeFlags, 32> flags;
  if (OB_FAIL(reader.open(filename))) {
  } else if (OB_FAIL(parser.init(&reader, tmp_alloc, &options))) {
  } else if (OB_FAIL(get_file_stat(filename, stat_info))) {
  } else {
    bool eof = false;
    int64_t expected = -1;
    if (options.has_header_) {
      if (OB_FAIL(parser.next_record(eof)) || eof) {
        if (OB_SUCC(ret)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("empty csv file has no header", K(ret));
        }
      } else {
        common::ObIArray<common::ObString> &names = parser.get_fields();
        for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
          common::ObString unique;
          if (OB_FAIL(make_unique_column_name(alloc, columns, names.at(i), i, unique))) {
          } else if (OB_FAIL(append_column(alloc, columns, unique.ptr(), unique.length()))) {
          }
        }
        expected = names.count();
      }
    }
    while (OB_SUCC(ret) && !eof) {
      if (OB_FAIL(parser.next_record(eof))) {
      } else if (eof) {
        break;
      } else {
        common::ObIArray<common::ObString> &fields = parser.get_fields();
        common::ObIArray<bool> &nulls = parser.get_nulls();
        if (expected < 0) {
          expected = fields.count();
          for (int64_t i = 0; OB_SUCC(ret) && i < expected; ++i) {
            char name[32];
            const int64_t len = snprintf(name, sizeof(name), "column%ld",
                                         static_cast<long>(i));
            if (OB_FAIL(append_column(alloc, columns, name, len))) {
            }
          }
        }
        if (OB_SUCC(ret) && fields.count() != expected) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("csv row has wrong field count", K(fields.count()), K(expected), K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "csv row has a different number of fields than the header");
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < expected; ++i) {
          if (i >= flags.count()) {
            ColumnTypeFlags f;
            if (OB_FAIL(flags.push_back(f))) {
            }
          }
          if (OB_SUCC(ret)) {
            flags.at(i).add(nulls.at(i), fields.at(i).ptr(), fields.at(i).length());
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      FileColumnMeta &meta = columns.at(i);
      if (i < flags.count()) {
        meta.column_type_ = flags.at(i).get_type();
        meta.nullable_ = flags.at(i).nullable_ || (FILE_COL_NULL == meta.column_type_);
      } else {
        meta.column_type_ = FILE_COL_NULL;
        meta.nullable_ = true;
      }
    }
  }
  reader.close();
  return ret;
}

int infer_jsonl_schema(const common::ObString &filename,
                       const FileScanOptions &options,
                       common::ObIAllocator &alloc,
                       common::ObIArray<FileColumnMeta> &columns,
                       FileStatInfo &stat_info)
{
  int ret = OB_SUCCESS;
  FileBufferReader reader;
  JsonLineParser parser;
  RecordBuffer line;
  common::ObArenaAllocator tmp_alloc;
  common::ObSEArray<ColumnTypeFlags, 32> flags;
  common::ObSEArray<bool, 32> seen;
  if (OB_FAIL(reader.open(filename))) {
  } else if (OB_FAIL(parser.init(tmp_alloc))) {
  } else if (OB_FAIL(line.init(tmp_alloc))) {
  } else if (OB_FAIL(get_file_stat(filename, stat_info))) {
  } else {
    bool eof = false;
    while (OB_SUCC(ret) && !eof) {
      if (OB_FAIL(read_line(reader, line, eof))) {
      } else if (eof) {
        break;
      } else if (line.empty()) {
        // skip blank lines
      } else if (OB_FAIL(parser.parse(line.ptr(), line.length()))) {
        LOG_WARN("failed to parse json line", K(ret));
      } else {
        common::ObIArray<common::ObString> &keys = parser.get_keys();
        common::ObIArray<common::ObString> &values = parser.get_values();
        common::ObIArray<bool> &nulls = parser.get_nulls();
        for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
          int64_t idx = -1;
          for (int64_t j = 0; idx < 0 && j < columns.count(); ++j) {
            if (columns.at(j).column_name_.length() == keys.at(i).length() &&
                0 == strncmp(columns.at(j).column_name_.ptr(), keys.at(i).ptr(),
                             keys.at(i).length())) {
              idx = j;
            }
          }
          if (idx < 0) {
            ColumnTypeFlags f;
            idx = columns.count();
            if (OB_FAIL(append_column(alloc, columns, keys.at(i).ptr(), keys.at(i).length()))) {
            } else if (OB_FAIL(flags.push_back(f))) {
            } else if (OB_FAIL(seen.push_back(false))) {
            }
          }
          if (OB_SUCC(ret)) {
            if (seen.count() < columns.count()) {
              bool init = false;
              while (OB_SUCC(ret) && seen.count() < columns.count()) {
                if (OB_FAIL(seen.push_back(init))) {
                }
              }
            }
            if (OB_SUCC(ret)) {
              flags.at(idx).add(nulls.at(i), values.at(i).ptr(), values.at(i).length());
              if (!nulls.at(i)) {
                seen.at(idx) = true;
              }
            }
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      FileColumnMeta &meta = columns.at(i);
      if (i < flags.count()) {
        meta.column_type_ = flags.at(i).get_type();
        meta.nullable_ = flags.at(i).nullable_ || (FILE_COL_NULL == meta.column_type_);
      } else {
        meta.column_type_ = FILE_COL_NULL;
        meta.nullable_ = true;
      }
    }
  }
  reader.close();
  return ret;
}

} // anonymous namespace

int infer_file_schema(const common::ObString &filename,
                      const FileScanOptions &options,
                      common::ObIAllocator &alloc,
                      common::ObIArray<FileColumnMeta> &columns,
                      FileStatInfo &stat_info)
{
  int ret = OB_SUCCESS;
  columns.reuse();
  if (OB_UNLIKELY(-1 != options.sample_size_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only sample_size = -1 is supported", K(options.sample_size_), K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sample_size other than -1");
  } else if (FILE_TABLE_CSV == options.format_) {
    ret = infer_csv_schema(filename, options, alloc, columns, stat_info);
  } else if (FILE_TABLE_JSONL == options.format_) {
    ret = infer_jsonl_schema(filename, options, alloc, columns, stat_info);
  } else if (FILE_TABLE_PARQUET == options.format_) {
    ret = infer_parquet_schema(filename, alloc, columns, stat_info);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported file format", K(options.format_), K(ret));
  }
  return ret;
}

struct ObFileTableScanner::Impl
{
  Impl() : format_(FILE_TABLE_CSV), parquet_(nullptr), eof_(false) {}
  FileTableFormat format_;
  FileBufferReader reader_;
  CsvRecordParser csv_parser_;
  JsonLineParser json_parser_;
  RecordBuffer json_line_;
  ObParquetReader *parquet_;
  bool eof_;
};

ObFileTableScanner::ObFileTableScanner()
  : impl_(nullptr), inner_alloc_(), fields_(), nulls_(), columns_(),
    options_(), row_count_(0), opened_(false)
{
}

ObFileTableScanner::~ObFileTableScanner()
{
  close();
}

int ObFileTableScanner::open(const common::ObString &filename,
                             const FileScanOptions &options,
                             const common::ObIArray<FileColumnMeta> &columns,
                             common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  (void)alloc;
  close();
  options_ = options;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    FileColumnMeta meta = columns.at(i);
    char *copy = static_cast<char *>(inner_alloc_.alloc(meta.column_name_.length() + 1));
    if (OB_ISNULL(copy)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(copy, meta.column_name_.ptr(), meta.column_name_.length());
      copy[meta.column_name_.length()] = '\0';
      meta.column_name_.assign_ptr(copy, meta.column_name_.length());
      if (OB_FAIL(columns_.push_back(meta))) {
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(impl_ = OB_NEWx(Impl, &inner_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    impl_->format_ = options_.format_;
    if (FILE_TABLE_CSV == options_.format_) {
      if (OB_FAIL(impl_->reader_.open(filename))) {
      } else if (OB_FAIL(impl_->csv_parser_.init(&impl_->reader_, inner_alloc_, &options_))) {
      } else if (options_.has_header_ && OB_SUCC(ret)) {
        bool eof = false;
        if (OB_FAIL(impl_->csv_parser_.next_record(eof)) || eof) {
          if (OB_SUCC(ret)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("csv file has no header", K(ret));
          }
        }
      }
    } else if (FILE_TABLE_JSONL == options_.format_) {
      if (OB_FAIL(impl_->reader_.open(filename))) {
      } else if (OB_FAIL(impl_->json_parser_.init(inner_alloc_))) {
      } else if (OB_FAIL(impl_->json_line_.init(inner_alloc_))) {
      }
    } else if (FILE_TABLE_PARQUET == options_.format_) {
      if (OB_ISNULL(impl_->parquet_ = OB_NEWx(ObParquetReader, &inner_alloc_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(impl_->parquet_->open(filename, columns_, inner_alloc_))) {
      }
    } else {
      ret = OB_NOT_SUPPORTED;
    }
  }
  if (OB_SUCC(ret)) {
    opened_ = true;
  }
  return ret;
}

int ObFileTableScanner::next_row()
{
  int ret = OB_SUCCESS;
  fields_.reuse();
  nulls_.reuse();
  if (OB_ISNULL(impl_) || !opened_) {
    ret = OB_NOT_INIT;
  } else if (impl_->eof_) {
    ret = OB_ITER_END;
  } else if (FILE_TABLE_CSV == impl_->format_) {
    bool eof = false;
    if (OB_FAIL(impl_->csv_parser_.next_record(eof))) {
    } else if (eof) {
      impl_->eof_ = true;
      ret = OB_ITER_END;
    } else {
      common::ObIArray<common::ObString> &fields = impl_->csv_parser_.get_fields();
      common::ObIArray<bool> &nulls = impl_->csv_parser_.get_nulls();
      if (fields.count() != columns_.count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("csv row has a different number of fields than the header",
                 K(fields.count()), K(columns_.count()), K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                       "csv row has a different number of fields than the header");
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
        if (OB_FAIL(fields_.push_back(fields.at(i)))) {
        } else if (OB_FAIL(nulls_.push_back(nulls.at(i)))) {
        }
      }
    }
  } else if (FILE_TABLE_JSONL == impl_->format_) {
    bool eof = false;
    if (OB_FAIL(read_line(impl_->reader_, impl_->json_line_, eof))) {
    } else if (eof) {
      impl_->eof_ = true;
      ret = OB_ITER_END;
    } else if (impl_->json_line_.empty()) {
      ret = next_row();
    } else if (OB_FAIL(impl_->json_parser_.parse(impl_->json_line_.ptr(),
                                                 impl_->json_line_.length()))) {
    } else {
      common::ObIArray<common::ObString> &keys = impl_->json_parser_.get_keys();
      common::ObIArray<common::ObString> &values = impl_->json_parser_.get_values();
      common::ObIArray<bool> &nulls = impl_->json_parser_.get_nulls();
      for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
        common::ObString empty;
        bool found = false;
        for (int64_t j = 0; !found && j < keys.count(); ++j) {
          if (keys.at(j).length() == columns_.at(i).column_name_.length() &&
              0 == strncmp(keys.at(j).ptr(), columns_.at(i).column_name_.ptr(),
                           keys.at(j).length())) {
            if (OB_FAIL(fields_.push_back(values.at(j)))) {
            } else if (OB_FAIL(nulls_.push_back(nulls.at(j)))) {
            } else {
              found = true;
            }
          }
        }
        if (OB_SUCC(ret) && !found) {
          if (OB_FAIL(fields_.push_back(empty))) {
          } else if (OB_FAIL(nulls_.push_back(true))) {
          }
        }
      }
    }
  } else if (FILE_TABLE_PARQUET == impl_->format_) {
    if (OB_ISNULL(impl_->parquet_)) {
      ret = OB_NOT_INIT;
    } else {
      ret = impl_->parquet_->next_row(fields_, nulls_);
      if (OB_ITER_END == ret) {
        impl_->eof_ = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    ++row_count_;
  }
  return ret;
}

void ObFileTableScanner::close()
{
  if (OB_NOT_NULL(impl_)) {
    if (OB_NOT_NULL(impl_->parquet_)) {
      impl_->parquet_->close();
    }
    impl_->reader_.close();
    impl_ = nullptr;
  }
  opened_ = false;
  fields_.reuse();
  nulls_.reuse();
  columns_.reuse();
  row_count_ = 0;
  inner_alloc_.reset();
}

void ObFileTableScanner::reuse()
{
  if (OB_NOT_NULL(impl_)) {
    impl_->eof_ = false;
  }
  fields_.reuse();
  nulls_.reuse();
  row_count_ = 0;
}

} // namespace sql
} // namespace oceanbase
