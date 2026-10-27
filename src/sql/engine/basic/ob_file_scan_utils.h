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

#pragma once

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array.h"
#include "common/object/ob_obj_type.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{

// File table formats supported by read_csv/read_json/read_parquet.
enum FileTableFormat : int64_t
{
  FILE_TABLE_CSV = 0,
  FILE_TABLE_JSONL = 1,
  FILE_TABLE_PARQUET = 2,
};

// Logical column type inferred from file content. It is intentionally
// independent from ObObjType so that schema inference never depends on the
// executor; FileColumnMeta::get_obj_type() maps it to the engine type.
enum FileColumnType : int64_t
{
  FILE_COL_NULL = 0,      // all values are NULL, exposed as VARCHAR
  FILE_COL_BIGINT = 1,
  FILE_COL_DOUBLE = 2,
  FILE_COL_BOOLEAN = 3,
  FILE_COL_VARCHAR = 4,
  FILE_COL_DATE = 5,
  FILE_COL_DATETIME = 6,
};

struct FileColumnMeta
{
  FileColumnMeta()
    : column_name_(),
      column_type_(FILE_COL_NULL),
      nullable_(true),
      source_index_(0) {}
  common::ObString column_name_;
  FileColumnType column_type_;
  bool nullable_;
  // index of this column in the source file (JSONL may skip/reorder fields)
  int64_t source_index_;

  common::ObObjType get_obj_type() const;
  // DuckDB style type name, used by "describe select * from read_csv(...)"
  const char *get_type_name() const;
};

struct FileScanOptions
{
  FileScanOptions()
    : format_(FILE_TABLE_CSV),
      has_header_(true),
      delimiter_(','),
      quote_('"'),
      escape_('\\'),
      allow_quoted_newline_(true),
      sample_size_(-1),
      null_string_(),
      json_format_() {}
  FileTableFormat format_;
  bool has_header_;
  char delimiter_;
  char quote_;
  char escape_;
  bool allow_quoted_newline_;
  // -1 means "infer from the whole file", which is the only supported value.
  int64_t sample_size_;
  common::ObString null_string_;
  // read_json(format = 'newline_delimited')
  common::ObString json_format_;
};

// Snapshot of a file at resolve time, used to detect changes before scanning.
struct FileStatInfo
{
  FileStatInfo() : size_(-1), mtime_(-1), ino_(0) {}
  int64_t size_;
  int64_t mtime_;
  int64_t ino_;
  TO_STRING_KV(K_(size), K_(mtime), K_(ino));
};

// Infer the full schema of a file. Only metadata is kept: the file is streamed
// once and never materialized. sample_size_ must be -1.
int infer_file_schema(const common::ObString &filename,
                      const FileScanOptions &options,
                      common::ObIAllocator &alloc,
                      common::ObIArray<FileColumnMeta> &columns,
                      FileStatInfo &stat_info);

int get_file_stat(const common::ObString &filename, FileStatInfo &stat_info);

// glob() table function: expand a shell style pattern into file paths.
int file_sql_glob(const common::ObString &pattern,
                  common::ObIAllocator &alloc,
                  common::ObIArray<common::ObString> &files);

// Streaming row iterator over a file. A row is exposed as a list of raw text
// fields; the caller converts them to datums according to the column types.
class ObFileTableScanner
{
public:
  ObFileTableScanner();
  ~ObFileTableScanner();

  int open(const common::ObString &filename,
           const FileScanOptions &options,
           const common::ObIArray<FileColumnMeta> &columns,
           common::ObIAllocator &alloc);
  // Returns OB_ITER_END after the last row.
  int next_row();
  void close();
  void reuse();

  int64_t get_column_count() const { return columns_.count(); }
  const common::ObString &get_field(int64_t idx) const { return fields_.at(idx); }
  bool is_null(int64_t idx) const { return nulls_.at(idx); }
  int64_t get_row_count() const { return row_count_; }

private:
  struct Impl;
  Impl *impl_;
  common::ObArenaAllocator inner_alloc_;
  common::ObSEArray<common::ObString, 16> fields_;
  common::ObSEArray<bool, 16> nulls_;
  common::ObSEArray<FileColumnMeta, 16> columns_;
  FileScanOptions options_;
  int64_t row_count_;
  bool opened_;
  DISALLOW_COPY_AND_ASSIGN(ObFileTableScanner);
};

// Convert a raw text field into a date/datetime value. Exposed for testing.
int file_sql_text_to_date(const char *ptr, int64_t len, int32_t &value);
int file_sql_text_to_datetime(const char *ptr, int64_t len, int64_t &value);

} // namespace sql
} // namespace oceanbase

