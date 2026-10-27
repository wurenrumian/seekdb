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

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_file_scan_utils.h"

namespace oceanbase
{
namespace sql
{

class ObFileScanSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObFileScanSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      alloc_(&alloc),
      file_path_(),
      function_type_(0),
      has_header_(true),
      delimiter_(','),
      quote_('"'),
      escape_('\\'),
      null_string_(),
      column_names_(alloc),
      column_types_(alloc),
      column_nullables_(alloc),
      output_column_indexes_(alloc),
      file_size_(-1),
      file_mtime_(-1),
      file_ino_(0),
      table_id_(common::OB_INVALID_ID),
      table_name_() {}

  common::ObIAllocator *alloc_;
  common::ObString file_path_;
  int64_t function_type_;
  bool has_header_;
  int64_t delimiter_;
  int64_t quote_;
  int64_t escape_;
  common::ObString null_string_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> column_names_;
  common::ObFixedArray<int64_t, common::ObIAllocator> column_types_;
  common::ObFixedArray<int64_t, common::ObIAllocator> column_nullables_;
  // Index of the source column in the scanned file for each output expr.
  common::ObFixedArray<int64_t, common::ObIAllocator> output_column_indexes_;
  int64_t file_size_;
  int64_t file_mtime_;
  int64_t file_ino_;
  uint64_t table_id_;
  common::ObString table_name_;
};

class ObFileScanOp : public ObOperator
{
public:
  ObFileScanOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      scanner_(),
      options_(),
      columns_(),
      glob_files_(),
      glob_idx_(0),
      opened_(false) {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual void destroy() override;

private:
  int fill_datum(ObExpr *expr, ObDatum &datum, int64_t column_idx, int64_t datum_idx);
  int check_file_unchanged();
  // File scanner state
  ObFileTableScanner scanner_;
  FileScanOptions options_;
  common::ObSEArray<FileColumnMeta, 16> columns_;
  common::ObSEArray<common::ObString, 16> glob_files_;
  int64_t glob_idx_;
  bool opened_;
};

} // namespace sql
} // namespace oceanbase
