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
#include "sql/engine/basic/ob_file_scan_utils.h"

namespace oceanbase
{
namespace sql
{

// Minimal Parquet reader: footer schema, multiple row groups, PLAIN and
// RLE_DICTIONARY encodings, optional columns, snappy/uncompressed pages.
class ObParquetReader
{
public:
  ObParquetReader();
  ~ObParquetReader();

  int open(const common::ObString &filename,
           const common::ObIArray<FileColumnMeta> &columns,
           common::ObIAllocator &alloc);
  // Returns OB_ITER_END after the last row.
  int next_row(common::ObIArray<common::ObString> &values,
               common::ObIArray<bool> &nulls);
  void close();

  struct Impl;
private:
  Impl *impl_;
  DISALLOW_COPY_AND_ASSIGN(ObParquetReader);
};

int infer_parquet_schema(const common::ObString &filename,
                         common::ObIAllocator &alloc,
                         common::ObIArray<FileColumnMeta> &columns,
                         FileStatInfo &stat_info);

} // namespace sql
} // namespace oceanbase

