/**
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

#include "common/object/ob_obj_type.h"
#include "share/schema/ob_column_schema.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObString;
class ObObjPrintParams;
}
namespace sql
{

class ObEvalCtx;
class ObExecContext;

// Enqueues pending AI column regeneration tasks into
// `oceanbase.__all_ai_column_task`, and removes them again when the target row
// disappears.
//
// The rows are written through ObAccessService with the session's current
// transaction descriptor, so they belong to the user DML's transaction: reading
// `__all_ai_column_task` inside the same transaction sees them, and a rollback
// removes them. Opening a session inner connection instead is not an option:
// `ObExecContext::get_sql_proxy()` is NULL on the plain DML path, which makes
// `open_inner_conn()` fail with -4016.
//
// The task row key is (table_id, column_id, task_id). `task_id` is derived
// deterministically from (table_id, rowkey_predicate), which makes enqueue
// idempotent (a re-queued row overwrites its pending task instead of
// duplicating it) and lets delete clean up without scanning the inner table.
class ObAiColumnTaskWriter
{
public:
  // Build the human readable primary key predicate of a row image, e.g.
  // "`id` = 2". The acceptance tests compare this string literally.
  //
  // @param row_exprs     new row image of the DML operator
  // @param column_ids    column ids positionally aligned with row_exprs
  // @param rowkey_ids    primary key column ids, in rowkey order
  // @param rowkey_names  primary key column names, aligned with rowkey_ids
  static int build_rowkey_predicate(ObEvalCtx &eval_ctx,
                                    const common::ObIArray<ObExpr *> &row_exprs,
                                    const common::ObIArray<uint64_t> &column_ids,
                                    const common::ObIArray<uint64_t> &rowkey_ids,
                                    const common::ObIArray<common::ObString> &rowkey_names,
                                    const common::ObObjPrintParams &print_params,
                                    common::ObIAllocator &allocator,
                                    common::ObString &predicate);

  // Enqueue one pending task per AI column for the given row.
  static int enqueue_tasks(ObExecContext &exec_ctx,
                           uint64_t table_id,
                           const common::ObIArray<uint64_t> &ai_column_ids,
                           const common::ObString &rowkey_predicate);

  // Collect the AI columns to regenerate for one modified row.
  //
  // @param schema_guard          runtime schema guard
  // @param table_id              data table being modified
  // @param is_insert_or_new_row  true for INSERT (all AI columns of the row are
  //                              new), false for UPDATE (only the columns whose
  //                              dependencies changed, as marked by the
  //                              resolver's implicit invalidation)
  // @param ai_invalidate_ids     AI column ids flagged during resolution
  // @param out                   resulting AI column ids
  static int collect_ai_columns(share::schema::ObSchemaGetterGuard &schema_guard,
                                uint64_t table_id,
                                bool is_insert_or_new_row,
                                const common::ObIArray<uint64_t> &ai_invalidate_ids,
                                common::ObIArray<uint64_t> &out);

  // Primary key column ids / names of a data table, for rowkey predicate
  // rendering. Names are returned as pointers into the schema.
  static int get_rowkey_columns(share::schema::ObSchemaGetterGuard &schema_guard,
                                uint64_t table_id,
                                common::ObIArray<uint64_t> &rowkey_ids,
                                common::ObIArray<common::ObString> &rowkey_names);

  // Remove the pending tasks of a row that is about to disappear.
  //
  // @param column_ids   task table column ids to delete by
  // @param rowkey_ids   task table primary key column ids, in rowkey order
  static int purge_tasks(ObExecContext &exec_ctx,
                         uint64_t table_id,
                         const common::ObIArray<uint64_t> &ai_column_ids,
                         const common::ObString &rowkey_predicate);

  // Group key shared by all tasks of one row level change, so the worker can
  // publish the group atomically.
  static int64_t gen_request_id();

private:
  // Deterministic task id for (table, ai column, row). Keeps enqueue idempotent
  // and makes the task row addressable by primary key without a scan.
  static uint64_t gen_task_id(uint64_t table_id,
                              uint64_t column_id,
                              const common::ObString &rowkey_predicate);
};

} // namespace sql
} // namespace oceanbase
