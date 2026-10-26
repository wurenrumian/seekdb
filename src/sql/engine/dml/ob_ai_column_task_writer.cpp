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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/dml/ob_ai_column_task_writer.h"

#include <algorithm>
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/table/ob_table_util.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_table_dml_param.h"
#include "storage/ob_value_row_iterator.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;

namespace sql
{

namespace
{

constexpr int64_t AI_TASK_PENDING_STATE = 0;

// Column ids of `__all_ai_column_task` in schema iteration order, plus the
// index of every column we need to fill. Iteration order is the rowkey columns
// first, then the normal columns -- it is NOT column_id order, so indices must
// be resolved by name rather than assumed.
struct TaskColumnIndex
{
  int64_t gmt_create_ = -1;
  int64_t gmt_modified_ = -1;
  int64_t table_id_ = -1;
  int64_t column_id_ = -1;
  int64_t task_id_ = -1;
  int64_t request_id_ = -1;
  int64_t rowkey_predicate_ = -1;
  int64_t eligible_ts_ = -1;
  int64_t state_ = -1;
  int64_t retry_count_ = -1;
  int64_t error_message_ = -1;

  bool complete() const
  {
    // error_message_ is intentionally excluded: it is a LOB column and is not
    // part of the row image we write.
    return gmt_create_ >= 0 && gmt_modified_ >= 0 && table_id_ >= 0
        && column_id_ >= 0 && task_id_ >= 0 && request_id_ >= 0
        && rowkey_predicate_ >= 0 && eligible_ts_ >= 0 && state_ >= 0
        && retry_count_ >= 0;
  }
};

// Column order expected by the storage layer: rowkey columns first (in rowkey
// order), then the remaining columns by column id. The schema iterator does not
// guarantee this, so the datum row must be built in an explicitly sorted order.
// Same convention as ob_cs_plugin_async_index.cpp::build_das_ins_ctdef_.
bool task_column_less(const ObColumnSchemaV2 *a, const ObColumnSchemaV2 *b)
{
  const int64_t a_pos = a->get_rowkey_position();
  const int64_t b_pos = b->get_rowkey_position();
  if (a_pos > 0 && b_pos > 0) {
    return a_pos < b_pos;
  }
  if (a_pos > 0) {
    return true;
  }
  if (b_pos > 0) {
    return false;
  }
  return a->get_column_id() < b->get_column_id();
}

int resolve_task_columns(const ObTableSchema &task_schema,
                         common::ObIAllocator &allocator,
                         ObIArray<uint64_t> &column_ids,
                         ObIArray<uint64_t> &rowkey_ids,
                         ObIArray<uint64_t> &write_column_ids,
                         TaskColumnIndex &idx)
{
  int ret = OB_SUCCESS;
  column_ids.reset();
  rowkey_ids.reset();
  write_column_ids.reset();
  ObSEArray<const ObColumnSchemaV2 *, 16> sorted_columns;
  for (ObTableSchema::const_column_iterator iter = task_schema.column_begin();
       OB_SUCC(ret) && iter != task_schema.column_end(); ++iter) {
    const ObColumnSchemaV2 *column = *iter;
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai column task: null column schema", K(ret));
    } else if (OB_FAIL(sorted_columns.push_back(column))) {
      LOG_WARN("ai column task: collect columns failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    std::sort(sorted_columns.begin(), sorted_columns.end(), task_column_less);
    for (int64_t i = 0; OB_SUCC(ret) && i < sorted_columns.count(); ++i) {
      const ObColumnSchemaV2 *column = sorted_columns.at(i);
      const ObString name = column->get_column_name_str();
      if (OB_FAIL(column_ids.push_back(column->get_column_id()))) {
        LOG_WARN("ai column task: push column id failed", K(ret));
      } else if (column->is_rowkey_column()
                 && OB_FAIL(rowkey_ids.push_back(column->get_column_id()))) {
        LOG_WARN("ai column task: push rowkey column id failed", K(ret));
      } else if (column->get_meta_type().is_lob_storage()) {
        // LOB columns are left out on purpose: the upsert path refuses to write
        // them, and every task starts with error_message unset anyway.
      } else {
        // `i` is the index inside the row image we actually write, which is the
        // write_column_ids order rather than the full schema order.
        const int64_t write_idx = write_column_ids.count();
        if (OB_FAIL(write_column_ids.push_back(column->get_column_id()))) {
          LOG_WARN("ai column task: push write column id failed", K(ret));
        } else if (0 == name.case_compare("gmt_create")) {
          idx.gmt_create_ = write_idx;
        } else if (0 == name.case_compare("gmt_modified")) {
          idx.gmt_modified_ = write_idx;
        } else if (0 == name.case_compare("table_id")) {
          idx.table_id_ = write_idx;
        } else if (0 == name.case_compare("column_id")) {
          idx.column_id_ = write_idx;
        } else if (0 == name.case_compare("task_id")) {
          idx.task_id_ = write_idx;
        } else if (0 == name.case_compare("request_id")) {
          idx.request_id_ = write_idx;
        } else if (0 == name.case_compare("rowkey_predicate")) {
          idx.rowkey_predicate_ = write_idx;
        } else if (0 == name.case_compare("eligible_ts")) {
          idx.eligible_ts_ = write_idx;
        } else if (0 == name.case_compare("state")) {
          idx.state_ = write_idx;
        } else if (0 == name.case_compare("retry_count")) {
          idx.retry_count_ = write_idx;
        } else if (0 == name.case_compare("error_message")) {
          idx.error_message_ = write_idx;
        }
      }
    }
  }
  UNUSED(allocator);
  if (OB_SUCC(ret) && !idx.complete()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ai column task: task table columns changed", K(ret),
             K(column_ids.count()), K(column_ids));
  }
  return ret;
}

int get_task_table_schema(ObSchemaGetterGuard &schema_guard,
                          const ObTableSchema *&task_schema,
                          ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  task_schema = nullptr;
  if (OB_FAIL(schema_guard.get_table_schema(OB_ALL_AI_COLUMN_TASK_TID, task_schema))) {
    LOG_WARN("ai column task: get task table schema failed", K(ret));
  } else if (OB_ISNULL(task_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("ai column task: task table schema not found", K(ret));
  } else if (OB_FAIL(table::ObTableUtils::get_tablet_id_by_part_idx(
                 schema_guard, OB_ALL_AI_COLUMN_TASK_TID, 0, 0, tablet_id))) {
    LOG_WARN("ai column task: resolve task tablet failed", K(ret));
  }
  return ret;
}

int init_dml_param(ObExecContext &exec_ctx,
                   const ObTableSchema &task_schema,
                   const ObIArray<uint64_t> &column_ids,
                   ObArenaAllocator &allocator,
                   ObTableDMLParam &table_param,
                   ObDMLBaseParam &dml_param,
                   ObStoreCtxGuard &store_ctx_guard,
                   transaction::ObTxDesc &tx_desc)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = share::server_service<ObAccessService>();
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  transaction::ObTxReadSnapshot snapshot;
  concurrent_control::ObWriteFlag write_flag;
  const int64_t timeout_us =
      ObTimeUtility::current_time() + config::internal_sql_execute_timeout();
  if (OB_ISNULL(oas)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ai column task: null access service", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ai column task: null session", K(ret));
  } else if (OB_FAIL(table_param.convert(&task_schema,
                                         task_schema.get_schema_version(),
                                         column_ids))) {
    LOG_WARN("ai column task: convert table param failed", K(ret));
  } else if (OB_FAIL(ObSqlTransControl::get_read_snapshot(
                 session, exec_ctx.get_physical_plan_ctx(), snapshot))) {
    LOG_WARN("ai column task: get read snapshot failed", K(ret));
  } else {
    write_flag.reset();
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.table_param_ = &table_param;
    dml_param.timeout_ = timeout_us;
    dml_param.snapshot_.assign(snapshot);
    dml_param.branch_id_ = 0;
    dml_param.store_ctx_guard_ = &store_ctx_guard;
    dml_param.schema_version_ = task_schema.get_schema_version();
    dml_param.runtime_schema_version_ = task_schema.get_schema_version();
    dml_param.dml_allocator_ = &allocator;
    if (OB_FAIL(oas->get_write_store_ctx_guard(timeout_us,
                                               tx_desc,
                                               dml_param.snapshot_,
                                               0,
                                               write_flag,
                                               store_ctx_guard))) {
      LOG_WARN("ai column task: get store ctx guard failed", K(ret));
    }
  }
  return ret;
}

// Fill one task row. Indices come from resolve_task_columns(), never assumed.
void fill_task_datum_row(blocksstable::ObDatumRow &row,
                         const TaskColumnIndex &idx,
                         uint64_t table_id,
                         uint64_t column_id,
                         uint64_t task_id,
                         int64_t request_id,
                         const ObString &rowkey_predicate,
                         int64_t eligible_ts)
{
  const int64_t now = ObTimeUtility::current_time();
  row.storage_datums_[idx.gmt_create_].set_timestamp(now);
  row.storage_datums_[idx.gmt_modified_].set_timestamp(now);
  row.storage_datums_[idx.table_id_].set_int(static_cast<int64_t>(table_id));
  row.storage_datums_[idx.column_id_].set_int(static_cast<int64_t>(column_id));
  row.storage_datums_[idx.task_id_].set_int(static_cast<int64_t>(task_id));
  row.storage_datums_[idx.request_id_].set_int(request_id);
  row.storage_datums_[idx.rowkey_predicate_].set_string(rowkey_predicate);
  row.storage_datums_[idx.eligible_ts_].set_timestamp(eligible_ts);
  row.storage_datums_[idx.state_].set_int(AI_TASK_PENDING_STATE);
  row.storage_datums_[idx.retry_count_].set_int(0);
}

// Fill only the primary key of a task row, for a delete by primary key.
void fill_task_rowkey(blocksstable::ObDatumRow &row,
                      const TaskColumnIndex &idx,
                      uint64_t table_id,
                      uint64_t column_id,
                      uint64_t task_id)
{
  row.storage_datums_[idx.table_id_].set_int(static_cast<int64_t>(table_id));
  row.storage_datums_[idx.column_id_].set_int(static_cast<int64_t>(column_id));
  row.storage_datums_[idx.task_id_].set_int(static_cast<int64_t>(task_id));
}

} // namespace

int64_t ObAiColumnTaskWriter::gen_request_id()
{
  return ObTimeUtility::current_time();
}

uint64_t ObAiColumnTaskWriter::gen_task_id(uint64_t table_id,
                                           uint64_t column_id,
                                           const ObString &rowkey_predicate)
{
  uint64_t hash_val = 0;
  hash_val = common::do_hash(rowkey_predicate, hash_val);
  hash_val = common::do_hash(column_id, hash_val);
  hash_val = common::do_hash(table_id, hash_val);
  // Keep it positive and non-zero so it stays a valid int64 row expression.
  hash_val &= 0x7FFFFFFFFFFFFFFFULL;
  return 0 == hash_val ? 1 : hash_val;
}

int ObAiColumnTaskWriter::build_rowkey_predicate(
    ObEvalCtx &eval_ctx,
    const ObIArray<ObExpr *> &row_exprs,
    const ObIArray<uint64_t> &column_ids,
    const ObIArray<uint64_t> &rowkey_ids,
    const ObIArray<ObString> &rowkey_names,
    const ObObjPrintParams &print_params,
    ObIAllocator &allocator,
    ObString &predicate)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  predicate.reset();
  if (rowkey_ids.count() != rowkey_names.count() || rowkey_ids.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ai column task: bad rowkey metadata", K(ret),
             K(rowkey_ids.count()), K(rowkey_names.count()));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, allocator, "%.*s",
                                     static_cast<int>(0), ""))) {
    LOG_WARN("ai column task: alloc predicate buffer failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_ids.count(); ++i) {
    int64_t row_idx = -1;
    for (int64_t j = 0; row_idx < 0 && j < column_ids.count(); ++j) {
      if (column_ids.at(j) == rowkey_ids.at(i)) {
        row_idx = j;
      }
    }
    if (row_idx < 0 || row_idx >= row_exprs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai column task: rowkey column missing from row image", K(ret),
               K(i), K(rowkey_ids), K(column_ids));
    } else {
      ObDatum *datum = nullptr;
      ObObj obj;
      ObExpr *expr = row_exprs.at(row_idx);
      if (OB_FAIL(expr->eval(eval_ctx, datum))) {
        LOG_WARN("ai column task: eval rowkey failed", K(ret));
      } else if (OB_ISNULL(datum)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(datum->to_obj(obj, expr->obj_meta_))) {
        LOG_WARN("ai column task: rowkey to obj failed", K(ret));
      } else if (i > 0 && OB_FAIL(databuff_printf(buf, buf_len, pos, allocator, " and "))) {
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, allocator, "`%.*s` = ",
                                         rowkey_names.at(i).length(),
                                         rowkey_names.at(i).ptr()))) {
      } else if (OB_FAIL(obj.print_sql_literal(buf, buf_len, pos, allocator, print_params))) {
        LOG_WARN("ai column task: print rowkey literal failed", K(ret), K(obj));
      }
    }
  }
  if (OB_SUCC(ret)) {
    predicate.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObAiColumnTaskWriter::enqueue_tasks(ObExecContext &exec_ctx,
                                        uint64_t table_id,
                                        const ObIArray<uint64_t> &ai_column_ids,
                                        const ObString &rowkey_predicate)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  transaction::ObTxDesc *tx_desc = nullptr;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *task_schema = nullptr;
  ObTabletID tablet_id;
  ObArenaAllocator allocator("AiColumnTask");
  ObTableDMLParam table_param(allocator);
  ObDMLBaseParam dml_param;
  ObStoreCtxGuard store_ctx_guard;
  ObSEArray<uint64_t, 16> column_ids;
  ObSEArray<uint64_t, 4> rowkey_ids;
  ObSEArray<uint64_t, 16> write_column_ids;
  TaskColumnIndex idx;
  storage::ObValueRowIterator row_iter;
  int64_t affected_rows = 0;
  if (ai_column_ids.empty()) {
    // nothing to do
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ai column task: null session", K(ret));
  } else if (OB_ISNULL(tx_desc = session->get_tx_desc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ai column task: no active transaction", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance()
                         .get_runtime_schema_guard(schema_guard))) {
    LOG_WARN("ai column task: get schema guard failed", K(ret));
  } else if (OB_FAIL(get_task_table_schema(schema_guard, task_schema, tablet_id))) {
  } else if (OB_FAIL(resolve_task_columns(*task_schema, allocator, column_ids,
                                          rowkey_ids, write_column_ids, idx))) {
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_iter.init())) {
    LOG_WARN("ai column task: init row iterator failed", K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    // One request id per row level change: all AI columns of this row publish
    // together, which the worker relies on to avoid partial visibility.
    const int64_t request_id = gen_request_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < ai_column_ids.count(); ++i) {
      // task_id is deterministic per (table, ai column, row), so re-enqueuing
      // the same row lands on the same primary key. The write goes through
      // put_rows (upsert), which replaces the pending task instead of failing,
      // so exactly one pending task per (row, column) survives and the newest
      // request wins.
      const uint64_t task_id =
          gen_task_id(table_id, ai_column_ids.at(i), rowkey_predicate);
      blocksstable::ObDatumRow datum_row;
      if (OB_FAIL(datum_row.init(allocator, write_column_ids.count()))) {
        LOG_WARN("ai column task: init datum row failed", K(ret));
      } else {
        datum_row.count_ = write_column_ids.count();
        fill_task_datum_row(datum_row, idx, table_id, ai_column_ids.at(i),
                            task_id, request_id, rowkey_predicate,
                            /*eligible_ts*/ now);
        if (OB_FAIL(row_iter.add_row(datum_row))) {
          LOG_WARN("ai column task: buffer task row failed", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_dml_param(exec_ctx, *task_schema, write_column_ids,
                                    allocator, table_param, dml_param,
                                    store_ctx_guard, *tx_desc))) {
  } else {
    ObAccessService *oas = share::server_service<ObAccessService>();
    if (OB_FAIL(oas->put_rows(tablet_id, *tx_desc, dml_param, write_column_ids,
                              &row_iter, affected_rows))) {
      LOG_WARN("ai column task: write task rows failed", K(ret),
               K(table_id), K(rowkey_predicate), K(ai_column_ids));
    }
  }
  store_ctx_guard.reset();
  return ret;
}

int ObAiColumnTaskWriter::purge_tasks(ObExecContext &exec_ctx,
                                      uint64_t table_id,
                                      const ObIArray<uint64_t> &ai_column_ids,
                                      const ObString &rowkey_predicate)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  transaction::ObTxDesc *tx_desc = nullptr;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *task_schema = nullptr;
  ObTabletID tablet_id;
  ObArenaAllocator allocator("AiColumnTask");
  ObTableDMLParam table_param(allocator);
  ObDMLBaseParam dml_param;
  ObStoreCtxGuard store_ctx_guard;
  ObSEArray<uint64_t, 16> column_ids;
  ObSEArray<uint64_t, 4> rowkey_ids;
  ObSEArray<uint64_t, 16> write_column_ids;
  TaskColumnIndex idx;
  storage::ObValueRowIterator delete_iter;
  int64_t affected_rows = 0;
  if (ai_column_ids.empty()) {
    // nothing pending for this row
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ai column task: null session", K(ret));
  } else if (OB_ISNULL(tx_desc = session->get_tx_desc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ai column task: no active transaction", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance()
                         .get_runtime_schema_guard(schema_guard))) {
    LOG_WARN("ai column task: get schema guard failed", K(ret));
  } else if (OB_FAIL(get_task_table_schema(schema_guard, task_schema, tablet_id))) {
  } else if (OB_FAIL(resolve_task_columns(*task_schema, allocator, column_ids,
                                          rowkey_ids, write_column_ids, idx))) {
  } else if (OB_FAIL(delete_iter.init())) {
    LOG_WARN("ai column task: init delete iterator failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ai_column_ids.count(); ++i) {
      const uint64_t task_id = gen_task_id(table_id, ai_column_ids.at(i), rowkey_predicate);
      blocksstable::ObDatumRow delete_row;
      if (OB_FAIL(delete_row.init(allocator, write_column_ids.count()))) {
        LOG_WARN("ai column task: init delete row failed", K(ret));
      } else {
        delete_row.count_ = write_column_ids.count();
        // The storage delete path compares the given row image with the stored
        // row. Columns marked NOP are skipped by that check, so only the
        // primary key is really verified.
        for (int64_t c = 0; c < write_column_ids.count(); ++c) {
          delete_row.storage_datums_[c].set_nop();
        }
        fill_task_rowkey(delete_row, idx, table_id, ai_column_ids.at(i), task_id);
        if (OB_FAIL(delete_iter.add_row(delete_row))) {
          LOG_WARN("ai column task: buffer delete row failed", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_dml_param(exec_ctx, *task_schema, write_column_ids,
                                    allocator, table_param, dml_param,
                                    store_ctx_guard, *tx_desc))) {
  } else {
    ObAccessService *oas = share::server_service<ObAccessService>();
    int tmp_ret = oas->delete_rows(tablet_id, *tx_desc, dml_param, write_column_ids,
                                   &delete_iter, affected_rows);
    if (OB_SUCCESS != tmp_ret && OB_ERR_DEFENSIVE_CHECK != tmp_ret) {
      // A defensive check here only means the pending task row did not exist,
      // which is the common case (a row is often deleted with no pending task).
      ret = tmp_ret;
      LOG_WARN("ai column task: purge task rows failed", K(ret),
               K(table_id), K(rowkey_predicate));
    }
  }
  store_ctx_guard.reset();
  return ret;
}

int ObAiColumnTaskWriter::collect_ai_columns(ObSchemaGetterGuard &schema_guard,
                                             uint64_t table_id,
                                             bool is_insert_or_new_row,
                                             const ObIArray<uint64_t> &ai_invalidate_ids,
                                             ObIArray<uint64_t> &out)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  out.reset();
  if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
    LOG_WARN("ai column task: get data table schema failed", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("ai column task: data table not found", K(ret), K(table_id));
  } else {
    for (ObTableSchema::const_column_iterator iter = table_schema->column_begin();
         OB_SUCC(ret) && iter != table_schema->column_end(); ++iter) {
      const ObColumnSchemaV2 *column = *iter;
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (!column->is_ai_column()) {
        // not an AI column
      } else if (is_insert_or_new_row
                 || has_exist_in_array(ai_invalidate_ids, column->get_column_id())) {
        if (OB_FAIL(out.push_back(column->get_column_id()))) {
          LOG_WARN("ai column task: collect ai column failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAiColumnTaskWriter::get_rowkey_columns(ObSchemaGetterGuard &schema_guard,
                                             uint64_t table_id,
                                             ObIArray<uint64_t> &rowkey_ids,
                                             ObIArray<ObString> &rowkey_names)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  rowkey_ids.reset();
  rowkey_names.reset();
  if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
    LOG_WARN("ai column task: get data table schema failed", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
  } else {
    const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      uint64_t column_id = OB_INVALID_ID;
      const ObColumnSchemaV2 *column = nullptr;
      if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
        LOG_WARN("ai column task: get rowkey column id failed", K(ret));
      } else if (OB_ISNULL(column = table_schema->get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ai column task: rowkey column not in schema", K(ret), K(column_id));
      } else if (OB_FAIL(rowkey_ids.push_back(column_id))) {
      } else if (OB_FAIL(rowkey_names.push_back(column->get_column_name_str()))) {
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
