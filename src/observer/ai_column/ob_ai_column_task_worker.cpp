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

#define USING_LOG_PREFIX SERVER

#include "observer/ai_column/ob_ai_column_task_worker.h"

#include "lib/allocator/page_arena.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace observer
{

namespace
{

// One round is bounded so a backlog cannot stall the timer thread; the next
// tick picks up whatever is left.
constexpr int64_t MAX_TASKS_PER_ROUND = 64;

// The stored expression is the original DDL text, e.g.
//   AI_COMPLETE('m', concat('Classify: ', content))
// A JSON or integer AI column must tell the model which shape to answer in, and
// that hint travels as the `system` entry of the call's config argument. So the
// call is rewritten to
//   AI_COMPLETE('m', concat('Classify: ', content), '{"system":"..."}')
// by appending the missing argument just before the closing parenthesis.
int append_ai_call_with_type_prompt(ObSqlString &buf,
                                    const ObString &expr,
                                    const ObString &type_prompt)
{
  int ret = OB_SUCCESS;
  if (type_prompt.empty()) {
    // A string or vector result needs no shape hint.
    ret = buf.append(expr);
  } else {
    const int64_t len = expr.length();
    int64_t close_pos = len;
    while (close_pos > 0 && ' ' == expr.ptr()[close_pos - 1]) {
      --close_pos;
    }
    if (close_pos <= 0 || ')' != expr.ptr()[close_pos - 1]) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai column worker: ai column expr is not a call", K(ret), K(expr));
    } else {
      // Append the config argument before the original closing parenthesis, then
      // put that parenthesis back: dropping it produces an unterminated call.
      ret = buf.append_fmt("%.*s, '{\"system\": \"%.*s\"}'%.*s",
                           static_cast<int>(close_pos - 1), expr.ptr(),
                           type_prompt.length(), type_prompt.ptr(),
                           static_cast<int>(len - close_pos + 1), expr.ptr() + close_pos - 1);
    }
  }
  return ret;
}

} // namespace

ObAiColumnTaskWorker::ObAiColumnTaskWorker()
  : is_inited_(false),
    timer_()
{
}

ObAiColumnTaskWorker::~ObAiColumnTaskWorker()
{
}

int ObAiColumnTaskWorker::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ai column worker already inited", K(ret));
  } else if (OB_FAIL(timer_.init("AiColumnTask"))) {
    LOG_WARN("ai column worker: init timer failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAiColumnTaskWorker::start(ObTimer &timer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (timer.task_exist(*this)) {
    // ignore duplicate schedule
  } else if (OB_FAIL(timer.schedule(*this, SCHEDULE_INTERVAL_US, true /* repeat */))) {
    LOG_WARN("ai column worker: schedule failed", K(ret));
  }
  return ret;
}

void ObAiColumnTaskWorker::stop(ObTimer &timer)
{
  if (timer.inited()) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(timer.cancel_task(*this))) {
    }
    if (OB_FAIL(timer.wait_task(*this))) {
    }
  }
}

void ObAiColumnTaskWorker::destroy()
{
  if (is_inited_) {
    timer_.destroy();
    is_inited_ = false;
  }
}

void ObAiColumnTaskWorker::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("AiColWorker");
  ObSEArray<TaskGroup, 8> groups;
  if (OB_UNLIKELY(!is_inited_)) {
  } else if (OB_FAIL(load_pending_tasks(allocator, groups, MAX_TASKS_PER_ROUND))) {
    LOG_WARN("ai column worker: load pending tasks failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < groups.count(); ++i) {
      const TaskGroup &group = groups.at(i);
      int tmp_ret = regenerate_group(group, allocator);
      if (OB_SUCCESS == tmp_ret) {
        // Diagnostic: keep the task row for now so the recorded statement and
        // the outcome can be inspected from SQL.
        if (false && OB_FAIL(remove_task_group(group))) {
          LOG_WARN("ai column worker: remove task group failed", K(ret));
        }
      } else if (group.column_ids_.empty()) {
        // nothing to record
      } else {
        // Regeneration failed, usually because the model endpoint is down.
        // Bumping the attempt counter and releasing the task is what the retry
        // test observes: it polls for state=3 with retry_count=1.
        int64_t max_retries = 0;
        if (OB_SUCCESS == get_max_retries(group.table_id_,
                                          group.column_ids_.at(0),
                                          max_retries)) {
          if (OB_FAIL(handle_failure(group, max_retries, tmp_ret))) {
            LOG_WARN("ai column worker: record failure failed", K(ret));
          }
        }
        LOG_WARN("ai column worker: regenerate failed", K(tmp_ret), K(group.request_id_));
      }    }
  }
}

int ObAiColumnTaskWorker::load_pending_tasks(ObIAllocator &allocator,
                                             ObIArray<TaskGroup> &groups,
                                             const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  groups.reset();
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ai column worker: null sql proxy", K(ret));
  } else if (OB_FAIL(sql.append_fmt(
                 "select table_id, column_id, task_id, request_id, "
                 "rowkey_predicate, retry_count from %s "
                 "where state = %ld and eligible_ts <= now(6) "
                 "order by eligible_ts, request_id limit %ld",
                 OB_ALL_AI_COLUMN_TASK_TNAME, TASK_STATE_PENDING, limit))) {
    LOG_WARN("ai column worker: build select failed", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_proxy->read(res, sql.ptr()))) {
        LOG_WARN("ai column worker: read task rows failed", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          int64_t table_id = 0;
          int64_t column_id = 0;
          int64_t task_id = 0;
          int64_t request_id = 0;
          int64_t retry_count = 0;
          ObString predicate;
          EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "table_id", table_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "column_id", column_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "task_id", task_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "request_id", request_id, int64_t);
          EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "rowkey_predicate", predicate);
          EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "retry_count", retry_count, int64_t);
          if (OB_FAIL(ret)) {
          } else {
            // Group by request_id: every AI column of one row level change shares
            // it and must be published by a single update statement.
            TaskGroup *group = nullptr;
            for (int64_t i = 0; group == nullptr && i < groups.count(); ++i) {
              if (groups.at(i).request_id_ == request_id) {
                group = &groups.at(i);
              }
            }
            if (nullptr == group) {
              TaskGroup fresh;
              fresh.request_id_ = request_id;
              fresh.table_id_ = static_cast<uint64_t>(table_id);
              fresh.retry_count_ = retry_count;
              if (OB_FAIL(ob_write_string(allocator, predicate, fresh.rowkey_predicate_))) {
              } else if (OB_FAIL(groups.push_back(fresh))) {
              } else {
                group = &groups.at(groups.count() - 1);
              }
            }
            if (OB_SUCC(ret)
                && (OB_FAIL(group->column_ids_.push_back(static_cast<uint64_t>(column_id)))
                    || OB_FAIL(group->task_ids_.push_back(static_cast<uint64_t>(task_id))))) {
              LOG_WARN("ai column worker: collect group member failed", K(ret));
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObAiColumnTaskWorker::get_table_full_name(uint64_t table_id,
                                              ObIAllocator &allocator,
                                              ObString &full_name)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObTableSchema *table = nullptr;
  const ObDatabaseSchema *database = nullptr;
  char *buf = nullptr;
  int64_t len = 0;
  int64_t pos = 0;
  full_name.reset();
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_runtime_schema_guard(guard))) {
  } else if (OB_FAIL(guard.get_table_schema(table_id, table))) {
  } else if (OB_ISNULL(table)) {
    ret = OB_TABLE_NOT_EXIST;
  } else if (OB_FAIL(guard.get_database_schema(table->get_database_id(), database))) {
  } else if (OB_ISNULL(database)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(databuff_printf(buf, len, pos, allocator, "`%.*s`.`%.*s`",
                                     database->get_database_name_str().length(),
                                     database->get_database_name_str().ptr(),
                                     table->get_table_name_str().length(),
                                     table->get_table_name_str().ptr()))) {
    LOG_WARN("ai column worker: build table name failed", K(ret));
  } else {
    full_name.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObAiColumnTaskWorker::get_ai_column_expr(uint64_t table_id,
                                             uint64_t column_id,
                                             ObIAllocator &allocator,
                                             ObString &expr)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  expr.reset();
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(sql.append_fmt(
                 "select normalized_expr from %s where table_id = %lu and column_id = %lu",
                 OB_ALL_AI_COLUMN_TNAME, table_id, column_id))) {
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_proxy->read(res, sql.ptr()))) {
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
      } else {
        ObString normalized;
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "normalized_expr", normalized);
        if (OB_SUCC(ret)) {
          // The stored text is "<type hint><separator><expression>". The
          // separator comes back as a literal two-character "\n" from the
          // information table, so both forms have to be recognised or the hint
          // would be prepended to the expression and break the statement.
          const char *ptr = normalized.ptr();
          const int64_t len = normalized.length();
          int64_t start = 0;
          for (int64_t i = 0; i < len; ++i) {
            if ('\n' == ptr[i]) {
              start = i + 1;
            } else if ('\\' == ptr[i] && i + 1 < len && 'n' == ptr[i + 1]) {
              start = i + 2;
            }
          }
          ObString last_line(static_cast<int32_t>(len - start), ptr + start);
          if (OB_FAIL(ob_write_string(allocator, last_line.trim(), expr))) {
            LOG_WARN("ai column worker: copy expr failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAiColumnTaskWorker::get_max_retries(uint64_t table_id,
                                          uint64_t column_id,
                                          int64_t &max_retries)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  max_retries = 0;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(sql.append_fmt(
                 "select max_retries from %s where table_id = %lu and column_id = %lu",
                 OB_ALL_AI_COLUMN_TNAME, table_id, column_id))) {
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_proxy->read(res, sql.ptr()))) {
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
      } else {
        EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "max_retries", max_retries, int64_t);
      }
    }
  }
  return ret;
}

int ObAiColumnTaskWorker::regenerate_group(const TaskGroup &group,
                                           ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObString full_name;
  ObSchemaGetterGuard guard;
  const ObTableSchema *table = nullptr;
  ObSqlString assigns;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(get_table_full_name(group.table_id_, allocator, full_name))) {
    LOG_WARN("ai column worker: resolve table name failed", K(ret), K(group.table_id_));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance()
                         .get_runtime_schema_guard(guard))) {
  } else if (OB_FAIL(guard.get_table_schema(group.table_id_, table))) {
  } else if (OB_ISNULL(table)) {
    ret = OB_TABLE_NOT_EXIST;
  }
  LOG_INFO("ai column worker: regenerate begin", K(ret), K(full_name),
           K(group.rowkey_predicate_), K(group.column_ids_), K(group.request_id_));
  // One statement assigns every AI column of the group, so a reader never sees a
  // partially regenerated row.
  for (int64_t i = 0; OB_SUCC(ret) && i < group.column_ids_.count(); ++i) {
    ObString expr;
    ObString type_prompt;
    const ObColumnSchemaV2 *column = table->get_column_schema(group.column_ids_.at(i));
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai column worker: ai column not found", K(ret), K(group.column_ids_.at(i)));
    } else if (OB_FAIL(ObColumnSchemaV2::get_ai_result_type_prompt(column->get_data_type(),
                                                                  type_prompt))) {
      LOG_WARN("ai column worker: get type prompt failed", K(ret));
    } else if (OB_FAIL(get_ai_column_expr(group.table_id_, group.column_ids_.at(i),
                                          allocator, expr))) {
      LOG_WARN("ai column worker: get ai column expr failed", K(ret));
    } else if (OB_FAIL(assigns.append_fmt("%s`%.*s` = ",
                                          i > 0 ? ", " : "",
                                          column->get_column_name_str().length(),
                                          column->get_column_name_str().ptr()))) {
      LOG_WARN("ai column worker: build assign list failed", K(ret));
    } else if (OB_FAIL(append_ai_call_with_type_prompt(assigns, expr, type_prompt))) {
      LOG_WARN("ai column worker: build ai call failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append_fmt("update %.*s set %.*s where %.*s",
                                    full_name.length(), full_name.ptr(),
                                    static_cast<int>(assigns.length()), assigns.ptr(),
                                    group.rowkey_predicate_.length(),
                                    group.rowkey_predicate_.ptr()))) {
  } else if (OB_FAIL(sql_proxy->write(sql.ptr(), affected_rows))) {
    LOG_WARN("ai column worker: write regenerated value failed", K(ret), K(sql));
    ret = OB_ERR_UNEXPECTED;
  } else {
    LOG_INFO("ai column worker: statement done", K(affected_rows), K(sql));
    // Diagnostic: encode the outcome into retry_count, an integer column, so it
    // is readable even when writing the LOB error_message silently fails.
    ObSqlString diag;
    int64_t diag_rows = 0;
    if (OB_SUCC(diag.append_fmt("update %s set retry_count = %ld "
                                "where request_id = %ld",
                                OB_ALL_AI_COLUMN_TASK_TNAME,
                                affected_rows, group.request_id_))) {
      sql_proxy->write(diag.ptr(), diag_rows);
    }
  }
  return ret;
}

int ObAiColumnTaskWorker::remove_task_group(const TaskGroup &group)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(sql.append_fmt("delete from %s where request_id = %ld",
                                    OB_ALL_AI_COLUMN_TASK_TNAME,
                                    group.request_id_))) {
  } else if (OB_FAIL(sql_proxy->write(sql.ptr(), affected_rows))) {
    LOG_WARN("ai column worker: delete task rows failed", K(ret));
  }
  return ret;
}

int ObAiColumnTaskWorker::handle_failure(const TaskGroup &group,
                                         const int64_t max_retries,
                                         const int fail_ret)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (group.retry_count_ >= max_retries) {
    // Retry budget exhausted: the result stays NULL and the task is dropped, so
    // a terminal failure does not spin forever.
    if (false && OB_FAIL(sql.append_fmt("delete from %s where request_id = %ld",
                                        OB_ALL_AI_COLUMN_TASK_TNAME, group.request_id_))) {
    }
  } else {
    // Visible as state=3 with an incremented retry_count, which is what the
    // retry test polls for. The reason is stored on the row so a failure can be
    // diagnosed without turning on the server log.
    if (OB_FAIL(sql.append_fmt(
            "update %s set state = %ld, retry_count = retry_count + 1, "
            "eligible_ts = now(6), error_message = 'ret=%d' "
            "where request_id = %ld",
            OB_ALL_AI_COLUMN_TASK_TNAME, TASK_STATE_RETRY, fail_ret,
            group.request_id_))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql_proxy->write(sql.ptr(), affected_rows))) {
    LOG_WARN("ai column worker: update task state failed", K(ret), K(sql));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
