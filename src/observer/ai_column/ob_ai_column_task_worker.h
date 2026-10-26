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

#include "lib/task/ob_timer.h"
#include "lib/net/ob_addr.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace observer
{

// Consumes pending AI column regeneration tasks from
// `oceanbase.__all_ai_column_task` and writes the regenerated values back into
// the user table.
//
// The recomputation is delegated to the SQL layer: the worker issues an
// internal
//
//   update <db>.<table> set <ai_col> = <normalized expr> where <rowkey>
//
// which reuses the very same expression evaluation and AI invocation as a
// foreground statement. This keeps the generated value byte-identical to what a
// synchronous DML would produce, and it gives the "all AI columns of a row
// become visible together" guarantee for free: one statement assigns all
// columns of the request group, so no reader can observe a half-published row.
//
// Re-entrancy is not an issue. The resolver only invalidates an AI column when a
// column its prompt references is assigned, and the worker assigns the AI column
// itself, so the statement above enqueues no new task.
class ObAiColumnTaskWorker : public common::ObTimerTask
{
public:
  static const int64_t SCHEDULE_INTERVAL_US = 200 * 1000; // 200ms

  // Task row states, mirrored from the task table definition.
  static const int64_t TASK_STATE_PENDING = 0;
  static const int64_t TASK_STATE_DONE = 1;
  static const int64_t TASK_STATE_FAILED = 2;
  static const int64_t TASK_STATE_RETRY = 3;

  ObAiColumnTaskWorker();
  virtual ~ObAiColumnTaskWorker();

  int init();
  int start(common::ObTimer &timer);
  void stop(common::ObTimer &timer);
  void destroy();

  virtual void runTimerTask() override;

private:
  // One pending task row.
  struct TaskRow
  {
    TaskRow() : table_id_(0), column_id_(0), task_id_(0), request_id_(0),
                rowkey_predicate_(), retry_count_(0) {}
    uint64_t table_id_;
    uint64_t column_id_;
    uint64_t task_id_;
    int64_t request_id_;
    common::ObString rowkey_predicate_;
    int64_t retry_count_;
  };

  // All pending tasks of one row level change: they share request_id_ and are
  // published by a single update statement.
  struct TaskGroup
  {
    TaskGroup() : request_id_(0), table_id_(0), rowkey_predicate_(),
                  column_ids_(), task_ids_(), retry_count_(0) {}
    int64_t request_id_;
    uint64_t table_id_;
    common::ObString rowkey_predicate_;
    common::ObSEArray<uint64_t, 4> column_ids_;
    common::ObSEArray<uint64_t, 4> task_ids_;
    int64_t retry_count_;
  };

  // Fetch the currently eligible tasks and group them by request.
  int load_pending_tasks(common::ObIAllocator &allocator,
                         common::ObIArray<TaskGroup> &groups,
                         const int64_t limit);
  // Build and run the regeneration statement for one group.
  int regenerate_group(const TaskGroup &group, common::ObIAllocator &allocator);
  // Build "AI_COMPLETE(...)" from the AI column's stored normalized expression.
  int get_ai_column_expr(uint64_t table_id,
                         uint64_t column_id,
                         common::ObIAllocator &allocator,
                         common::ObString &expr);
  // Resolve "<db>.<table>" for a data table id.
  int get_table_full_name(uint64_t table_id,
                          common::ObIAllocator &allocator,
                          common::ObString &full_name);
  // Max retries configured for an AI column, -1 when unknown.
  int get_max_retries(uint64_t table_id, uint64_t column_id, int64_t &max_retries);
  // Remove the task rows of a group once it is resolved.
  int remove_task_group(const TaskGroup &group);
  // Record a failed attempt: bump retry_count and mark the task for retry, or
  // drop it when the retry budget is exhausted.
  int handle_failure(const TaskGroup &group, int64_t max_retries, int fail_ret);

private:
  bool is_inited_;
  common::ObTimer timer_;
  DISALLOW_COPY_AND_ASSIGN(ObAiColumnTaskWorker);
};

} // namespace observer
} // namespace oceanbase
