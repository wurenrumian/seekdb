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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_explain_resolver.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/parser/ob_parser.h"
#include "lib/string/ob_sql_string.h"

using namespace oceanbase;
using namespace sql;
using namespace common;
namespace oceanbase
{
namespace sql
{
namespace
{

// Escape a value for use inside a single quoted SQL literal.
int append_sql_literal(ObSqlString &sql, const ObString &value)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
    const char c = value.ptr()[i];
    if ('\'' == c || '\\' == c) {
      char buf[2] = {'\\', c};
      ret = sql.append(buf, 2);
    } else {
      char buf[1] = {c};
      ret = sql.append(buf, 1);
    }
  }
  return ret;
}

const char *file_column_type_name(int64_t column_type)
{
  const char *name = "VARCHAR";
  switch (column_type) {
    case 1: name = "BIGINT"; break;
    case 2: name = "DOUBLE"; break;
    case 3: name = "BOOLEAN"; break;
    case 4: name = "VARCHAR"; break;
    case 5: name = "DATE"; break;
    case 6: name = "DATETIME"; break;
    default: name = "VARCHAR"; break;
  }
  return name;
}

} // anonymous namespace
} // namespace sql
} // namespace oceanbase

const static int64_t EXPLAIN_FORMAT = 0;
const static int64_t EXPLAIN_DISPLAY_OPTION = 1;
const static int64_t EXPLAIN_INTO_TABLE = 2;
const static int64_t EXPLAIN_STMT_ID = 3;
const static int64_t EXPLAIN_CHILD_STMT = 4;
int ObExplainResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObExplainStmt *explain_stmt = NULL;
  if (!(T_EXPLAIN == parse_tree.type_  && 5 == parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid EXPLAIN syntax",
             "type", parse_tree.type_,
             "# of child", parse_tree.num_child_);
  } else if (NULL == (explain_stmt = create_stmt<ObExplainStmt>())) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create explain stmt");
  } else {
    ParseNode *child_node = parse_tree.children_[EXPLAIN_CHILD_STMT];
    ObStmt *child_stmt = NULL;
    bool rewritten = false;
    ObExplainDisplayOpt opt;
    opt.with_tree_line_ = true;
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty parse tree for stmt");
    } else if (!(T_SELECT == child_node->type_
                 || T_DELETE == child_node->type_
                 || T_INSERT == child_node->type_
                 || T_UPDATE == child_node->type_
                 || T_REPLACE == child_node->type_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid stmt type for EXPLAIN", "type", child_node->type_);
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "stmt type for EXPLAIN");
    } else {
      ObResolver resolver(params_);
      if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                   *child_node,
                                   child_stmt))) {
      } else if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to generate stmt for child stmt");
      } else if (parse_tree.param_num_ > 0 &&
                 OB_FAIL(rewrite_file_describe(child_stmt, rewritten))) {
        LOG_WARN("failed to rewrite describe for file table", K(ret));
      } else if (rewritten) {
        // stmt_ has been replaced by the generated metadata query
      } else {
        explain_stmt->set_explain_query_stmt(static_cast<ObDMLStmt *>(child_stmt));
        ParseNode *format = parse_tree.children_[EXPLAIN_FORMAT];
        if (NULL != format) {
          switch (format->type_) {
          case T_OUTLINE:
          {
            explain_stmt->set_explain_format(EXPLAIN_OUTLINE);
            opt.with_tree_line_ = true;
          } break;
          case T_EXTENDED:
          {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED);
            opt.with_tree_line_ = true;
          } break;
          case T_PARTITIONS:
          {
            explain_stmt->set_explain_format(EXPLAIN_PARTITIONS);
            opt.with_tree_line_ = true;
          } break;
          case T_TRADITIONAL:
          {
            explain_stmt->set_explain_format(EXPLAIN_TRADITIONAL);
            opt.with_tree_line_ = true;
          } break;
          case T_FORMAT_JSON:
          {
            explain_stmt->set_explain_format(EXPLAIN_FORMAT_JSON);
            opt.with_tree_line_ = false;
          } break;
          case T_BASIC: {
            explain_stmt->set_explain_format(EXPLAIN_BASIC);
            opt.with_tree_line_ = true;
            break;
          }
          case T_EXTENDED_NOADDR: {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED_NOADDR);
            opt.with_tree_line_ = true;
            break;
          }
          case T_PLANREGRESS: {
            explain_stmt->set_explain_format(EXPLAIN_PLANREGRESS);
            opt.with_tree_line_ = true;
            break;
          }
          default:
            ret = OB_ERROR;
            LOG_WARN("unknown explain format", "format", format->type_);
            break;
          }
        }
      }
    }
    ParseNode *opt_node = parse_tree.children_[EXPLAIN_DISPLAY_OPTION];
    if (OB_SUCC(ret) && NULL != opt_node) {
      switch (opt_node->type_) {
      case T_PRETTY_COLOR:
        opt.with_color_ = true;
        // pass
      case T_PRETTY:
        opt.with_tree_line_ = true;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      explain_stmt->set_display_opt(opt);
    }
    ParseNode *into_table_node = parse_tree.children_[EXPLAIN_INTO_TABLE];
    if (OB_SUCC(ret) && NULL != into_table_node) {
      ObString into_table(into_table_node->str_len_, into_table_node->str_value_);
      explain_stmt->set_into_table(into_table);
    }
    ParseNode *statement_id_node = parse_tree.children_[EXPLAIN_STMT_ID];
    if (OB_SUCC(ret) && NULL != statement_id_node) {
      ObString statement_id(statement_id_node->str_len_, statement_id_node->str_value_);
      explain_stmt->set_statement_id(statement_id);
    }
  }

  return ret;
}

int ObExplainResolver::rewrite_file_describe(ObStmt *child_stmt, bool &rewritten)
{
  int ret = OB_SUCCESS;
  rewritten = false;
  ObSelectStmt *select_stmt = NULL;
  const TableItem *file_item = NULL;
  if (OB_ISNULL(child_stmt) || !child_stmt->is_select_stmt()) {
    // Only DESCRIBE of a file table query is rewritten; other statements keep
    // the regular EXPLAIN behavior.
  } else if (OB_ISNULL(select_stmt = static_cast<ObSelectStmt *>(child_stmt))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (1 != select_stmt->get_table_size()) {
    // ignore queries with joins
  } else if (OB_ISNULL(file_item = select_stmt->get_table_item(0)) ||
             !file_item->is_file_table()) {
    // not a file table query
  } else if (OB_ISNULL(file_item->file_table_def_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const ObFileTableDef *table_def = file_item->file_table_def_;
    ObSqlString sql;
    if (OB_FAIL(sql.append("select `column_name`, `column_type`, `null`, `key`, "
                           "`default`, `extra` from ("))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_def->columns_.count(); ++i) {
      const ObFileColumnDef &col = table_def->columns_.at(i);
      if (i > 0 && OB_FAIL(sql.append(" union all "))) {
      } else if (OB_FAIL(sql.append_fmt("select %ld as `ord`, '", static_cast<long>(i)))) {
      } else if (OB_FAIL(append_sql_literal(sql, col.column_name_))) {
      } else if (OB_FAIL(sql.append_fmt("' as `column_name`, '%s' as `column_type`, "
                                        "'%s' as `null`, NULL as `key`, NULL as `default`, "
                                        "NULL as `extra`",
                                        file_column_type_name(col.column_type_),
                                        col.nullable_ ? "YES" : "NO"))) {
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append(") `file_desc` order by `ord`"))) {
    } else if (OB_ISNULL(session_info_) || OB_ISNULL(params_.allocator_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ParseResult parse_result;
      ObParser parser(*params_.allocator_, session_info_->get_sql_mode());
      if (OB_FAIL(parser.parse(sql.string(), parse_result))) {
        LOG_WARN("failed to parse generated describe sql", K(sql), K(ret));
      } else if (OB_ISNULL(parse_result.result_tree_) ||
                 parse_result.result_tree_->num_child_ < 1 ||
                 OB_ISNULL(parse_result.result_tree_->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObStmt *new_stmt = NULL;
        ObResolver resolver(params_);
        if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                     *parse_result.result_tree_->children_[0],
                                     new_stmt))) {
          LOG_WARN("failed to resolve generated describe sql", K(ret));
        } else if (OB_ISNULL(new_stmt)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          stmt_ = new_stmt;
          rewritten = true;
        }
      }
    }
  }
  return ret;
}
