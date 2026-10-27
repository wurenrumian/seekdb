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

#include "sql/engine/expr/ob_expr_tokenize.h"

#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"
#include "common/json_type/ob_json_base.h"
#include "common/json_type/ob_json_bin.h"
#include "common/json_type/ob_json_tree.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/lock/ob_spin_lock.h"
#include "object/ob_object.h"
#include "sql/resolver/ddl/ob_fts_index_builder_util.h"
#include "share/ob_json_access_utils.h"
#include "data_plane/fts/ob_fts_parser_helper.h"
#include "data_plane/fts/dict/ob_gen_dic_loader.h"
#include "data_plane/fts/ob_fts_parser_property.h"

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_json_func_helper.h" // file not self-contained, there're logs inside.

namespace oceanbase
{
namespace sql
{

/**
 * Result cache for TOKENIZE().
 *
 * TOKENIZE() is a pure function of its arguments and the dictionaries the parser borrows: an ik
 * dictionary is loaded once per process (ObDicLoader::is_load_) and never replaced in place, so the
 * same text always segments into the same tokens. Tokenizing the same text again therefore repeats
 * work whose answer is already known - a literal repeated in a loop, a generated column recomputed
 * for the same value, or simply the same statement issued many times.
 *
 * The cache keeps the encoded JSON document, not the token list, so a hit skips the segmentation,
 * the word map, the JSON document build and its serialization. It is direct mapped (one slot per
 * hash bucket, replaced on collision) so both the lookup and the memory footprint stay bounded: no
 * allocation happens on a hit beyond the copy into the caller's allocator, and an oversized request
 * is simply not cached.
 */
class FtsTokenizeResultCache
{
public:
  static const int64_t SHARD_CNT = 16;
  static const int64_t SLOT_CNT_PER_SHARD = 32;
  static const int64_t MAX_KEY_LEN = 1024;
  static const int64_t MAX_VALUE_LEN = 8 * 1024;
  static const int64_t MIN_SLOT_CAPACITY = 512;

  struct Entry
  {
    Entry() : hash_(0), key_len_(0), value_len_(0), capacity_(0), buf_(nullptr) {}
    uint64_t hash_;
    int64_t key_len_;
    int64_t value_len_;
    int64_t capacity_;
    char *buf_;  // key bytes followed by the value bytes
  };

  struct Shard
  {
    Shard() = default;
    common::ObSpinLock lock_;
    Entry slots_[SLOT_CNT_PER_SHARD];
  };

  static FtsTokenizeResultCache &instance();

  // All failures (including a miss) only leave `hit` false; they never fail the statement.
  void get(const ObString &key, ObIAllocator &allocator, bool &hit, ObString &value);
  void put(const ObString &key, const char *value, const int64_t value_len);

private:
  FtsTokenizeResultCache() = default;
  Entry &locate(const uint64_t hash, Shard *&shard);
  DISALLOW_COPY_AND_ASSIGN(FtsTokenizeResultCache);

private:
  Shard shards_[SHARD_CNT];
};

FtsTokenizeResultCache &FtsTokenizeResultCache::instance()
{
  static FtsTokenizeResultCache cache;
  return cache;
}

FtsTokenizeResultCache::Entry &FtsTokenizeResultCache::locate(const uint64_t hash,
                                                             Shard *&shard)
{
  shard = &shards_[hash % SHARD_CNT];
  return shard->slots_[(hash / SHARD_CNT) % SLOT_CNT_PER_SHARD];
}

void FtsTokenizeResultCache::get(const ObString &key,
                                 ObIAllocator &allocator,
                                 bool &hit,
                                 ObString &value)
{
  hit = false;
  if (OB_UNLIKELY(key.empty() || key.length() > MAX_KEY_LEN)) {
    // nothing to do
  } else {
    const uint64_t hash = common::murmurhash(key.ptr(), key.length(), 0);
    Shard *shard = nullptr;
    Entry &entry = locate(hash, shard);
    common::ObSpinLockGuard guard(shard->lock_);
    if (entry.hash_ == hash
        && entry.key_len_ == key.length()
        && entry.value_len_ > 0
        && nullptr != entry.buf_
        && 0 == MEMCMP(entry.buf_, key.ptr(), key.length())) {
      char *dst = static_cast<char *>(allocator.alloc(entry.value_len_));
      if (OB_ISNULL(dst)) {
        // The caller falls back to tokenizing; a full arena must not fail the statement here.
      } else {
        MEMCPY(dst, entry.buf_ + entry.key_len_, entry.value_len_);
        value.assign_ptr(dst, static_cast<int32_t>(entry.value_len_));
        hit = true;
      }
    }
  }
}

void FtsTokenizeResultCache::put(const ObString &key, const char *value, const int64_t value_len)
{
  if (OB_UNLIKELY(key.empty() || key.length() > MAX_KEY_LEN || value_len <= 0
                  || value_len > MAX_VALUE_LEN || OB_ISNULL(value))) {
    // not worth caching
  } else {
    const uint64_t hash = common::murmurhash(key.ptr(), key.length(), 0);
    Shard *shard = nullptr;
    Entry &entry = locate(hash, shard);
    common::ObSpinLockGuard guard(shard->lock_);
    const int64_t need = key.length() + value_len;
    if (entry.capacity_ < need) {
      const int64_t capacity = MAX(need, MIN_SLOT_CAPACITY);
      char *buf = static_cast<char *>(ob_malloc(capacity, ObMemAttr("FTTokenizeCache")));
      if (OB_ISNULL(buf)) {
        // skip caching, the statement has already produced its result
      } else {
        if (nullptr != entry.buf_) {
          ob_free(entry.buf_);
        }
        entry.buf_ = buf;
        entry.capacity_ = capacity;
      }
    }
    if (nullptr != entry.buf_ && entry.capacity_ >= need) {
      MEMCPY(entry.buf_, key.ptr(), key.length());
      MEMCPY(entry.buf_ + key.length(), value, value_len);
      entry.hash_ = hash;
      entry.key_len_ = key.length();
      entry.value_len_ = value_len;
    }
  }
}

ObExprTokenize::ObExprTokenize(common::ObIAllocator &alloc)
    : ObStringExprOperator(alloc,
                           T_FUN_TOKENIZE,
                           N_TOKENIZE,
                           MORE_THAN_ZERO,
                           VALID_FOR_GENERATED_COL)
{
}

ObExprTokenize::~ObExprTokenize() {}

int ObExprTokenize::eval_tokenize(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();

  ObIJsonBase *json_result = nullptr;
  TokenizeParam param;

  // check param num, which is checked in ObExprOperator::calc_result_typeN.
  if (OB_UNLIKELY(expr.arg_cnt_ < 1 || expr.arg_cnt_ > 3)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parse_param(expr, ctx, temp_allocator, param))) {
  } else {
    ObString cache_key;
    bool cacheable = false;
    bool cache_hit = false;
    ObString cache_value;
    if (OB_FAIL(build_tokenize_cache_key(param, temp_allocator, cache_key, cacheable))) {
      LOG_WARN("failed to build tokenize cache key", K(ret));
    } else if (cacheable) {
      FtsTokenizeResultCache::instance().get(cache_key, temp_allocator, cache_hit, cache_value);
    }
    if (OB_SUCC(ret) && cache_hit) {
      ret = ObJsonExprHelper::pack_json_str_res(expr, ctx, expr_datum, cache_value);
    } else if (OB_SUCC(ret)) {
      if (OB_FAIL(tokenize_fulltext(param, param.output_mode_, temp_allocator, json_result))) {
      } else if (OB_FAIL(ObJsonExprHelper::pack_json_res(expr,
                                                         ctx,
                                                         temp_allocator,
                                                         json_result,
                                                         expr_datum))) {
      } else if (cacheable) {
        cache_tokenize_result(cache_key, json_result, temp_allocator);
      }
    }
  }

  return ret;
}

int ObExprTokenize::build_tokenize_cache_key(const TokenizeParam &param,
                                             ObIAllocator &allocator,
                                             ObString &key,
                                             bool &cacheable)
{
  int ret = OB_SUCCESS;
  cacheable = false;

  const int64_t header_len = static_cast<int64_t>(sizeof(int64_t)) * 5;
  const int64_t parser_len = param.parser_name_.length();
  const int64_t properties_len = param.properties_.length();
  const int64_t fulltext_len = param.fulltext_.length();
  const int64_t key_len = header_len + parser_len + properties_len + fulltext_len;

  if (parser_len < 0 || properties_len < 0 || fulltext_len < 0) {
    ret = OB_ERR_UNEXPECTED;
  } else if (key_len > FtsTokenizeResultCache::MAX_KEY_LEN) {
    // A long document is unlikely to repeat and would push the cache footprint up; tokenize it.
  } else {
    char *buf = static_cast<char *>(allocator.alloc(key_len));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      int64_t pos = 0;
      const int64_t mode = static_cast<int64_t>(param.output_mode_);
      const int64_t collation = static_cast<int64_t>(param.meta_.get_collation_type());
      MEMCPY(buf + pos, &parser_len, sizeof(int64_t));
      pos += sizeof(int64_t);
      MEMCPY(buf + pos, &properties_len, sizeof(int64_t));
      pos += sizeof(int64_t);
      MEMCPY(buf + pos, &fulltext_len, sizeof(int64_t));
      pos += sizeof(int64_t);
      MEMCPY(buf + pos, &mode, sizeof(int64_t));
      pos += sizeof(int64_t);
      MEMCPY(buf + pos, &collation, sizeof(int64_t));
      pos += sizeof(int64_t);
      if (parser_len > 0) {
        MEMCPY(buf + pos, param.parser_name_.ptr(), parser_len);
        pos += parser_len;
      }
      if (properties_len > 0) {
        MEMCPY(buf + pos, param.properties_.ptr(), properties_len);
        pos += properties_len;
      }
      if (fulltext_len > 0) {
        MEMCPY(buf + pos, param.fulltext_.ptr(), fulltext_len);
        pos += fulltext_len;
      }
      key.assign_ptr(buf, static_cast<int32_t>(pos));
      cacheable = true;
    }
  }
  return ret;
}

void ObExprTokenize::cache_tokenize_result(const ObString &key,
                                           ObIJsonBase *result,
                                           ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  bool should_pack_diff = false;
  ObString raw_binary;

  if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (result->is_bin()
             && OB_FAIL(static_cast<ObJsonBin *>(result)->should_pack_diff(should_pack_diff))) {
  } else if (should_pack_diff) {
    // The packed form differs from the plain binary form, so the encoded bytes are not reusable.
  } else if (OB_FAIL(share::ObJsonWrapper::get_raw_binary(result, raw_binary, &allocator))) {
  } else {
    // Caching is best effort: a failure here must never fail the statement.
    FtsTokenizeResultCache::instance().put(key, raw_binary.ptr(), raw_binary.length());
  }
  if (OB_FAIL(ret)) {
    LOG_TRACE("skip caching tokenize result", K(ret));
  }
}

int ObExprTokenize::tokenize_fulltext(const TokenizeParam &param,
                                      TokenizeParam::OUTPUT_MODE mode,
                                      ObIAllocator &allocator,
                                      ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  storage::ObFTParseHelper tokenize_helper;
  const int64_t ft_word_bkt_cnt = MIN(MAX(param.fulltext_.length() / 2, 2), 997);
  int64_t doc_len = 0;
  storage::ObFTWordMap token_map;

  if (TokenizeParam::OUTPUT_MODE::DEFAULT != mode && TokenizeParam::OUTPUT_MODE::ALL != mode) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(tokenize_helper.init(&allocator, param.parser_name_, param.properties_))) {
  } else if (OB_FAIL(token_map.create(ft_word_bkt_cnt, common::ObMemAttr("FTWordMap")))) {
  } else if (
      (0 != param.fulltext_.length())
      && OB_FAIL(tokenize_helper.segment(
                     param.meta_,
                     param.fulltext_.ptr(),
                     param.fulltext_.length(),
                     doc_len,
                     token_map))) {
  } else {
    switch (param.output_mode_) {
    case TokenizeParam::OUTPUT_MODE::DEFAULT: {
      if (OB_FAIL(tokenize_helper.make_token_array_json(token_map, result))) {
      } else {
        // pass
      }
      break;
    }
    case TokenizeParam::OUTPUT_MODE::ALL: {
      if (OB_FAIL(tokenize_helper.make_detail_json(token_map, doc_len, result))) {
      } else {
        // pass
      }
      break;
    }
    default:
      ret = OB_INVALID_ARGUMENT;
    }
  }
  return ret;
}

ObExprTokenize::TokenizeParam ::TokenizeParam()
  : allocator_(ObMemAttr("TokenizeParam")),
    parser_name_(ObString(OB_DEFAULT_FULLTEXT_PARSER_NAME)),
    meta_(),
    fulltext_(),
    output_mode_(OUTPUT_MODE::DEFAULT)
{
}

int ObExprTokenize::TokenizeParam::parse_json_param(const ObIJsonBase *obj)
{
  int ret = OB_SUCCESS;
  ObString str;
  ObIJsonBase *val;

  if (OB_UNLIKELY(nullptr == obj)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (ObJsonNodeType::J_OBJECT != obj->json_type()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (obj->element_count() == 0) {
    // no data
  } else if (OB_FAIL(obj->get_object_value(0, str, val))) {
  } else if (0 == str.case_compare(CASE_INDICATOR_STR)) {
    if (ObJsonNodeType::J_STRING != val->json_type()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (0 == ObString(val->get_data_length(), val->get_data()).case_compare("UPPER")) {
    } else if (0 == ObString(val->get_data_length(), val->get_data()).case_compare("LOWER")) {
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "case indentifier");
  } else if (0 == str.case_compare(OUTPUT_MODE_STR)) {
    if (ObJsonNodeType::J_STRING != val->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "output mode should be string default or all");
    } else if (0 == ObString(val->get_data_length(), val->get_data()).case_compare("DEFAULT")) {
      output_mode_ = DEFAULT;
    } else if (0 == ObString(val->get_data_length(), val->get_data()).case_compare("ALL")) {
      output_mode_ = ALL;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "output mode should be string default or all");
    }
  } else if (0 == str.case_compare(STOPWORDS_LIST_STR)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "stopwords");
  } else if (0 == str.case_compare(ADDITIONAL_ARGS_STR)) {
    if (ObJsonNodeType::J_ARRAY != val->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "parser arguments");
    } else {
      ObString json_str;
      if (OB_FAIL(storage::ObFTParserJsonProps::tokenize_array_to_props_json(allocator_, val, json_str))) {
        ObSqlString message;
        message.append_fmt("format in %s form", ADDITIONAL_ARGS_STR);
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, message.ptr());
      } else {
        properties_ = json_str;
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    ObSqlString message;
    message.append_fmt("config: %s", str.ptr());
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, message.ptr());
  }
  return ret;
}

int ObExprTokenize::parse_param(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                common::ObArenaAllocator &allocator,
                                TokenizeParam &param)
{
  int ret = OB_SUCCESS;

  ObDatum *parser_params_datum;
  ObString raw_parser_name = ObString::make_string(OB_DEFAULT_FULLTEXT_PARSER_NAME);

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());

  if (OB_UNLIKELY(expr.arg_cnt_ < 1 || expr.arg_cnt_ > 3)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parse_fulltext(expr, ctx, param))) {
  } else if (OB_FAIL(parse_parser_name(expr, ctx, param))) {
  } else if (OB_FAIL(parse_parser_properties(expr, ctx, temp_allocator, param))) {
  } else if (OB_FAIL(param.reform_parser_properties(param.properties_))) {
  } else if (OB_FAIL(param.try_load_dictionary_for_ik())) {
  }
  return ret;
}

int ObExprTokenize::construct_ft_parser_inner_name(const ObString &input_str, TokenizeParam &param)
{
  int ret = OB_SUCCESS;
  storage::ObFTParser parser;

  char *parser_name_buf = nullptr;
  if (OB_ISNULL(parser_name_buf
                = static_cast<char *>(param.allocator_.alloc(storage::OB_FT_PARSER_NAME_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(parser.init(input_str))) {
  } else if (OB_FAIL(parser.serialize_to_str(parser_name_buf, storage::OB_FT_PARSER_NAME_LENGTH))) {
  } else {
    param.parser_name_ = ObString::make_string(parser_name_buf);
  }
  return ret;
}

int ObExprTokenize::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types,
                                      int64_t param_num,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(param_num < 1 || param_num > 3)) {
    ret = OB_ERR_PARAM_SIZE;
    ObString expr_name(N_TOKENIZE);
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, expr_name.length(), expr_name.ptr());
  } else {
    // just okay
  }

  ObLength length = ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType].get_length();

  if (OB_SUCC(ret)) {
    // set res type
    type.set_json();
    type.set_length(length); // keep consistent with other json expr, maybe calc it later.

    // param type set, skip charset after first param
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; ++i) {
      if (ob_is_string_type(types[i].get_type())) {
        if (types[i].get_charset_type() != CHARSET_UTF8MB4) {
          types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }

    // handle param
    if (param_num >= 2) {
      types[1].set_varchar();
    }

    if (param_num >= 3) {
      if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types, 2, N_TOKENIZE))) {
      }
    }
  }

  return ret;
}

int ObExprTokenize::cg_expr(ObExprCGCtx &op_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK((rt_expr.arg_cnt_ >= 1 && rt_expr.arg_cnt_ <= 3));
  if (OB_SUCC(ret)) {
    // do register
    rt_expr.eval_func_ = eval_tokenize;
  }
  return ret;
}

int ObExprTokenize::parse_fulltext(const ObExpr &expr, ObEvalCtx &ctx, TokenizeParam &param)
{
  int ret = OB_SUCCESS;

  ObDatum *fulltext_datum;

  if (OB_FAIL(expr.args_[0]->eval(ctx, fulltext_datum))) {
  } else {
    if (fulltext_datum->is_null()) {
      // do nothing, return empty result
      param.fulltext_ = ObString::make_empty_string();
    } else {
      param.fulltext_ = fulltext_datum->get_string();
    }
    param.meta_.set_varchar(); // as we hardcoded in fts_index
    param.meta_.set_collation_type(expr.args_[0]->obj_meta_.get_collation_type());
  }
  return ret;
}

int ObExprTokenize::parse_parser_name(const ObExpr &expr, ObEvalCtx &ctx, TokenizeParam &param)
{
  int ret = OB_SUCCESS;
  ObDatum *parser_datum = nullptr;
  ObString raw_parser_name;

  if (expr.arg_cnt_ < 2) {
    raw_parser_name = ObString::make_string(OB_DEFAULT_FULLTEXT_PARSER_NAME);
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, parser_datum))) {
  } else {
    if (parser_datum->is_null()) {
      raw_parser_name = ObString::make_string(OB_DEFAULT_FULLTEXT_PARSER_NAME);
    } else {
      ObString name = parser_datum->get_string();
      raw_parser_name = name.trim();
    }
  }

  if (OB_FAIL(ret)) {
    // already logged
  } else if (OB_FAIL(construct_ft_parser_inner_name(raw_parser_name, param))) {
  }

  return ret;
}

int ObExprTokenize::parse_parser_properties(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            MultimodeAlloctor &mm_alloc,
                                            TokenizeParam &param)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *base = nullptr;

  if (expr.arg_cnt_ < 3) {
    // do nothing
  } else {
    bool is_null = false;
    if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, mm_alloc, 2, base, is_null))) {
    } else {
      if (ObJsonNodeType::J_ARRAY != base->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "parser args should be in an array.");
      } else {
        for (uint64_t i = 0; OB_SUCC(ret) && i < base->element_count(); ++i) {
          ObIJsonBase *node = nullptr;
          if (OB_FAIL(base->get_array_element(i, node))) {
          } else if (ObJsonNodeType::J_OBJECT != (node->json_type())) {
            ret = OB_INVALID_ARGUMENT;
          } else if (OB_FAIL(param.parse_json_param(node))) {
          }
        } // for
      }
    }
  }

  return ret;
}

int ObExprTokenize::TokenizeParam::reform_parser_properties(const ObString &properties)
{
  int ret = OB_SUCCESS;
  storage::ObFTParserJsonProps parser_properties;

  if (properties.empty()) {
    // No properties were given. ObFTParserProperty is default-constructed with
    // exactly the values the rebuild below would fill in (min/max token size,
    // ngram sizes, ik_mode=smart), and parse_for_parser_helper already treats a
    // missing key as "use the default". Round-tripping the empty document
    // through parse -> rebuild -> print only burns JSON parsing and printing on
    // every TOKENIZE evaluation, so skip it and leave properties_ empty.
    properties_ = ObString();
  } else if (OB_FAIL(parser_properties.init())) {
  } else if (OB_FAIL(parser_properties.parse_from_valid_str(properties))) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "parser properties invalid.");
  } else if (OB_FAIL(parser_properties.rebuild_props_for_ddl(parser_name_,
                                                             ObCollationType::CS_TYPE_UTF8MB4_BIN,
                                                             true))) {
  } else if (OB_FAIL(parser_properties.to_format_json(allocator_, properties_))) {
  }

  return ret;
}

int ObExprTokenize::TokenizeParam::try_load_dictionary_for_ik()
{
  int ret = OB_SUCCESS;
  bool need_to_load_dic = false;
  storage::ObDicLoaderHandle dic_loader_handle;
  if (OB_FAIL(share::ObFtsIndexBuilderUtil::check_need_to_load_dic(
          parser_name_, need_to_load_dic))) {
  } else if (need_to_load_dic) {
    if (OB_FAIL(storage::ObGenDicLoader::get_instance().get_dic_loader(
                    ObString::make_string(storage::ObFTSLiteral::PARSER_NAME_IK), // currently only ik, use parser_name_ without version suffix
                    ObCharset::charset_type_by_coll(meta_.get_collation_type()),
                    dic_loader_handle))) {
    } else if (OB_UNLIKELY(!dic_loader_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(dic_loader_handle.get_loader()->try_load_dictionary_in_trans())) {
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
