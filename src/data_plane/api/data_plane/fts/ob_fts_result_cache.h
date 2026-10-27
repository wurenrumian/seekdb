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

#ifndef OCEANBASE_DATA_PLANE_FTS_OB_FTS_RESULT_CACHE_H_
#define OCEANBASE_DATA_PLANE_FTS_OB_FTS_RESULT_CACHE_H_

#include "lib/allocator/ob_malloc.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace storage
{

/**
 * Process wide cache of a full text analysis result, keyed by everything the analysis depends on.
 *
 * Both TOKENIZE() and the query side of MATCH ... AGAINST() redo the same analysis work over and
 * over: a literal repeated in a loop, a generated column recomputed for the same value, or the same
 * query text issued many times all produce the answer the previous call already computed. The
 * answers are stable because the dictionaries a parser borrows are loaded once per process
 * (ObDicLoader::is_load_) and never replaced in place, so a cached entry can only become stale when
 * the whole parser configuration is reloaded.
 *
 * The cache stores opaque bytes, so callers decide what a hit skips: the encoded json document for
 * TOKENIZE(), the token list for a query. It is direct mapped (one slot per hash bucket, replaced on
 * collision) so both the lookup and the footprint stay bounded: a hit allocates nothing besides the
 * copy into the caller's allocator, and an oversized entry is simply not cached.
 */
class ObFtsResultCache
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

  static ObFtsResultCache &instance()
  {
    static ObFtsResultCache cache;
    return cache;
  }

  // All failures (including a miss) only leave `hit` false; they never fail the statement.
  void get(const common::ObString &key,
           common::ObIAllocator &allocator,
           bool &hit,
           common::ObString &value)
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
          // The caller falls back to recomputing the result; a full arena must not fail it here.
        } else {
          MEMCPY(dst, entry.buf_ + entry.key_len_, entry.value_len_);
          value.assign_ptr(dst, static_cast<int32_t>(entry.value_len_));
          hit = true;
        }
      }
    }
  }

  void put(const common::ObString &key, const char *value, const int64_t value_len)
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
        char *buf = static_cast<char *>(ob_malloc(capacity, common::ObMemAttr("FTAnalysisCache")));
        if (OB_ISNULL(buf)) {
          // skip caching, the caller has already produced its result
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

private:
  ObFtsResultCache() = default;
  Entry &locate(const uint64_t hash, Shard *&shard)
  {
    shard = &shards_[hash % SHARD_CNT];
    return shard->slots_[(hash / SHARD_CNT) % SLOT_CNT_PER_SHARD];
  }
  DISALLOW_COPY_AND_ASSIGN(ObFtsResultCache);

private:
  Shard shards_[SHARD_CNT];
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_DATA_PLANE_FTS_OB_FTS_RESULT_CACHE_H_
