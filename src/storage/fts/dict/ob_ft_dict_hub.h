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

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_HUB_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_HUB_H_

#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"
#include "lib/lock/ob_bucket_lock.h"
#include "lib/lock/ob_mutex.h"
#include "storage/fts/dict/ob_ft_dict_def.h"

namespace oceanbase
{
namespace storage
{
// typedef uint64_t ObFTTableID;
// typedef ObFTTableID ObFTDictTableID;

class ObFTDictInfo
{
public:
  ObFTDictInfo()
      : name_(""),
        type_(ObFTDictType::DICT_TYPE_INVALID),
        charset_(CHARSET_INVALID),
        version_(0),
        range_count_(0)
  {
  }

public:
  char name_[2048]; // for now
  ObFTDictType type_;
  ObCharsetType charset_;
  int64_t version_; // in memory
  int32_t range_count_;
};

struct ObFTDictInfoKey
{
public:
  ObFTDictInfoKey()
      : type_(static_cast<uint64_t>(ObFTDictType::DICT_TYPE_INVALID))
  {
  } // default constructor
  ObFTDictInfoKey(const uint64_t type)
      : type_(type)
  {
  }
  int hash(uint64_t &hash_value) const
  {
    int ret = OB_SUCCESS;
    hash_value = hash();
    return ret;
  }

  uint64_t hash() const
  {
    uint64_t hash = 0;
    hash = common::murmurhash(&type_, sizeof(int64_t), hash);
    return hash;
  }

  bool operator==(const ObFTDictInfoKey &other) const
  {
    return type_ == other.type_;
  }

  int compare(const ObFTDictInfoKey &other) const
  {
    int ret = 0;
    if (0 == ret) {
      ret = type_ - other.type_;
    }
    return ret;
  }

private:
  uint64_t type_;
  // name
};

class ObFTCacheRangeContainer;
class ObFTRangeDict;
// One built dictionary and the cache container it reads:
// a built dictionary is immutable, so one copy per dictionary type serves the
// whole process and every parser only reads it.
struct ObFTSharedDict;

class ObFTDictHub
{
public:
  ObFTDictHub()
      : is_inited_(false),
        dict_map_(),
        rw_dict_lock_(),
        build_lock_(),
        shared_alloc_(lib::ObMemAttr("FTSharedDict")),
        shared_dict_map_(),
        shared_dict_lock_()
  {}
  ~ObFTDictHub() { reset_shared_dicts(); }

  int init();

  int destroy();

  int build_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container);

  int load_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container);

  // Returns the read only dictionary of 'desc', building it on the first call.
  // The caller must not free the returned dictionary.
  int get_shared_dict(const ObFTDictDesc &desc, ObFTRangeDict *&dict);

private:
  int get_dict_info(const ObFTDictInfoKey &key, ObFTDictInfo &info);

  int put_dict_info(const ObFTDictInfoKey &key, const ObFTDictInfo &info);

  // Build one shared dictionary. Runs under build_lock_, so it may call
  // load_cache() / build_cache() without re-entering the shared map locks.
  int build_shared_dict(const ObFTDictDesc &desc, ObFTSharedDict *&shared);

  // Look the shared dictionary up under the bucket read lock.
  int find_shared_dict(const ObFTDictInfoKey &key, ObFTSharedDict *&shared);

  int put_shared_dict(const ObFTDictInfoKey &key, ObFTSharedDict *shared);

  void reset_shared_dicts();


private:
  bool is_inited_;
  // holds info of dict
  hash::ObHashMap<ObFTDictInfoKey, ObFTDictInfo> dict_map_;
  ObBucketLock rw_dict_lock_;

  // Serializes the one time build of a shared dictionary. It is deliberately
  // separate from shared_dict_lock_, which it is never held together with.
  lib::ObMutex build_lock_;

  // Everything reachable from shared_dict_map_ lives here, so the map can be
  // emptied and the arena released as one unit.
  ObArenaAllocator shared_alloc_;
  hash::ObHashMap<ObFTDictInfoKey, ObFTSharedDict *> shared_dict_map_;
  ObBucketLock shared_dict_lock_;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_HUB_H_
