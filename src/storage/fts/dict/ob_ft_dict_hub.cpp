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

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ft_dict_hub.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/dict/ob_ft_cache_container.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_range_dict.h"
namespace oceanbase
{
namespace storage
{
// A built dictionary and the cache container it reads. The two have to be
// released together, the dictionary first, because it pins handles of the
// container; resetting the arena alone would skip both destructors.
struct ObFTSharedDict
{
  explicit ObFTSharedDict(ObIAllocator &alloc) : container_(alloc), dict_(nullptr) {}

  ObFTCacheRangeContainer container_;
  ObFTRangeDict *dict_;
};

int ObFTDictHub::init()
{
  static constexpr int K_MAX_DICT_BUCKET = 128; // for now, only built-in dicts.
  int ret = OB_SUCCESS;
  if (OB_FAIL(dict_map_.create(K_MAX_DICT_BUCKET, "dict_map"))) {
  } else if (OB_FAIL(rw_dict_lock_.init(K_MAX_DICT_BUCKET))) {
  } else if (OB_FAIL(shared_dict_map_.create(K_MAX_DICT_BUCKET, "shared_dict_map"))) {
  } else if (OB_FAIL(shared_dict_lock_.init(K_MAX_DICT_BUCKET))) {
  } else {
    is_inited_ = true;
  }
  return ret;
};

int ObFTDictHub::destroy()
{
  int ret = OB_SUCCESS;
  reset_shared_dicts();
  shared_dict_lock_.destroy();
  is_inited_ = false;
  return ret;
}

void ObFTDictHub::reset_shared_dicts()
{
  for (hash::ObHashMap<ObFTDictInfoKey, ObFTSharedDict *>::iterator iter = shared_dict_map_.begin();
       iter != shared_dict_map_.end();
       ++iter) {
    ObFTSharedDict *shared = iter->second;
    if (OB_NOT_NULL(shared)) {
      if (OB_NOT_NULL(shared->dict_)) {
        shared->dict_->~ObFTRangeDict();
      }
      shared->~ObFTSharedDict();
    }
  }
  shared_dict_map_.reuse();
  shared_alloc_.reset();
}

int ObFTDictHub::build_shared_dict(const ObFTDictDesc &desc, ObFTSharedDict *&shared)
{
  int ret = OB_SUCCESS;
  ObFTSharedDict *new_shared = nullptr;
  shared = nullptr;
  if (OB_ISNULL(new_shared = OB_NEWx(ObFTSharedDict, &shared_alloc_, shared_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate shared dict", K(ret));
  } else if (OB_FAIL(load_cache(desc, new_shared->container_))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(build_cache(desc, new_shared->container_))) {
        LOG_WARN("failed to build dict cache", K(ret), K(desc.type_));
      }
    } else {
      LOG_WARN("failed to load dict cache", K(ret), K(desc.type_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(new_shared->dict_ = OB_NEWx(ObFTRangeDict, &shared_alloc_,
                                                   shared_alloc_,
                                                   &new_shared->container_,
                                                   desc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate range dict", K(ret));
  } else if (OB_FAIL(new_shared->dict_->init())) {
    LOG_WARN("failed to init range dict", K(ret), K(desc.type_));
  } else {
    shared = new_shared;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(new_shared)) {
    // Nothing can reach this item, so nothing else would ever release it: the
    // arena reset does not run destructors, which would keep the pinned cache
    // handles alive for the whole process.
    if (OB_NOT_NULL(new_shared->dict_)) {
      new_shared->dict_->~ObFTRangeDict();
    }
    new_shared->~ObFTSharedDict();
    shared_alloc_.free(new_shared);
  }
  return ret;
}

int ObFTDictHub::find_shared_dict(const ObFTDictInfoKey &key, ObFTSharedDict *&shared)
{
  int ret = OB_SUCCESS;
  ObBucketHashRLockGuard guard(shared_dict_lock_, key.hash());
  if (OB_FAIL(shared_dict_map_.get_refactored(key, shared))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get shared dict", K(ret));
    }
  }
  return ret;
}

int ObFTDictHub::put_shared_dict(const ObFTDictInfoKey &key, ObFTSharedDict *shared)
{
  int ret = OB_SUCCESS;
  ObBucketHashWLockGuard guard(shared_dict_lock_, key.hash());
  if (OB_FAIL(shared_dict_map_.set_refactored(key, shared))) {
    LOG_WARN("failed to set shared dict", K(ret));
  }
  return ret;
}

int ObFTDictHub::get_shared_dict(const ObFTDictDesc &desc, ObFTRangeDict *&dict)
{
  int ret = OB_SUCCESS;
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  ObFTSharedDict *shared = nullptr;
  dict = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(find_shared_dict(key, shared))) {
    if (OB_HASH_NOT_EXIST != ret) {
    } else {
      ret = OB_SUCCESS;
      // The first parser of a dictionary type builds it, the rest find it in the
      // map. build_lock_ keeps the build out of the shared map locks: the cache
      // load below takes the bucket lock of rw_dict_lock_ itself.
      lib::ObMutexGuard build_guard(build_lock_);
      if (OB_FAIL(find_shared_dict(key, shared))) {
        if (OB_HASH_NOT_EXIST != ret) {
        } else {
          ret = OB_SUCCESS;
          if (OB_FAIL(build_shared_dict(desc, shared))) {
          } else if (OB_FAIL(put_shared_dict(key, shared))) {
            // The item never became reachable through the map.
            if (OB_NOT_NULL(shared->dict_)) {
              shared->dict_->~ObFTRangeDict();
            }
            shared->~ObFTSharedDict();
            shared_alloc_.free(shared);
            shared = nullptr;
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(shared)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    dict = shared->dict_;
  }
  return ret;
}
int ObFTDictHub::build_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  ObFTDictInfo info;
  container.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObBucketHashWLockGuard guard(rw_dict_lock_, key.hash());

    // try if valid with no recursive lock
    if (OB_FAIL(get_dict_info(key, info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // dict not exist, make new one, by caller
        ret = OB_ENTRY_NOT_EXIST;
      } else {
      }
    } else if (OB_FAIL(ObFTRangeDict::try_load_cache(desc, info.range_count_, container))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
      } else {
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(ObFTRangeDict::build_cache_from_ik_dict(desc, container))) {
        } else if (FALSE_IT(info.range_count_ = container.get_handles().size())) {
        } else if (OB_FAIL(put_dict_info(key, info))) {
        }
      }
    }
  }
  return ret;
}

int ObFTDictHub::load_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  ObFTDictInfo info;
  container.reset();
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    {
      ObBucketHashRLockGuard guard(rw_dict_lock_, key.hash());
      if (OB_FAIL(get_dict_info(key, info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // dict not exist, make new one, by caller
          ret = OB_ENTRY_NOT_EXIST;
        } else {
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObFTRangeDict::try_load_cache(desc, info.range_count_, container))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // dict not exist, make new one, by caller
      } else {
      }
    }
  }

  return ret;
}


int ObFTDictHub::get_dict_info(const ObFTDictInfoKey &key, ObFTDictInfo &info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dict_map_.get_refactored(key, info))) {
    if (OB_HASH_NOT_EXIST != ret) {
    }
  }

  return ret;
}

int ObFTDictHub::put_dict_info(const ObFTDictInfoKey &key, const ObFTDictInfo &info)
{
  int ret = OB_SUCCESS;
  const int cover_exist_flag = 1;
  if (OB_FAIL(dict_map_.set_refactored(key, info, cover_exist_flag))) {
  }

  return ret;
}
} //  namespace storage
} //  namespace oceanbase
