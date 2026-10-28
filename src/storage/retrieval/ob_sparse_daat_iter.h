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

#ifndef OB_SPARSE_DAAT_ITER_H_
#define OB_SPARSE_DAAT_ITER_H_

#include "ob_i_sparse_retrieval_iter.h"
#include "ob_sparse_utils.h"
#include "storage/access/ob_scan_merge_loser_tree.h"
#include "storage/access/ob_simple_rows_merger.h"

namespace oceanbase
{
namespace storage
{

struct ObSRMergeItem
{
  ObSRMergeItem() : relevance_(0.0), iter_idx_(-1), equal_with_next_(false) {}
  ~ObSRMergeItem() = default;
  TO_STRING_KV(K_(iter_idx), K_(relevance), K_(equal_with_next));

  double relevance_;
  int64_t iter_idx_;
  bool equal_with_next_;
};

struct ObSRMergeCmp
{
  enum CmpMode
  {
    GENERIC_CMP = 0,
    BINARY_CMP = 1,
  };

  ObSRMergeCmp();
  virtual ~ObSRMergeCmp() {}

  int init(sql::ObDatumMeta id_meta, const ObFixedArray<const ObDatum *, ObIAllocator> *iter_ids);
  // A domain id of a fulltext index is stored as a binary string. Comparing two of them through the
  // generic datum comparison costs a collation validity check, a charset lookup and an indirect call
  // per duel, so do the plain byte comparison that BINARY collation is defined as when possible.
  OB_INLINE int cmp(const ObSRMergeItem &l, const ObSRMergeItem &r, int64_t &cmp_ret)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else {
      const ObDatum &l_datum = get_id_datum(l.iter_idx_);
      const ObDatum &r_datum = get_id_datum(r.iter_idx_);
      int tmp_ret = 0;
      if (OB_UNLIKELY(l_datum.is_null()) || OB_UNLIKELY(r_datum.is_null())) {
        // a domain id is never null, keep the generic comparison for that (unexpected) case
        if (OB_FAIL(cmp_func_(l_datum, r_datum, tmp_ret, nullptr))) {
        }
      } else if (OB_LIKELY(BINARY_CMP == cmp_mode_)) {
        const int64_t l_len = l_datum.len_;
        const int64_t r_len = r_datum.len_;
        const int64_t cmp_len = MIN(l_len, r_len);
        const int cmp_res = cmp_len > 0 ? MEMCMP(l_datum.ptr_, r_datum.ptr_, cmp_len) : 0;
        tmp_ret = 0 != cmp_res ? cmp_res : static_cast<int>(l_len - r_len);
      } else if (OB_FAIL(cmp_func_(l_datum, r_datum, tmp_ret, nullptr))) {
      }
      if (OB_SUCC(ret)) {
        cmp_ret = tmp_ret;
      }
    }
    return ret;
  }
private:
  inline const ObDatum &get_id_datum(const int64_t iter_idx)
  {
    const ObDatum *datum = iter_ids_->get_data()[iter_idx];
    OB_ASSERT(nullptr != datum);
    return *datum;
  }
private:
  common::ObDatumCmpFuncType cmp_func_;
  // TODO: if memory lifetime of docid datum is guaranteed by dim_iters, we can use pointer to datum directly
  //       and avoid deep copy into merge heap here
  const ObFixedArray<const ObDatum *, ObIAllocator> *iter_ids_;
  CmpMode cmp_mode_;
  bool is_inited_;
};

typedef ObSimpleRowsMerger<ObSRMergeItem, ObSRMergeCmp> ObSRSimpleMerger;
typedef ObMergeLoserTree<ObSRMergeItem, ObSRMergeCmp> ObSRLoserTree;
typedef common::ObRowsMerger<ObSRMergeItem, ObSRMergeCmp> ObSRMergeHeap;

// implementation of basic DaaT query processing algorithm primitives
class ObSRDaaTIterImpl : public ObISparseRetrievalMergeIter
{
public:
  ObSRDaaTIterImpl();
  virtual ~ObSRDaaTIterImpl() {}
  virtual int get_next_row() override;
  virtual int get_next_rows(const int64_t capacity, int64_t &count) override;
  int init(
      ObSparseRetrievalMergeParam &iter_param,
      ObIArray<ObISRDaaTDimIter *> &dim_iters,
      ObIAllocator &iter_allocator,
      ObSRDaaTRelevanceCollector &relevance_collector);
  virtual void reuse(const bool switch_tablet = false) override;
  virtual void reset() override;
  virtual int get_query_max_score(double &score) override;
  
  INHERIT_TO_STRING_KV("ObISparseRetrievalMergeIter", ObISparseRetrievalMergeIter,
      K_(next_round_iter_idxes), K_(next_round_cnt));
protected:
  virtual int pre_process();
  virtual int do_one_merge_round(int64_t &count);
  virtual int fill_merge_heap();
  virtual int collect_dims_by_id(const ObDatum *&id_datum, double &relevance, bool &got_valid_id);
  virtual int process_collected_row(const ObDatum &id_datum, const double relevance);
  virtual int filter_on_demand(const int64_t count, const double relevance, bool &need_project);
  virtual int cache_result(int64_t &count, const ObDatum &id_datum, const double relevance);
  virtual int project_results(const int64_t count);
  int init_merge_heap(const int64_t count);
protected:
  ObIAllocator *iter_allocator_;
  ObSparseRetrievalMergeParam *iter_param_;
  ObIArray<ObISRDaaTDimIter *> *dim_iters_;
  ObSRMergeCmp merge_cmp_;
  ObSRMergeHeap *merge_heap_;
  ObSRDaaTRelevanceCollector *relevance_collector_;
  ObFixedArray<const ObDatum *, ObIAllocator> iter_domain_ids_; // record every dim iter's output domain id, one (ObDatum *) for one dim iter
  ObFixedArray<ObDocIdExt, ObIAllocator> buffered_domain_ids_; // cache for output
  ObFixedArray<double, ObIAllocator> buffered_relevances_;
  ObFixedArray<int64_t, ObIAllocator> next_round_iter_idxes_;
  int64_t next_round_cnt_;
  void (*set_datum_func_)(ObDatum &, const ObDocIdExt &);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSRDaaTIterImpl);
};

} // namespace storage
} // namespace oceanbase

#endif
