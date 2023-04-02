//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {

/**
 * @brief The IndexScanExecutor iterates over an index to retrieve RIDs for tuples. The operator then uses these RIDs to
 * retrieve their tuples in the corresponding table. It then emits these tuples one at a time.
 *
 * @param exec_ctx
 * @param plan
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())),
      iter_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())->GetBeginIterator()) {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_.IsEnd()) {
    auto r = (*iter_).second;
    auto [m, t] = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)->table_->GetTuple(r);
    ++iter_;
    if (!m.is_deleted_) {
      *tuple = t;
      *rid = r;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
