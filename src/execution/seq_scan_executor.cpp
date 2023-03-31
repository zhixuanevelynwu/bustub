//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_{exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_->MakeIterator()} {}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // const auto table_meta = exec_ctx_->GetCatalog()->GetTable(plan_->table_name_);
  // emit the next non-deleted tuple
  while (!iter_.IsEnd()) {
    auto [m, t] = iter_.GetTuple();
    auto r = iter_.GetRID();
    ++iter_;
    // std::cout << t.ToString(&(table_meta->schema_)) << std::endl;
    // std::cout << "current page_id: " << r.GetPageId() << ", " << r.GetSlotNum() << std::endl;
    if (!m.is_deleted_) {
      *tuple = t;
      *rid = r;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
