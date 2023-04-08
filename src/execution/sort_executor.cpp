#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    sorted_tuples_.push_back(t);
  }

  auto order_bys = plan_->GetOrderBy();
  auto schema = plan_->OutputSchema();
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), [order_bys, schema](const Tuple &t1, const Tuple &t2) {
    for (const auto &order_by : order_bys) {
      auto order_by_type = order_by.first;
      auto expr = order_by.second;
      if (static_cast<bool>(expr->Evaluate(&t1, schema).CompareEquals(expr->Evaluate(&t2, schema)))) {
        continue;
      }
      switch (order_by_type) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          return static_cast<bool>(expr->Evaluate(&t1, schema).CompareLessThan(expr->Evaluate(&t2, schema)));
        case OrderByType::DESC:
          return static_cast<bool>(expr->Evaluate(&t1, schema).CompareGreaterThan(expr->Evaluate(&t2, schema)));
      }
    }
    return false;
  });
  iter_ = sorted_tuples_.begin();
}

/**
 * Yield the next tuple from the sort.
 * @param[out] tuple The next tuple produced by the sort
 * @param[out] rid The next tuple RID produced by the sort
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == sorted_tuples_.end()) {
    return false;
  }
  *tuple = *iter_;
  *rid = tuple->GetRid();
  ++iter_;
  return true;
}

}  // namespace bustub
