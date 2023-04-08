#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  // create priority queue with comparitor constructed from the plan node
  auto order_bys = plan_->GetOrderBy();
  auto schema = plan_->OutputSchema();
  auto top_n_comp = [order_bys, schema](Tuple &t1, Tuple &t2) -> bool {
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
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(top_n_comp)> top_n(top_n_comp);
  // populate priority queue with the top N tuple
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    top_n.push(t);
    if (top_n.size() > plan_->GetN()) {
      top_n.pop();
    }
  }
  while (!top_n.empty()) {
    top_entries_.push_back(top_n.top());
    top_n.pop();
  }
  iter_ = top_entries_.end();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == top_entries_.begin()) {
    return false;
  }
  --iter_;
  *tuple = *iter_;
  *rid = tuple->GetRid();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_.size(); };

}  // namespace bustub
