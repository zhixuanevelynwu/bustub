#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      top_n_comp_([this](const Tuple &t1, const Tuple &t2) -> bool {
        auto schema = GetOutputSchema();
        for (const auto &order_by : plan_->GetOrderBy()) {
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
      }),
      top_entries_(top_n_comp_) {}

// populate priority queue with the top N tuple
void TopNExecutor::Init() {
  child_executor_->Init();
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    top_entries_.push(t);
    if (GetNumInHeap() > plan_->GetN()) {
      top_entries_.pop();
    }
  }
  while (!top_entries_.empty()) {
    output_tuples_.emplace_back(top_entries_.top());
    top_entries_.pop();
  }
  iter_ = output_tuples_.end();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == output_tuples_.begin()) {
    return false;
  }
  --iter_;
  *tuple = *iter_;
  *rid = tuple->GetRid();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_.size(); };

}  // namespace bustub
