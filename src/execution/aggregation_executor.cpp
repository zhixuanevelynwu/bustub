//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_{std::move(child)},
      aht_({plan->aggregates_, plan->agg_types_}),
      aht_iterator_({aht_.Begin()}) {}

/**
 * @brief Carefully decide whether the build phase of the aggregation should be performed in AggregationExecutor::Init()
 * or AggregationExecutor::Next().
 *
 */
void AggregationExecutor::Init() {
  child_->Init();
  Tuple t;
  RID r;
  // build the hash table
  while (child_->Next(&t, &r)) {
    aht_.InsertCombine(MakeAggregateKey(&t), MakeAggregateValue(&t));
  }
  aht_iterator_ = aht_.Begin();
  // populate empty tuple if no group by
  if (aht_iterator_ == aht_.End() && plan_->GetGroupBys().empty()) {
    aht_.InsertEmpty();
    aht_iterator_ = aht_.Begin();
  }
  // no groups, no output
}

/**
 * Yield the next tuple from the insert.
 * @param[out] tuple The next tuple produced by the aggregation
 * @param[out] rid The next tuple RID produced by the aggregation
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ != aht_.End()) {
    std::vector<Value> vec;
    vec.insert(vec.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
    vec.insert(vec.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
    *tuple = {vec, &GetOutputSchema()};
    *rid = tuple->GetRid();
    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
