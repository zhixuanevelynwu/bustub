//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple right_tuple;
  RID r1;
  RID r2;
  has_left_tuple_ = left_executor_->Next(&left_tuple_, &r1);
  while (right_executor_->Next(&right_tuple, &r2)) {
    right_tuples_.emplace_back(right_tuple);
  }
  right_tuples_iterator_ = right_tuples_.begin();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto l_schema = plan_->GetLeftPlan()->OutputSchema();
  auto r_schema = plan_->GetRightPlan()->OutputSchema();
  while (has_left_tuple_) {
    if (right_tuples_iterator_ >= right_tuples_.end()) {  // wrap-around
      // left join on non-matching tuple
      if (plan_->GetJoinType() == JoinType::LEFT && !matched_) {
        matched_ = true;
        std::vector<Value> vec;
        for (uint32_t i = 0; i < l_schema.GetColumnCount(); i++) {
          vec.emplace_back(left_tuple_.GetValue(&l_schema, i));
        }
        for (uint32_t i = 0; i < r_schema.GetColumnCount(); i++) {
          vec.emplace_back(ValueFactory::GetNullValueByType(r_schema.GetColumn(i).GetType()));
        }
        *tuple = {vec, &plan_->OutputSchema()};
        return true;
      }
      matched_ = false;
      right_executor_->Init();
      right_tuples_iterator_ = right_tuples_.begin();
      if (!left_executor_->Next(&left_tuple_, rid)) {
        return false;
      }
    }
    // inner join on predicate
    for (auto iter = right_tuples_iterator_; iter != right_tuples_.end(); ++iter) {
      auto right_tuple = *iter;
      if (!plan_->Predicate()->EvaluateJoin(&left_tuple_, l_schema, &right_tuple, r_schema).GetAs<bool>()) {
        continue;
      }
      matched_ = true;
      std::vector<Value> vec;
      for (uint32_t i = 0; i < l_schema.GetColumnCount(); i++) {
        vec.emplace_back(left_tuple_.GetValue(&l_schema, i));
      }
      for (uint32_t i = 0; i < r_schema.GetColumnCount(); i++) {
        vec.emplace_back(right_tuple.GetValue(&r_schema, i));
      }
      *tuple = {vec, &plan_->OutputSchema()};
      right_tuples_iterator_ = ++iter;
      return true;
    }
    right_tuples_iterator_ = right_tuples_.end();
  }
  return false;
}

}  // namespace bustub
