//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  auto l_schema = plan_->GetLeftPlan()->OutputSchema();
  auto r_schema = plan_->GetRightPlan()->OutputSchema();
  auto l_expression = plan_->LeftJoinKeyExpressions();
  auto r_expression = plan_->RightJoinKeyExpressions();

  // Phase #1: Build the hash table
  Tuple t;
  RID r;
  while (right_executor_->Next(&t, &r)) {
    std::vector<Value> join_keys;
    for (auto &expr : r_expression) {
      join_keys.emplace_back(expr->Evaluate(&t, r_schema));
    }
    ht_[{join_keys}].emplace_back(t);
  }

  // Phase #2: Iterate over the left table and probe the hash table
  Tuple left_tuple;
  std::vector<Value> left_keys;
  std::vector<Value> right_keys;
  left_keys.reserve(l_expression.size());
  right_keys.reserve(r_expression.size());
  while (left_executor_->Next(&left_tuple, &r)) {
    for (auto &expr : l_expression) {
      left_keys.emplace_back(expr->Evaluate(&left_tuple, l_schema));
    }
    HashJoinKey left_key{left_keys};
    if (ht_.count(left_key) > 0) {
      for (auto &right_tuple : ht_[left_key]) {
        for (auto &expr : r_expression) {
          right_keys.emplace_back(expr->Evaluate(&right_tuple, r_schema));
        }
        HashJoinKey right_key{right_keys};
        if (left_key == right_key) {
          std::vector<Value> vec;
          for (uint32_t i = 0; i < l_schema.GetColumnCount(); i++) {
            vec.emplace_back(left_tuple.GetValue(&l_schema, i));
          }
          for (uint32_t i = 0; i < r_schema.GetColumnCount(); i++) {
            vec.emplace_back(right_tuple.GetValue(&r_schema, i));
          }
          result_.emplace_back(vec, &plan_->OutputSchema());
        }
      }
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
    }
  }
  result_iter_ = result_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_iter_ == result_.end()) {
    return false;
  }
  *tuple = *result_iter_;
  ++result_iter_;
  return true;
}

}  // namespace bustub
