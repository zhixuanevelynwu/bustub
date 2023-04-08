#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto construct_hash_join_plan = [](const NestedLoopJoinPlanNode &nlj_plan,
                                   const ComparisonExpression *expr) -> std::shared_ptr<HashJoinPlanNode> {
  if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
      left_expr != nullptr) {
    if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
        right_expr != nullptr) {
      // Ensure both exprs have tuple_id == 0
      std::vector<AbstractExpressionRef> left_key_expr(
          1, std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
      std::vector<AbstractExpressionRef> right_key_expr(
          1, std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
      if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), left_key_expr, right_key_expr,
                                                  nlj_plan.GetJoinType());
      }
      if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), right_key_expr, left_key_expr,
                                                  nlj_plan.GetJoinType());
      }
    }
  }
  return nullptr;
};

// TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
// Note for 2023 Spring: You should at least support join keys of the form:
// 1. <column expr> = <column expr>
// 2. <column expr> = <column expr> AND <column expr> = <column expr>
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // std::cout << plan->ToString() << std::endl;
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    // Case 1: <column expr> = <column expr>
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get());
        expr != nullptr && expr->comp_type_ == ComparisonType::Equal && expr->GetChildren().size() == 2) {
      return construct_hash_join_plan(nlj_plan, expr);
    }
    // Case 2: <column expr> = <column expr> AND <column expr> = <column expr>
    if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get());
        expr != nullptr && expr->logic_type_ == LogicType::And && expr->GetChildren().size() == 2) {
      const auto *expr1 = dynamic_cast<const ComparisonExpression *>(expr->GetChildren()[0].get());
      const auto *expr2 = dynamic_cast<const ComparisonExpression *>(expr->GetChildren()[1].get());
      if (expr1 != nullptr && expr1->comp_type_ == ComparisonType::Equal && expr2 != nullptr &&
          expr2->comp_type_ == ComparisonType::Equal) {
        // Construct a hash join plans for both expressions
        auto hash_join_plan1 = construct_hash_join_plan(nlj_plan, expr1);
        auto hash_join_plan2 = construct_hash_join_plan(nlj_plan, expr2);
        // Combine the hash join plans if both are valid
        if (hash_join_plan1 != nullptr && hash_join_plan2 != nullptr) {
          // Combine the key expressions from both hash join plans
          std::vector<AbstractExpressionRef> left_key_expr;
          std::vector<AbstractExpressionRef> right_key_expr;
          left_key_expr.insert(left_key_expr.end(), hash_join_plan1->LeftJoinKeyExpressions().begin(),
                               hash_join_plan1->LeftJoinKeyExpressions().end());
          left_key_expr.insert(left_key_expr.end(), hash_join_plan2->LeftJoinKeyExpressions().begin(),
                               hash_join_plan2->LeftJoinKeyExpressions().end());
          right_key_expr.insert(right_key_expr.end(), hash_join_plan1->RightJoinKeyExpressions().begin(),
                                hash_join_plan1->RightJoinKeyExpressions().end());
          right_key_expr.insert(right_key_expr.end(), hash_join_plan2->RightJoinKeyExpressions().begin(),
                                hash_join_plan2->RightJoinKeyExpressions().end());
          // Create the combined hash join plan
          return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                    nlj_plan.GetRightPlan(), left_key_expr, right_key_expr,
                                                    nlj_plan.GetJoinType());
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
