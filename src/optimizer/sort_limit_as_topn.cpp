#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

/**
 * @brief Use topN for queries containing ORDER BY and LIMIT clauses.
 *
 * @param plan
 * @return AbstractPlanNodeRef
 */
auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) {
    if (optimized_plan->children_.size() == 1 && optimized_plan->GetChildAt(0)->GetType() == PlanType::Sort) {
      const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*(optimized_plan->children_[0]));
      return std::make_shared<TopNPlanNode>(plan->output_schema_, sort_plan.GetChildAt(0), sort_plan.GetOrderBy(),
                                            limit_plan.GetLimit());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
