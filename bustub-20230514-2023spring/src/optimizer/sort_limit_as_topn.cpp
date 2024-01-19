#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

// implement sort + limit -> top N optimizer rule
auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // if (optimized_plan->GetType() == PlanType::Limit) {
  //   BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "Limit with multiple children?? Impossible!");
  //   const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
  //   const auto &top_n = limit_plan.GetLimit();
  //   if (auto optimized_child_plan = limit_plan.GetChildAt(0); optimized_child_plan != nullptr) {  // 这里的 plan
  //   用了最开始的 plan, 所以得不到最后的结果
  //     if (optimized_child_plan->GetType() == PlanType::Sort) {
  //       // 这里的 sort_plan 会不会有可能在其他位置 (check in test)
  //       const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*optimized_child_plan);
  //       const auto &order_bys = sort_plan.GetOrderBy();

  //       auto new_plan = std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort_plan.GetChildAt(0),
  //       order_bys, top_n); std::cout << new_plan->ToString() << std::endl; return new_plan;
  //     }
  //   }
  // }

  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    const auto &top_n = limit_plan.GetLimit();
    BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "Limit with multiple children?? Impossible!");
    // 这里应该回头去看一下 LimitPlanNode 的成员函数, LimitPlanNode 最多只有一个 child, 调用 GetChildPlan()
    // 还有一个错误: 记得使用 dynamic_cast 之后的 plan 来调用成员函数
    const auto child_plan = limit_plan.GetChildPlan();
    if (child_plan->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child_plan);
      const auto &order_bys = sort_plan.GetOrderBy();
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort_plan.GetChildPlan(), order_bys, top_n);
    }
  }

  return optimized_plan;
}

}  // namespace bustub
