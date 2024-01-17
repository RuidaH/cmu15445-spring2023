#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    // std::cout << plan->ToString() << std::endl;
    // std::cout << "Predicate of the plan: " << nlj_plan.Predicate()->ToString() << std::endl << std::endl; // e.g. (#0.0=#1.0)

    // 这个写法很妙
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {  // <column expr> = <column expr>
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get()); 
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get()); 
              right_expr != nullptr) {
              auto left_expr_tuple_0 = std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
              auto right_expr_tuple_0 = std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());
              std::vector<AbstractExpressionRef> left_exprs{left_expr_tuple_0};
              std::vector<AbstractExpressionRef> right_exprs{right_expr_tuple_0}; 

              // column from outer table is on the left hand side (不确定这里需不需要加上 SeqScan 的检测)
              if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
                return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), 
                                                          nlj_plan.GetRightPlan(), left_exprs, 
                                                          right_exprs, nlj_plan.GetJoinType());
              }
              if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
                return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetRightPlan(), 
                                                          nlj_plan.GetLeftPlan(), right_exprs, 
                                                          left_exprs, nlj_plan.GetJoinType());
              }
          }
        }
      }
    } 
    // else if (const auto *expr = dynamic_cast<const LogicExpression*>(nlj_plan.Predicate().get()); expr != nullptr) {  // <column expr> = <column expr> AND <column expr> = <column expr>
    //   if (expr->logic_type_ == LogicType::And) {
    //     std::cout << nlj_plan.Predicate()->ToString() << std::endl;
    //   }
    // }
  }

  return optimized_plan;
}

}  // namespace bustub


/**
 * You should modify the optimiser to transform a NestedLoopJoinPlanNode into a HashJoinPlanNode
 *   when it is possible to use a hash join.
 * 
 * After applying the NLJHashJoin optimiser rule, the left and right join key expression will be 
 *   extracted from the single join predicate in the NestedLoopJoinPlanNode
 * 
 * E.g.
 * NestedLoopJoin { type=Inner, predicate=(#0.0=#1.0) } | (s1.col1:INTEGER, s2.col1:INTEGER, s2.col2:INTEGER)
  SeqScan { table=test_simple_seq_1 } | (s1.col1:INTEGER)
  SeqScan { table=test_simple_seq_2 } | (s2.col1:INTEGER, s2.col2:INTEGER)

  HashJoin { type=Inner, left_key=[#0.1], right_key=[#0.0] } | (__mock_table_tas_2023.github_id:VARCHAR, __mock_table_tas_2023.office_hour:VARCHAR, __mock_table_schedule_2023.day_of_week:VARCHAR, __mock_table_schedule_2023.has_lecture:INTEGER)
  MockScan { table=__mock_table_tas_2023 } | (__mock_table_tas_2023.github_id:VARCHAR, __mock_table_tas_2023.office_hour:VARCHAR)
  MockScan { table=__mock_table_schedule_2023 } | (__mock_table_schedule_2023.day_of_week:VARCHAR, __mock_table_schedule_2023.has_lecture:INTEGER)
 * 
*/