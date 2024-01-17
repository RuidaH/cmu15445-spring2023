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
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_child)), right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  ht_.clear();
  left_executor_->Init();
  right_executor_->Init();

  // 构建 hash join table
  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    auto left_plan = plan_->GetLeftPlan();
    auto &left_join_key_expr = plan_->LeftJoinKeyExpressions();
    for (auto &expr : left_join_key_expr) {
      std::cout << expr->Evaluate(&tuple, left_plan->OutputSchema()).ToString() << std::endl;
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  return false;
}

}  // namespace bustub

/**
 * Comparison Expression
 * Column Value Expression
 * Column Value Expression
*/