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
      child_(std::move(child)),
      aht_(plan_->aggregates_, plan_->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  // LOG_DEBUG("plan->group by size(): %zu", plan_->GetGroupBys().size());
  // for (auto &expr : plan_->GetGroupBys()) {
  //   LOG_DEBUG("group-by: %s", expr->ToString().c_str());
  // }
  // for (auto &expr : plan_->GetAggregates()) {
  //   LOG_DEBUG("aggregate: %s", expr->ToString().c_str());
  // }
  // for (auto &expr : plan_->GetAggregateTypes()) {
  //   LOG_DEBUG("aggregate type: %d", expr);
  // }

  // 遍历 child_executor 提前在 aht_ 中聚合好数据
  // 当前 plan_node 被复用的话就需要重新初始化
  child_->Init();
  aht_.Clear();
  num_of_tuples_ = 0;
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    ++num_of_tuples_;
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 空表搭配 group by 不需要有任何输出
  if (num_of_tuples_ == 0 && plan_->group_bys_.empty()) {  // empty table
    Tuple output_tuple(aht_.GenerateInitialAggregateValue().aggregates_, &plan_->OutputSchema());
    *tuple = output_tuple;
    *rid = output_tuple.GetRid();
    --num_of_tuples_;  // 防止下次再次进入相同的逻辑
    return true;
  }

  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  std::vector<Value> values = aht_iterator_.Key().group_bys_;
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
  Tuple output_tuple(values, &plan_->OutputSchema());

  // LOG_DEBUG("output tuple: %s", output_tuple.ToString(&plan_->OutputSchema()).c_str());

  *tuple = output_tuple;
  *rid = output_tuple.GetRid();
  ++aht_iterator_;

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
