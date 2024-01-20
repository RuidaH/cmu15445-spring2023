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
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  right_executor_->Init();

  if (build_) {
    return;
  }

  build_ = true;
  ht_.clear();
  not_joined_.clear();
  left_executor_->Init();
  tuple_bucket_ = std::nullopt;
  cur_index_ = 0;

  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {  // 构建 hash join table
    JoinKey left_join_key = GenerateJoinKey(plan_->GetLeftPlan(), plan_->LeftJoinKeyExpressions(), tuple);
    InsertJoinKey(left_join_key, tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto &left_table_schema = plan_->GetLeftPlan()->OutputSchema();
  auto &right_table_schema = plan_->GetRightPlan()->OutputSchema();

  while (true) {
    if (tuple_bucket_.has_value() && cur_index_ < tuple_bucket_.value().size()) {
      Tuple left_tuple = tuple_bucket_.value().at(cur_index_);
      ++cur_index_;

      if (right_finished_) {
        OutputTuple(left_table_schema, right_table_schema, &left_tuple, tuple, false);  // left join
      } else {
        OutputTuple(left_table_schema, right_table_schema, &left_tuple, tuple, true);
      }
      return true;
    }

    // 到这里一个 right tuple 对应的 left tuple 全部匹配完毕
    right_tuple_ = {};
    tuple_bucket_ = std::nullopt;
    cur_index_ = 0;

    JoinKey key;
    const auto status = right_executor_->Next(&right_tuple_, rid);
    if (!status) {
      if (plan_->GetJoinType() == JoinType::INNER) {
        return false;
      }

      // 处理 left join
      right_finished_ = true;
      const auto not_joined_iter = not_joined_.begin();
      if (not_joined_iter != not_joined_.end()) {
        key = (*not_joined_iter);
      } else {
        return false;  // not_joined 中的 left key 已经全部处理完毕了, 直接返回
      }
    }

    // 拿到下一个的 right tuple
    if (!right_finished_) {
      key = GenerateJoinKey(plan_->GetRightPlan(), plan_->RightJoinKeyExpressions(), right_tuple_);
    }

    tuple_bucket_ = GetTupleBucket(key);
    not_joined_.erase(key);
  }
}

void HashJoinExecutor::OutputTuple(const Schema &left_table_schema, const Schema &right_table_schema, Tuple *left_tuple,
                                   Tuple *tuple, bool matched) {
  std::vector<Value> new_tuple_values;
  new_tuple_values.reserve(
      GetOutputSchema().GetColumnCount());  // 提前分配好固定大小的内存空间, 避免动态扩容 (很聪明的优化)

  for (uint32_t i = 0; i < left_table_schema.GetColumnCount(); ++i) {
    new_tuple_values.emplace_back(left_tuple->GetValue(&left_table_schema, i));
  }

  if (!matched) {
    for (uint32_t i = 0; i < right_table_schema.GetColumnCount(); ++i) {
      auto type_id = right_table_schema.GetColumn(i).GetType();
      new_tuple_values.emplace_back(ValueFactory::GetNullValueByType(type_id));
    }
  } else {
    for (uint32_t i = 0; i < right_table_schema.GetColumnCount(); ++i) {
      new_tuple_values.emplace_back(right_tuple_.GetValue(&right_table_schema, i));
    }
  }

  *tuple = {new_tuple_values, &GetOutputSchema()};  // avoid copy
  // std::cout << "output tuple: " << tuple->ToString(&GetOutputSchema()) << std::endl;
}
}  // namespace bustub
