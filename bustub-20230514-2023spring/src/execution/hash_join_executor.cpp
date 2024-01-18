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
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_child)), right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  ht_.clear();
  not_joined_.clear();
  tuple_bucket_ = std::nullopt;
  right_tuple_ = new Tuple();
  finished_ = false;

  left_executor_->Init();
  right_executor_->Init();

  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {  // 构建 hash join table
    JoinKey left_join_key = GenerateJoinKey(plan_->GetLeftPlan(), plan_->LeftJoinKeyExpressions(), tuple);
    InsertJoinKey(left_join_key, tuple);
  }

  // 这里会不会多次被初始化, 如果会的话就加上一个 build_ 来防止
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto &left_table_schema = plan_->GetLeftPlan()->OutputSchema();
  auto &right_table_schema = plan_->GetRightPlan()->OutputSchema();
  auto &right_join_key_expr = plan_->RightJoinKeyExpressions();
  auto right_plan = plan_->GetRightPlan();

  while (true) {

    if (!tuple_bucket_.has_value()) {
      bool status = right_executor_->Next(right_tuple_, rid);
      if (!status) {
        right_tuple_ = nullptr;
        finished_ = true;
      } else {
        JoinKey join_key = GenerateJoinKey(right_plan, right_join_key_expr, *right_tuple_);
        tuple_bucket_ = GetTupleBucket(join_key);
        if (!tuple_bucket_.has_value()) {
          continue;  // right_tuple_ doesn't find the matched left tuples, iterate to the next right tuple
        } else {
          not_joined_.erase(join_key);  // matches, those left tuples won't be used in left join
        }
      }
    }

    if (!finished_ && !tuple_bucket_.value().empty()) {
      Tuple left_tuple = tuple_bucket_.value().back();
      tuple_bucket_.value().pop_back();
      if (right_tuple_ == nullptr) {  // cope with the left join
        OutputTuple(left_table_schema, right_table_schema, &left_tuple, tuple, false);
      } else {
        OutputTuple(left_table_schema, right_table_schema, &left_tuple, tuple, true);
      }
      
      if (tuple_bucket_.value().empty()) {
        tuple_bucket_ = std::nullopt;
      }
      return true;
    }

    if (finished_) {
      // deal with the left tuples without a match in left join
      finished_ = false;
      if (plan_->GetJoinType() == JoinType::LEFT && !not_joined_.empty()) {
        auto iter = not_joined_.begin();
        tuple_bucket_ = GetTupleBucket(*iter);
        not_joined_.erase(iter);
        continue;  // enter next loop to deal with the rest of the left tuple
      } else {
        return false;
      }
    }

  }
}

void HashJoinExecutor::OutputTuple(const Schema &left_table_schema, const Schema &right_table_schema, Tuple *left_tuple, Tuple *tuple, bool matched) {
  std::vector<Value> new_tuple_values;
  new_tuple_values.reserve(GetOutputSchema().GetColumnCount());  // 提前分配好固定大小的内存空间, 避免动态扩容 (很聪明的优化)

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
      new_tuple_values.emplace_back(right_tuple_->GetValue(&right_table_schema, i));
    }
    
  }

  *tuple = {new_tuple_values, &GetOutputSchema()};  // avoid copy
}
}  // namespace bustub

/**
 * Comparison Expression
 * Column Value Expression
 * Column Value Expression
*/