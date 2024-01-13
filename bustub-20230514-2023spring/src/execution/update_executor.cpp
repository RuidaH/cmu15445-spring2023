//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  indexes_info_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
  outputted = false;
}

// 析构函数需不需要覆写 ??? (删除某些指针指向的内存区域)

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (outputted) {
    return false;
  }

  int update_tuple_nums = 0;
  while (child_executor_->Next(tuple, rid)) {
    // 先删除后插入
    TupleMeta tuple_meta{INVALID_TXN_ID, INVALID_TXN_ID, true};
    table_info_->table_->UpdateTupleMeta(tuple_meta, *rid);

    // e.g. plan_->target_expressions_ 会输出 3 列, 第三列的值被设定为 445 [#0.0, #0.1, 445]
    // target_expressions_ 中的每一个 expression 都可以是不同的类型 (派生类); 通过 Evaluate 来获取对应的值
    std::vector<Value> tuple_values;
    for (auto expr : plan_->target_expressions_) {
      // LOG_DEBUG("target expression: %s", target_expr->ToString().c_str());
      tuple_values.push_back(expr->Evaluate(tuple, table_info_->schema_));
    }
    Tuple updated_tuple(tuple_values, &table_info_->schema_);
    tuple_meta.is_deleted_ = false;
    std::optional<RID> result = table_info_->table_->InsertTuple(tuple_meta, updated_tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->TableOid());
    if (!result.has_value()) {
      return false;
    }
    *rid = result.value();
    ++update_tuple_nums;

    // LOG_DEBUG("insert tuple: %s", tuple->ToString(&child_executor_->GetOutputSchema()).c_str());
    // LOG_DEBUG("rid of inserted tuple: %s", rid->ToString().c_str());
    // LOG_DEBUG("only child of the update node:\n %s\n", plan_->GetChildPlan()->ToString().c_str());

    for (auto& index_info : indexes_info_) {
      index_info->index_->DeleteEntry(*tuple, *rid, exec_ctx_->GetTransaction());
      bool inserted = index_info->index_->InsertEntry(*tuple, *rid, exec_ctx_->GetTransaction());
      if (!inserted) {
        return false;
      }
    }
  }

  std::vector<Value> values {{TypeId::INTEGER, update_tuple_nums}};
  Tuple output_tuple(values, &GetOutputSchema());
  *tuple = output_tuple;
  outputted = true;
  return true;
}

}  // namespace bustub
