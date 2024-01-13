//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

// acquire table_info_ and index_info_
void InsertExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());  // table_info_->table_
  indexes_info_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
  outputted = false;
}

// before insert, make sure the table exists in the database
auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (outputted) {  // 测试程序会一直调用该方法, 所以如果已经插入成功, 那么应该停止执行
    return false;
  }

  int inserted_tuple_nums = 0;
  while (child_executor_->Next(tuple, rid)) {
    TupleMeta tuple_meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    std::optional<RID> result = table_info_->table_->InsertTuple(tuple_meta, *tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), table_info_->oid_);
    if (!result.has_value()) {
      return false;
    }
    *rid = result.value();
    ++inserted_tuple_nums;

    // LOG_DEBUG("insert tuple: %s", tuple->ToString(&GetOutputSchema()).c_str());
    // LOG_DEBUG("rid of inserted tuple: %s\n", rid->ToString().c_str());

    for (auto& index_info : indexes_info_) {
      Tuple key = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      if (!index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction())) {
        return false;
      }
    }
  }

  // 最后的 tuple 应该包含插入的 tuple 数量的信息
  std::vector<Value> values{{TypeId::INTEGER, inserted_tuple_nums}};
  Tuple output_tuple(values, &GetOutputSchema());
  *tuple = output_tuple;

  outputted = true;
  return true;
}

}  // namespace bustub
