//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  indexes_info_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
  outputted = false;
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (outputted) {
    return false;
  }

  int deleted_tuple_nums = 0;
  while (child_executor_->Next(tuple, rid)) {
    TupleMeta tuple_meta{INVALID_TXN_ID, INVALID_TXN_ID, true};
    table_info_->table_->UpdateTupleMeta(tuple_meta, *rid);
    ++deleted_tuple_nums;

    for (auto& index_info : indexes_info_) {
      Tuple key = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> values {{TypeId::INTEGER, deleted_tuple_nums}};
  Tuple output_tuple(values, &GetOutputSchema());
  *tuple = output_tuple;
  outputted = true;
  return true;
}

}  // namespace bustub
