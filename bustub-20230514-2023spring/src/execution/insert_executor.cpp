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
}

// before insert, make sure the table exists in the database
auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {

    // 这里还漏了 child_executor 可能会产出多条 inserted tuples

    // acquire the inserted tuple from the child_executor_
    child_executor_->Next(tuple, rid);

    // insert the tuple and tuple_meta into the table
    TupleMeta tuple_meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    *rid = table_info_->table_->InsertTuple(tuple_meta, *tuple, nullptr, nullptr, table_info_->oid_);

    // update the index if there is one
    for (auto index_info : indexes_info_) {
        if (!index_info->index_->InsertEntry(*tuple, *rid, nullptr)) {
            return false;
        }
    }
    
    return true;
}

}  // namespace bustub
