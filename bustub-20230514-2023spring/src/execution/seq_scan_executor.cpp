//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
    auto catalog = exec_ctx_->GetCatalog();
    auto table_info = catalog->GetTable();
    // 这里返回的是初始化列表, 所以需要使用指针
    iter_ = new TableIterator(table_info->table_->MakeIterator());
}

// next tuple and the corresponding rid should be assigned to the param
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    // keep iterating through the table until you find a non-deleted tuple
    while (true) {
        if (iter_->IsEnd()) {
            return false;
        }

        if (!(*iter_)->GetTuple().first.is_deleted_) {
            *tuple = (*iter_)->GetTuple();
            *rid = (*iter_)->GetRID();
            ++(*iter_);
            return true;
        }

        ++(*iter_);
    }
}

}  // namespace bustub
