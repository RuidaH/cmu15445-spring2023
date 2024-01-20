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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

SeqScanExecutor::~SeqScanExecutor() {
  if (iter_ != nullptr) {
    delete iter_;
    iter_ = nullptr;
  }
}

void SeqScanExecutor::Init() {
  std::cout << "Init" << std::endl;
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->table_oid_);
  // hash join 的时候, seqscan 作为 right_child 会被多次调用 Init()
  // 所以每次初始化都需要把之前的 TableIterator 内存释放掉
  if (iter_ != nullptr) {
    delete iter_;
    iter_ = new TableIterator(table_info->table_->MakeIterator());
  } else {
    iter_ = new TableIterator(table_info->table_->MakeIterator());
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // keep iterating through the table until you find a non-deleted tuple
  while (true) {
    if (iter_->IsEnd()) {
      return false;
    }

    std::pair<TupleMeta, Tuple> &&temp_tuple = iter_->GetTuple();
    if (!temp_tuple.first.is_deleted_) {
      *tuple = std::move(temp_tuple.second);
      *rid = iter_->GetRID();
      ++(*iter_);
      return true;
    }

    ++(*iter_);
  }
}

}  // namespace bustub
