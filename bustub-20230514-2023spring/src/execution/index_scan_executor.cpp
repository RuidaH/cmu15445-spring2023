//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

// 索引创建不包含在此阶段

// 在 Init 阶段拿到 B+ 树索引的迭代器
void IndexScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  index_info_ = catalog->GetIndex(plan_->GetIndexOid());
  table_info_ = catalog->GetTable(index_info_->table_name_);
  auto *b_plus_tree_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());

  tree_iter_ = b_plus_tree_index->GetBeginIterator();
  tree_end_iter_ = b_plus_tree_index->GetEndIterator();
}

// 在 Next 阶段使用迭代器遍历并返回结果
auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (tree_iter_ == tree_end_iter_) {
      return false;
    }

    *rid = (*tree_iter_).second;
    std::pair<TupleMeta, Tuple> &&tuple_pair = table_info_->table_->GetTuple(*rid);
    if (!tuple_pair.first.is_deleted_) {
      *tuple = std::move(tuple_pair.second);
      ++tree_iter_;
      return true;
    }

    ++tree_iter_;
  }
}

}  // namespace bustub
