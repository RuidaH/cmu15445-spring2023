//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(int size) { size_ = size; }
void BPlusTreePage::IncreaseSize(int amount) { size_ += amount; }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
auto BPlusTreePage::GetMinSize() const -> int {
  if (page_type_ == IndexPageType::INTERNAL_PAGE) {
    return (max_size_ + 1) / 2;
  }
  return max_size_ / 2;
}

// insert: true; delete: false;
auto BPlusTreePage::IsSafe(OperationType op_type) const -> bool {
  if (page_type_ == IndexPageType::INTERNAL_PAGE) {
    return op_type == OperationType::INSERT ? GetSize() < GetMaxSize() : GetSize() > GetMinSize();
  }
  // page_type_ == IndexPageType::LEAF_PAGE
  // return op_type == OperationType::INSERT ? GetSize() < GetMaxSize() - 1 : GetSize() > GetMinSize();
  return op_type == OperationType::INSERT ? GetSize() < GetMaxSize() - 2 : GetSize() > GetMinSize();
}

}  // namespace bustub
