//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  // LOG_DEBUG("Leaf page size = %d", max_size);
  SetMaxSize(max_size);  // leaf page size = 255
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  assert(index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index < GetSize());
  return array_[index].second;
}

/**
 * Find the corresponding value based on the target in the leaf node
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindValue(const KeyType &key, ValueType &value, const KeyComparator &comparator) const
    -> bool {
  auto compare_first = [comparator](const MappingType &lhs, KeyType rhs) -> bool {
    return comparator(lhs.first, rhs) < 0;
  };

  // 这里 - 1 是因为如果 key 的值比 array_ 中的所有值都大, 我要他返回 array_ 最后一个值
  // 如果这里不 - 1 的话, 那么最后返回的 res 会在 array_ 外面一位
  auto res = std::lower_bound(array_, array_ + GetSize() - 1, key, compare_first);
  if (comparator(key, res->first) == 0) {
    value = res->second;
    return true;
  }

  return false;
}

/**
 * Insert the <key, value> pair into the leaf page
 * (if the duplicate key found, return false)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  auto compare_first = [comparator](const MappingType &lhs, KeyType rhs) -> bool {
    return comparator(lhs.first, rhs) < 0;
  };

  int size = GetSize();
  auto it = std::lower_bound(array_, array_ + size, key, compare_first);
  if (it < array_ + size && comparator(key, it->first) == 0) {  // find the duplicate key
    return false;
  }

  // insert new <key, value> pair
  int index = std::distance(array_, it);
  std::copy_backward(array_ + index, array_ + size, array_ + size + 1);
  IncreaseSize(1);

  array_[index].first = key;
  array_[index].second = value;

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPage(page_id_t page_id) { next_page_id_ = page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *array, int min_size, int size) {
  std::copy(array + min_size, array + size, array_);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetData() -> MappingType * { return array_; }

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_LEAF_PAGE_TYPE::EraseHalf() {
//   for (int i = GetMinSize(); i < GetSize(); ++i) {
//     SetKeyValueAt(i, -1, -1);
//   }
// }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyValueAt(int index, const KeyType &key, const ValueType &value) {
  assert(index < GetSize());
  array_[index].first = key;
  array_[index].second = value;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
