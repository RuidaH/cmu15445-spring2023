//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t parent_page_id, int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetParentPageId(parent_page_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // assert(index != 0 && index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  // assert(index != 0 && index < GetSize());
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index < GetSize());
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyValueAt(int index, const KeyType &key, const ValueType &value) {
  // assert(index < GetSize());
  array_[index].first = key;
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  // assert(index < GetSize());
  array_[index].second = value;
}

/**
 * Find the right next page id (ValueType) based on the given key.
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindValue(KeyType key, const KeyComparator &comparator,
                                               int *child_page_index) const -> ValueType {
  // 这里需要 <=; 因为搜索的 key 是有可能跟中间节点的 key 相等的
  auto compare_first = [comparator](const MappingType &lhs, KeyType rhs) -> bool {
    return comparator(lhs.first, rhs) <= 0;
  };

  auto res = std::lower_bound(array_, array_ + GetSize(), key, compare_first);
  res = std::prev(res);
  // res = (res == array_ + GetSize()) ? std::prev(res) : res;

  // 记录一下孩子节点的索引下标, 用于删除
  if (child_page_index != nullptr) {
    *child_page_index = std::distance(array_, res);
  }

  // LOG_DEBUG("Internal page: %s", ToString().c_str());
  // LOG_DEBUG("Internal page findValue() result: <%s, %s>", std::to_string(res->first.ToString()).c_str(),
  //           std::to_string(res->second).c_str());

  return res->second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  // LOG_DEBUG("---- Internal page size (before insertion): %d", GetSize());

  int size = GetSize();

  // 好离谱的错误
  // if (comparator(key, array_[size - 1].first) > 0) {
  //   array_[size].first = key;
  //   array_[size].second = value;
  //   IncreaseSize(1);
  //   // LOG_DEBUG("---- Internal page size (after insertion): %d", GetSize());
  //   return true;
  // }

  // insert 的特殊情况: 中间节点 pushed_key 是有可能作为新节点的第一位的
  if (comparator(key, array_[0].first) < 0) {
    std::copy_backward(array_, array_ + size, array_ + size + 1);
    IncreaseSize(1);
    array_[0].first = key;
    array_[0].second = value;
    // LOG_DEBUG("---- Internal page size (after insertion): %d", GetSize());
    return true;
  }

  auto compare_first = [comparator](const MappingType &lhs, KeyType rhs) -> bool {
    return comparator(lhs.first, rhs) < 0;
  };

  auto it = std::lower_bound(array_ + 1, array_ + size, key, compare_first);  // don't insert into index 0

  // insert new <key, value> pair
  int index = std::distance(array_, it);
  std::copy_backward(array_ + index, array_ + size, array_ + size + 1);
  IncreaseSize(1);
  array_[index] = std::make_pair(key, value);

  // LOG_DEBUG("Internal page insertion: distance %d", index);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  auto compare_first = [comparator](const MappingType &lhs, KeyType rhs) -> bool {
    return comparator(lhs.first, rhs) < 0;
  };

  auto res = std::lower_bound(array_, array_ + GetSize() - 1, key, compare_first);
  if (comparator(key, res->first) == 0 && value == res->second) {
    int dist = std::distance(array_, res);
    std::copy(array_ + dist + 1, array_ + GetSize(), array_ + dist);
    IncreaseSize(-1);
    return true;
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(MappingType *array, int size) {
  std::copy(array, array + size, array_ + GetSize());
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ShiftData(int dist) {
  if (dist > 0) {  // 向右移动
    std::copy_backward(array_, array_ + GetSize(), array_ + GetSize() + dist);
  } else if (dist < 0) {  // 向左移动
    std::copy(array_ - dist, array_ + GetSize(), array_);
  }
  IncreaseSize(dist);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetParentPageId() -> page_id_t { return parent_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(MappingType *array, int min_size, int size) {
  std::copy(array + min_size, array + size, array_);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetData() -> MappingType * { return array_; }

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::EraseHalf() {
//   for (int i = GetMinSize(); i < GetSize(); ++i) {
//     SetKeyValueAt(i, -1, -1);
//   }
// }

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
