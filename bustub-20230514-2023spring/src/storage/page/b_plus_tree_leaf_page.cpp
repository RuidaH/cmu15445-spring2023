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
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t parent_page_id, int max_size) {
  SetMaxSize(max_size);  // leaf page size = 255
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  next_page_id_ = INVALID_PAGE_ID;
  SetParentPageId(parent_page_id);
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindValue(const KeyType &key, ValueType &value, const KeyComparator &comparator,
                                           int *index) const -> bool {
  auto compare_first = [comparator](const MappingType &lhs, KeyType rhs) -> bool {
    return comparator(lhs.first, rhs) < 0;
  };

  auto res = std::lower_bound(array_, array_ + GetSize() - 1, key, compare_first);
  if (comparator(key, res->first) == 0) {
    value = res->second;

    if (index != nullptr) {
      std::cout << "Locating the starting index: " << std::distance(array_, res) << std::endl;
      *index = std::distance(array_, res);
    }

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

  // insert <key, value>
  int index = std::distance(array_, it);
  // BUSTUB_ASSERT(GetSize() == 255, "The leaf page is full already!!!");
  std::move_backward(array_ + index, array_ + size, array_ + size + 1);

  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &comparator) -> bool {
  if (GetSize() == 0) {
    return false;
  }

  auto compare_first = [comparator](const MappingType &lhs, KeyType rhs) -> bool {
    return comparator(lhs.first, rhs) < 0;
  };

  auto res = std::lower_bound(array_, array_ + GetSize() - 1, key, compare_first);
  if (comparator(key, res->first) == 0) {
    // remove <key, value>
    int dist = std::distance(array_, res);
    std::copy(array_ + dist + 1, array_ + GetSize(), array_ + dist);
    IncreaseSize(-1);
    return true;
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(MappingType *array, int size) {
  std::copy(array, array + size, array_ + GetSize());
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ShiftData(int dist) {
  if (dist > 0) {  // 向右移动
    std::copy_backward(array_, array_ + GetSize(), array_ + GetSize() + dist);
  } else if (dist < 0) {  // 向左移动
    std::copy(array_ - dist, array_ + GetSize(), array_);
  }
  IncreaseSize(dist);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetParentPageId() -> page_id_t { return parent_page_id_; }

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t page_id) { next_page_id_ = page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *array, int min_size, int size) {
  std::copy(array + min_size, array + size, array_);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetData() -> MappingType * { return array_; }

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
