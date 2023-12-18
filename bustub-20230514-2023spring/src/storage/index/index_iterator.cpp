/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t page_id, int index,
                                  MappingType &entry)
    : bpm_(buffer_pool_manager), cur_page_id_(page_id), index_(index) {
  entry_.first = entry.first;
  entry_.second = entry.second;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t page_id, int index)
    : bpm_(buffer_pool_manager), cur_page_id_(page_id), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  ReadPageGuard cur_guard = bpm_->FetchPageRead(cur_page_id_);
  auto cur_page = cur_guard.As<LeafPage>();
  return cur_page->GetNextPageId() == INVALID_PAGE_ID && index_ == cur_page->GetSize() - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return entry_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  ReadPageGuard cur_guard = bpm_->FetchPageRead(cur_page_id_);
  auto cur_page = cur_guard.As<LeafPage>();

  // 当前 iterator 已经是 end 了, 直接返回
  if (cur_page_id_ == INVALID_PAGE_ID) {
    return *this;
  }

  // 判断下一个 iterator 是不是 end
  if (IsEnd()) {
    cur_page_id_ = INVALID_PAGE_ID;
    index_ = -1;
    return *this;
  }

  // 下一个 iterator 在当前页节点中
  if (index_ < cur_page->GetSize() - 1) {
    ++index_;
    entry_.first = cur_page->KeyAt(index_);
    entry_.second = cur_page->ValueAt(index_);
    return *this;
  }

  // 下一个 iterator 在下一个页节点中
  page_id_t next_page_id = cur_page->GetNextPageId();
  ReadPageGuard next_guard = bpm_->FetchPageRead(next_page_id);
  auto next_page = next_guard.As<LeafPage>();

  index_ = 0;
  entry_.first = next_page->KeyAt(index_);
  entry_.second = next_page->ValueAt(index_);
  cur_page_id_ = next_page_id;

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return cur_page_id_ == itr.cur_page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
