#include <sstream>
#include <string>

#include <cmath>
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  // LOG_DEBUG("BPlusTree() | internal_max_size: %d; leaf_max_size: %d", internal_max_size_, leaf_max_size_);

  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard header_page_gaurd = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_gaurd.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(Context &ctx, const KeyType &key, OperationType op_type, bool optimistic,
                                  Transaction *txn, std::unordered_map<page_id_t, int> *page_id_to_index) -> bool {
  if (optimistic) {
    ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
    auto header_page = header_guard.As<BPlusTreeHeaderPage>();
    ctx.root_page_id_ = header_page->root_page_id_;

    // b+ tree is empty
    if (header_page->root_page_id_ == INVALID_PAGE_ID) {
      return false;
    }

    ReadPageGuard guard = bpm_->FetchPageRead(ctx.root_page_id_);
    auto page = guard.As<BPlusTreePage>();
    const InternalPage *internal_page = nullptr;
    page_id_t tmp_page_id = ctx.root_page_id_;

    while (!page->IsLeafPage()) {
      internal_page = guard.As<InternalPage>();
      tmp_page_id = internal_page->FindValue(key, comparator_);
      guard = bpm_->FetchPageRead(tmp_page_id);
      page = guard.As<BPlusTreePage>();
    }

    if (op_type == OperationType::FIND) {
      ctx.read_set_.emplace_back(std::move(guard));
    } else {
      guard.Drop();
      ctx.write_set_.emplace_back(bpm_->FetchPageWrite(tmp_page_id));
    }

    return true;
  }

  // pessimistic: latch crabbing
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;

  // b+ tree is empty
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    if (op_type == OperationType::DELETE) {
      ctx.header_page_ = std::nullopt;
      return false;
    }
    NewLeafRootPage(ctx, &header_page->root_page_id_);
  }

  ctx.write_set_.emplace_back(std::move(ctx.header_page_.value()));
  ctx.header_page_ = std::nullopt;

  WritePageGuard guard = bpm_->FetchPageWrite(ctx.root_page_id_);
  auto page = guard.AsMut<BPlusTreePage>();
  InternalPage *internal_page = nullptr;
  while (!page->IsLeafPage()) {
    internal_page = guard.AsMut<InternalPage>();

    if (internal_page->IsSafe(op_type)) {
      ctx.write_set_.clear();
    }
    ctx.write_set_.emplace_back(std::move(guard));

    page_id_t child_page_id = -1;
    if (op_type == OperationType::DELETE) {
      int child_page_index = -1;
      child_page_id = internal_page->FindValue(key, comparator_, &child_page_index);
      (*page_id_to_index)[child_page_id] = child_page_index;
    } else {
      child_page_id = internal_page->FindValue(key, comparator_);
    }

    guard = bpm_->FetchPageWrite(child_page_id);
    page = guard.AsMut<BPlusTreePage>();
  }

  // add the leaf page guard into the write_set_
  ctx.write_set_.emplace_back(std::move(guard));

  return true;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  Context ctx;
  (void)ctx;

  // LOG_DEBUG("Find | key %s", std::to_string(key.ToString()).c_str());

  // tree is empty
  if (!FindLeafPage(ctx, key, OperationType::FIND, true, txn)) {
    return false;
  }
  ReadPageGuard guard = std::move(ctx.read_set_.back());

  ValueType leaf_value;
  const auto *leaf_page = guard.As<LeafPage>();
  if (leaf_page->FindValue(key, leaf_value, comparator_)) {
    result->push_back(leaf_value);
    return true;
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  Context ctx;
  (void)ctx;

  // LOG_DEBUG("Insert | key %s", std::to_string(key.ToString()).c_str());

  bool optimistic = true;
  if (FindLeafPage(ctx, key, OperationType::INSERT, optimistic, txn)) {
    WritePageGuard guard = std::move(ctx.write_set_.back());
    auto leaf_page = guard.AsMut<LeafPage>();
    if (leaf_page->IsSafe(OperationType::INSERT)) {
      return leaf_page->Insert(key, value, comparator_);
    }
    ctx.write_set_.clear();
  }

  // ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  // auto header_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  // ctx.root_page_id_ = header_page->root_page_id_;

  // // tree is empty
  // if (ctx.root_page_id_ == INVALID_PAGE_ID) {
  //   BasicPageGuard root_page_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
  //   auto page = root_page_guard.AsMut<LeafPage>();
  //   page->Init(INVALID_PAGE_ID, leaf_max_size_);
  //   root_page_guard.Drop();

  //   WritePageGuard root_page_write_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  //   page = root_page_write_guard.AsMut<LeafPage>();
  //   page->Insert(key, value, comparator_);

  //   ctx.header_page_ = std::nullopt;
  //   return true;
  // }

  // // insert the header_page_ into the write_set_ for latch crabbing
  // ctx.write_set_.emplace_back(std::move(ctx.header_page_.value()));
  // ctx.header_page_ = std::nullopt;

  optimistic = false;
  FindLeafPage(ctx, key, OperationType::INSERT, optimistic, txn);

  // leaf page is safe, just insert and return
  auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  if (leaf_page->IsSafe(OperationType::INSERT)) {
    bool res = leaf_page->Insert(key, value, comparator_);
    ctx.write_set_.clear();
    return res;
  }

  // if (leaf_page->GetSize() < leaf_page->GetMaxSize() - 1) {
  //   bool res = leaf_page->Insert(key, value, comparator_);
  //   ctx.write_set_.clear();
  //   return res;
  // }

  // leaf_page is not safe (one step towards the full page)
  if (leaf_page->Insert(key, value, comparator_)) {
    // int min_size = leaf_page->GetMinSize();
    // int cur_size = leaf_page->GetSize();

    // page_id_t new_page_id;
    // BasicPageGuard new_page_guard = bpm_->NewPageGuarded(&new_page_id);
    // auto new_page = new_page_guard.AsMut<LeafPage>();
    // new_page->Init(parent_page_id, leaf_max_size_);
    // new_page_guard.Drop();

    // WritePageGuard new_guard = bpm_->FetchPageWrite(new_page_id);
    // new_page = new_guard.AsMut<LeafPage>();

    page_id_t new_page_id;
    LeafPage *new_page = NewLeafPage(ctx, &new_page_id, leaf_page->GetParentPageId());

    // split the current leaf page
    KeyType pushed_key = Split(leaf_page, new_page);

    // new_page->CopyHalfFrom(leaf_page->GetData(), min_size, cur_size);
    // KeyType pushed_key = leaf_page->KeyAt(min_size);
    // new_page->SetSize(cur_size - min_size);
    // leaf_page->SetSize(min_size);

    new_page->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(new_page_id);

    WritePageGuard new_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    InsertInParent(pushed_key, std::move(new_guard), ctx);
  }

  // find duplicate key
  ctx.write_set_.clear();
  return false;

  // // return if duplicate key is found
  // ValueType temp_value;
  // if (leaf_page->FindValue(key, temp_value, comparator_)) {
  //   ctx.write_set_.clear();
  //   return false;
  // }

  // // leaf page is full, split before insertion
  // page_id_t new_page_id;
  // BasicPageGuard new_page_guard = bpm_->NewPageGuarded(&new_page_id);
  // auto new_page = new_page_guard.AsMut<LeafPage>();
  // new_page->Init(leaf_page->GetParentPageId(), leaf_max_size_);
  // new_page_guard.Drop();

  // WritePageGuard new_guard = bpm_->FetchPageWrite(new_page_id);
  // new_page = new_guard.AsMut<LeafPage>();

  // int min_size = leaf_page->GetMinSize();
  // int cur_size = leaf_page->GetSize();

  // // take care of the corner case of max_size_ = 2
  // KeyType min_idx_key = (min_size == cur_size) ? leaf_page->KeyAt(0) : leaf_page->KeyAt(min_size);
  // if (comparator_(key, min_idx_key) == 0) {
  //   return false;
  // }

  // bool res = true;
  // if (comparator_(key, min_idx_key) > 0 ||
  //     (comparator_(key, min_idx_key) < 0 && comparator_(key, leaf_page->KeyAt(min_size - 1)) > 0)) {
  //   // insert key into right page after split
  //   new_page->CopyHalfFrom(leaf_page->GetData(), min_size, cur_size);
  //   leaf_page->SetSize(min_size);
  //   new_page->SetSize(cur_size - min_size);
  //   res = new_page->Insert(key, value, comparator_);
  // } else {
  //   // insert key into left page after split
  //   new_page->CopyHalfFrom(leaf_page->GetData(), min_size - 1, cur_size);
  //   leaf_page->SetSize(min_size - 1);
  //   new_page->SetSize(cur_size - min_size + 1);
  //   res = leaf_page->Insert(key, value, comparator_);
  // }

  // new_page->SetNextPageId(leaf_page->GetNextPageId());
  // leaf_page->SetNextPageId(new_page_id);
  // InsertInParent(new_page->KeyAt(0), std::move(new_guard), ctx);

  // return res;
}

/**
 * key: the key pushed to the parent node
 * cur_page: the old page
 * new_page: newly created page
 * ctx: keep track of the path
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(const KeyType &key, WritePageGuard &&new_page_guard, Context &ctx) {
  // root_page is full, create a new root page
  page_id_t cur_page_id = ctx.write_set_.back().PageId();
  if (ctx.IsRootPage(cur_page_id)) {
    WritePageGuard &header_page_guard = ctx.write_set_.front();
    auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();

    BasicPageGuard new_root_page_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
    ctx.root_page_id_ = header_page->root_page_id_;
    new_root_page_guard.Drop();

    WritePageGuard new_root_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
    auto new_root_page = new_root_guard.AsMut<InternalPage>();
    new_root_page->Init(INVALID_PAGE_ID, internal_max_size_);

    UpdateParentPage(ctx, key, new_root_page, std::move(new_page_guard), cur_page_id, header_page->root_page_id_);
    ctx.write_set_.clear();

    return;
  }

  // pop the current page first, then find the parent page
  ctx.write_set_.pop_back();
  WritePageGuard &cur_page_guard = ctx.write_set_.back();
  auto cur_page = cur_page_guard.AsMut<InternalPage>();

  // parent page is not full, just insert it and return
  if (cur_page->GetSize() < cur_page->GetMaxSize()) {
    cur_page->Insert(key, new_page_guard.PageId(), comparator_);
    ctx.write_set_.clear();
    return;
  }

  // the parent page is full, split before insertion
  page_id_t new_page_id;
  BasicPageGuard new_basic_page_guard = bpm_->NewPageGuarded(&new_page_id);
  auto new_page = new_basic_page_guard.AsMut<InternalPage>();
  new_page->Init(cur_page->GetParentPageId(), internal_max_size_);
  new_basic_page_guard.Drop();

  WritePageGuard new_parent_page_guard = bpm_->FetchPageWrite(new_page_id);
  new_page = new_parent_page_guard.AsMut<InternalPage>();

  int min_size = cur_page->GetMinSize();
  int cur_size = cur_page->GetSize();

  KeyType pushed_key = cur_page->KeyAt(min_size);

  auto last_key = cur_page->KeyAt(min_size - 1);
  bool is_first_case = comparator_(key, pushed_key) > 0;
  bool is_second_case = comparator_(key, pushed_key) < 0 && comparator_(key, last_key) > 0;
  if (is_first_case || is_second_case) {  // insert the key into the new page
    new_page->CopyHalfFrom(cur_page->GetData(), min_size, cur_size);
    cur_page->SetSize(min_size);
    new_page->SetSize(cur_size - min_size);

    new_page->Insert(key, new_page_guard.PageId(), comparator_);
    pushed_key = is_second_case ? key : pushed_key;

  } else {  // insert the key into the current page
    new_page->CopyHalfFrom(cur_page->GetData(), min_size - 1, cur_size);

    cur_page->SetSize(min_size - 1);
    new_page->SetSize(cur_size - min_size + 1);

    cur_page->Insert(key, new_page_guard.PageId(), comparator_);
    pushed_key = last_key;
  }

  InsertInParent(pushed_key, std::move(new_parent_page_guard), ctx);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateParentPage(Context &ctx, const KeyType &key, InternalPage *new_root_page,
                                      WritePageGuard &&new_page_guard, page_id_t cur_page_id, page_id_t root_page_id) {
  // update the parent_page_id for the new root page
  bool is_leaf_page = ctx.write_set_.back().AsMut<BPlusTreePage>()->IsLeafPage();
  if (is_leaf_page) {
    auto first_child_page = ctx.write_set_.back().AsMut<LeafPage>();
    auto second_child_page = new_page_guard.AsMut<LeafPage>();

    first_child_page->SetParentPageId(root_page_id);
    second_child_page->SetParentPageId(root_page_id);
  } else {
    auto first_child_page = ctx.write_set_.back().AsMut<InternalPage>();
    auto second_child_page = new_page_guard.AsMut<InternalPage>();

    first_child_page->SetParentPageId(root_page_id);
    second_child_page->SetParentPageId(root_page_id);
  }

  new_root_page->SetValueAt(0, cur_page_id);
  new_root_page->SetKeyValueAt(1, key, new_page_guard.PageId());
  new_root_page->IncreaseSize(2);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::NewLeafRootPage(Context &ctx, page_id_t *root_page_id) {
  BasicPageGuard root_page_guard = bpm_->NewPageGuarded(root_page_id);
  ctx.root_page_id_ = *root_page_id;
  auto page = root_page_guard.AsMut<LeafPage>();
  page->Init(INVALID_PAGE_ID, leaf_max_size_);
  root_page_guard.Drop();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewLeafPage(Context &ctx, page_id_t *new_page_id, page_id_t parent_page_id) ->
    typename BPLUSTREE_TYPE::LeafPage * {
  BasicPageGuard new_page_guard = bpm_->NewPageGuarded(new_page_id);
  auto new_page = new_page_guard.AsMut<LeafPage>();
  new_page->Init(parent_page_id, leaf_max_size_);
  new_page_guard.Drop();

  WritePageGuard new_guard = bpm_->FetchPageWrite(*new_page_id);
  ctx.write_set_.emplace_back(std::move(new_guard));
  return ctx.write_set_.back().AsMut<LeafPage>();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(LeafPage *leaf_page, LeafPage *new_page) -> KeyType {
  int min_size = leaf_page->GetMinSize();
  int cur_size = leaf_page->GetSize();
  new_page->CopyHalfFrom(leaf_page->GetData(), min_size, cur_size);
  KeyType pushed_key = leaf_page->KeyAt(min_size);
  new_page->SetSize(cur_size - min_size);
  leaf_page->SetSize(min_size);

  return pushed_key;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  Context ctx;
  (void)ctx;

  // LOG_DEBUG("Remove | key %s", std::to_string(key.ToString()).c_str());

  bool optimistic = true;
  if (FindLeafPage(ctx, key, OperationType::DELETE, optimistic, txn)) {
    WritePageGuard guard = std::move(ctx.write_set_.back());
    auto leaf_page = guard.AsMut<LeafPage>();
    if (leaf_page->IsSafe(OperationType::DELETE)) {
      leaf_page->Delete(key, comparator_);
      return;
    }
    ctx.write_set_.clear();
  }

  // 此时树可能是空的, 亦或是叶子节点不安全

  // ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  // auto header_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  // ctx.root_page_id_ = header_page->root_page_id_;

  // // b+ tree is empty
  // if (header_page->root_page_id_ == INVALID_PAGE_ID) {
  //   return;
  // }

  // ctx.write_set_.emplace_back(std::move(ctx.header_page_.value()));
  // ctx.header_page_ = std::nullopt;

  optimistic = false;
  std::unordered_map<page_id_t, int> page_id_to_index;
  bool find = FindLeafPage(ctx, key, OperationType::DELETE, optimistic, txn, &page_id_to_index);
  if (!find) {  // b+ tree is empty
    return;
  }

  RemoveLeafEntry(ctx, key, &page_id_to_index);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveLeafEntry(Context &ctx, KeyType key, std::unordered_map<page_id_t, int> *page_id_to_index) {
  WritePageGuard cur_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();

  page_id_t cur_leaf_page_id = cur_guard.PageId();
  auto cur_leaf_page = cur_guard.AsMut<LeafPage>();

  // fail to delete (e.g. cannot find the corresponding entry)
  if (!cur_leaf_page->Delete(key, comparator_)) {
    return;
  }

  // leaf page is the root page and it's empty, update the root_page_id_
  if (cur_leaf_page_id == ctx.root_page_id_ && cur_leaf_page->GetSize() == 0) {
    WritePageGuard &header_page_guard = ctx.write_set_.front();
    auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = INVALID_PAGE_ID;
    ctx.root_page_id_ = INVALID_PAGE_ID;
    ctx.write_set_.clear();
    return;
  }

  // leaf page is root page and the size > 0
  if (cur_leaf_page_id == ctx.root_page_id_) {
    ctx.write_set_.clear();
    return;
  }

  // leaf page is not the root page, but it has enough entries (i.e. safe)
  if (cur_leaf_page->GetSize() >= cur_leaf_page->GetMinSize()) {
    ctx.write_set_.clear();
    return;
  }

  // acquire parent page guard
  page_id_t index_in_parent_page = (*page_id_to_index)[cur_leaf_page_id];
  WritePageGuard &parent_guard = ctx.write_set_.back();
  auto parent_page = parent_guard.AsMut<InternalPage>();

  // acquire sibling page guard
  page_id_t sibling_page_id = -1;
  bool is_last_entry = index_in_parent_page == parent_page->GetSize() - 1;
  if (is_last_entry) {  // leaf page is at the right-most position, find the left sibling
    sibling_page_id = parent_page->ValueAt(index_in_parent_page - 1);
  } else {  // normal situation: find the right sibling
    sibling_page_id = parent_page->ValueAt(index_in_parent_page + 1);
  }
  WritePageGuard sibling_page_guard = bpm_->FetchPageWrite(sibling_page_id);

  LeafPage *left_page = nullptr;
  LeafPage *right_page = nullptr;
  KeyType up_key;
  page_id_t up_value;
  bool redistribute_toward_right = true;

  if (is_last_entry) {
    left_page = sibling_page_guard.AsMut<LeafPage>();
    right_page = cur_leaf_page;
    up_key = parent_page->KeyAt(index_in_parent_page);
    up_value = cur_leaf_page_id;
  } else {
    left_page = cur_leaf_page;
    right_page = sibling_page_guard.AsMut<LeafPage>();
    up_key = parent_page->KeyAt(index_in_parent_page + 1);
    up_value = sibling_page_guard.PageId();
    redistribute_toward_right = false;
  }

  // right page merge into left page
  int left_page_cur_size = left_page->GetSize();
  int right_page_cur_size = right_page->GetSize();
  if (left_page_cur_size + right_page_cur_size < left_page->GetMaxSize()) {
    left_page->Merge(right_page->GetData(), right_page->GetSize());
    left_page->SetNextPageId(right_page->GetNextPageId());
    RemoveInternalEntry(ctx, up_key, up_value, page_id_to_index);
    return;
  }

  // redistribute
  if (redistribute_toward_right) {  // left page => right page
    right_page->ShiftData(1);
    right_page->SetKeyValueAt(0, left_page->KeyAt(left_page_cur_size - 1), left_page->ValueAt(left_page_cur_size - 1));
    left_page->IncreaseSize(-1);
    parent_page->SetKeyAt(index_in_parent_page, right_page->KeyAt(0));
  } else {  // right page => left page
    left_page->IncreaseSize(1);
    left_page->SetKeyValueAt(left_page_cur_size, right_page->KeyAt(0), right_page->ValueAt(0));
    right_page->ShiftData(-1);
    parent_page->SetKeyAt(index_in_parent_page + 1, right_page->KeyAt(0));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveInternalEntry(Context &ctx, KeyType key, page_id_t val,
                                         std::unordered_map<page_id_t, int> *page_id_to_index) {
  WritePageGuard cur_internal_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();

  auto cur_internal_page = cur_internal_guard.AsMut<InternalPage>();
  page_id_t cur_internal_page_id = cur_internal_guard.PageId();

  if (!cur_internal_page->Delete(key, comparator_)) {
    return;
  }

  // make the only child in the current root node as the new root node
  if (cur_internal_page_id == ctx.root_page_id_ && cur_internal_page->GetSize() == 1) {
    WritePageGuard &header_page_guard = ctx.write_set_.front();
    auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = cur_internal_page->ValueAt(0);
    ctx.root_page_id_ = header_page->root_page_id_;
    ctx.write_set_.clear();
    return;
  }

  // return when the root page size > 1
  if (cur_internal_page_id == ctx.root_page_id_) {
    return;
  }

  // return when current internal page has enough entry
  if (cur_internal_page->GetSize() >= cur_internal_page->GetMinSize()) {
    ctx.write_set_.clear();
    return;
  }

  page_id_t index_in_parent_page = (*page_id_to_index)[cur_internal_page_id];
  WritePageGuard &parent_guard = ctx.write_set_.back();
  auto parent_page = parent_guard.AsMut<InternalPage>();

  // acquire sibling page guard
  page_id_t sibling_page_id = -1;
  bool is_last_entry = index_in_parent_page == parent_page->GetSize() - 1;
  if (is_last_entry) {
    sibling_page_id = parent_page->ValueAt(index_in_parent_page - 1);
  } else {
    sibling_page_id = parent_page->ValueAt(index_in_parent_page + 1);
  }
  WritePageGuard sibling_page_guard = bpm_->FetchPageWrite(sibling_page_id);

  InternalPage *left_page = nullptr;
  InternalPage *right_page = nullptr;
  KeyType up_key;
  page_id_t up_value;
  bool redistribute_toward_right = true;

  if (is_last_entry) {
    left_page = sibling_page_guard.AsMut<InternalPage>();
    right_page = cur_internal_page;
    up_key = parent_page->KeyAt(index_in_parent_page);
    up_value = cur_internal_page_id;
  } else {
    left_page = cur_internal_page;
    right_page = sibling_page_guard.AsMut<InternalPage>();
    up_key = parent_page->KeyAt(index_in_parent_page + 1);
    up_value = sibling_page_guard.PageId();
    redistribute_toward_right = false;
  }

  // merge right page into left page
  int left_page_cur_size = left_page->GetSize();
  int right_page_cur_size = right_page->GetSize();
  if (left_page_cur_size + right_page_cur_size <= left_page->GetMaxSize()) {
    right_page->SetKeyAt(0, up_key);
    left_page->Merge(right_page->GetData(), right_page->GetSize());
    RemoveInternalEntry(ctx, up_key, up_value, page_id_to_index);
    return;
  }

  // redistribute
  right_page->SetKeyAt(0, up_key);
  if (redistribute_toward_right) {  // left page => right page
    right_page->ShiftData(1);
    right_page->SetKeyValueAt(0, left_page->KeyAt(left_page_cur_size - 1), left_page->ValueAt(left_page_cur_size - 1));
    left_page->IncreaseSize(-1);
    parent_page->SetKeyAt(index_in_parent_page, right_page->KeyAt(0));
  } else {  // right page => left page
    left_page->IncreaseSize(1);
    left_page->SetKeyValueAt(left_page_cur_size, right_page->KeyAt(0), right_page->ValueAt(0));
    right_page->ShiftData(-1);
    parent_page->SetKeyAt(index_in_parent_page + 1, right_page->KeyAt(0));
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // LOG_DEBUG("Begin | calling iter.begin()");

  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    throw std::runtime_error("B+ tree is empty");
  }

  ReadPageGuard guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto page = guard.As<BPlusTreePage>();
  const InternalPage *internal_page = nullptr;
  while (!page->IsLeafPage()) {
    internal_page = guard.As<InternalPage>();
    guard = bpm_->FetchPageRead(internal_page->ValueAt(0));
    page = guard.As<BPlusTreePage>();
  }

  const auto *leaf_page = guard.As<LeafPage>();
  MappingType entry = MappingType(leaf_page->KeyAt(0), leaf_page->ValueAt(0));

  return INDEXITERATOR_TYPE(bpm_, guard.PageId(), 0, entry);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // LOG_DEBUG("Begin | calling iter.begin(%s)", std::to_string(key.ToString()).c_str());

  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    throw std::runtime_error("B+ tree is empty");
  }

  ReadPageGuard guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto page = guard.As<BPlusTreePage>();
  const InternalPage *internal_page = nullptr;
  while (!page->IsLeafPage()) {
    internal_page = guard.As<InternalPage>();
    guard = bpm_->FetchPageRead(internal_page->FindValue(key, comparator_));
    page = guard.As<BPlusTreePage>();
  }

  const auto *leaf_page = guard.As<LeafPage>();
  ValueType res;
  int index = -1;
  if (leaf_page->FindValue(key, res, comparator_, &index)) {
    MappingType entry = MappingType(key, res);
    return INDEXITERATOR_TYPE(bpm_, guard.PageId(), index, entry);
  }

  // fail to find the key in the leaf page
  return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  // LOG_DEBUG("End | calling iter.end()");
  return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Leaf Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << "(" << internal->GetSize() << ")" << std::endl;

    // Print the contents of the internal page.
    std::cout << "Internal Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintPage(WritePageGuard &guard, bool is_leaf_page) {
  if (is_leaf_page) {
    auto temp_page = guard.AsMut<LeafPage>();
    std::cout << "Leaf Contents (page_id " << guard.PageId() << "): " << std::endl;
    for (int i = 0; i < temp_page->GetSize(); i++) {
      std::cout << "index " << i << ": {" << temp_page->KeyAt(i).ToString() << "}";
      if ((i + 1) < temp_page->GetSize()) {
        std::cout << ", ";
      }
    }
  } else {
    auto temp_page = guard.AsMut<InternalPage>();
    std::cout << "Internal Contents (page_id " << guard.PageId() << "): " << std::endl;
    for (int i = 0; i < temp_page->GetSize(); i++) {
      std::cout << "index " << i << ": {" << temp_page->KeyAt(i).ToString() << ": " << temp_page->ValueAt(i) << "}";
      if ((i + 1) < temp_page->GetSize()) {
        std::cout << ", ";
      }
    }
  }
  std::cout << std::endl;
  std::cout << std::endl;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintPage(ReadPageGuard &guard, bool is_leaf_page) {
  if (is_leaf_page) {
    auto temp_page = guard.As<LeafPage>();
    std::cout << "Leaf Contents (page_id " << guard.PageId() << "): " << std::endl;
    for (int i = 0; i < temp_page->GetSize(); i++) {
      std::cout << "index " << i << ": {" << temp_page->KeyAt(i).ToString() << "}";
      if ((i + 1) < temp_page->GetSize()) {
        std::cout << ", ";
      }
    }
  } else {
    auto temp_page = guard.As<InternalPage>();
    std::cout << "Internal Contents (page_id " << guard.PageId() << "): " << std::endl;
    for (int i = 0; i < temp_page->GetSize(); i++) {
      std::cout << "index " << i << ": {" << temp_page->KeyAt(i).ToString() << ": " << temp_page->ValueAt(i) << "}";
      if ((i + 1) < temp_page->GetSize()) {
        std::cout << ", ";
      }
    }
  }
  std::cout << std::endl;
  std::cout << std::endl;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
