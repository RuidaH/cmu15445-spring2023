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
  // leaf_max_size_ = 255;
  // internal_max_size_ = 255;

  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return true; }
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
  // Declaration of context instance.
  // Context ctx;
  // (void)ctx;

  LOG_DEBUG("B+ Tree seach key {%s}", std::to_string(key.ToString()).c_str());

  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {  // b+ tree is empty
    return false;
  }

  ReadPageGuard guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto page = guard.As<BPlusTreePage>();

  const InternalPage *internal_page = nullptr;
  while (!page->IsLeafPage()) {
    internal_page = guard.As<InternalPage>();

    // std::cout << "#### Find the internal page (page id " << guard.PageId() << ")" << std::endl;
    // PrintPage(guard, internal_page->IsLeafPage());

    guard = bpm_->FetchPageRead(internal_page->FindValue(key, comparator_));
    page = guard.As<BPlusTreePage>();
  }

  // find the leaf page
  const auto *leaf_page = guard.As<LeafPage>();
  ValueType res;

  // LOG_DEBUG("Find the key in leaf_page %d", guard.PageId());

  if (leaf_page->FindValue(key, res, comparator_)) {
    LOG_DEBUG("Find the key %s", std::to_string(key.ToString()).c_str());
    result->push_back(res);
    return true;
  }

  // std::cout << "Fail to find the key, Final leaf node with res size " << result->size() << std::endl;
  // PrintPage(guard, leaf_page->IsLeafPage());

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
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  LOG_DEBUG("B+ Tree Insert key {%s, <%s>}", std::to_string(key.ToString()).c_str(), value.ToString().c_str());

  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;

  LOG_DEBUG("B+ Tree Insert key %s: head_page is locked", std::to_string(key.ToString()).c_str());

  // tree is empty
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    BasicPageGuard root_page_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
    ctx.root_page_id_ = root_page_guard.PageId();
    auto page = root_page_guard.AsMut<LeafPage>();
    page->Init(leaf_max_size_);
    // root_page_guard.SetDirty(true);
    root_page_guard.Drop();

    WritePageGuard root_page_write_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
    page = root_page_write_guard.AsMut<LeafPage>();  // set is_dirty_ = true;
    page->Insert(key, value, comparator_);
    // root_page_write_guard.SetDirty(true);

    LOG_DEBUG("Create a new root (internal) node %d with content %s", root_page_write_guard.PageId(),
              page->ToString().c_str());

    // LOG_DEBUG("B+ Tree Insert key %s: create a new root page", std::to_string(key.ToString()).c_str());

    // PrintPage(root_page_write_guard, page->IsLeafPage());

    return true;
  }

  // to find the leaf node
  WritePageGuard guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  auto page = guard.AsMut<BPlusTreePage>();
  // LOG_DEBUG("Current page id: %d (leaf page: %d)", guard.PageId(), page->IsLeafPage());

  LOG_DEBUG("B+ Tree Insert key %s: fetch the root page (%d) and start looking for the leaf page",
            std::to_string(key.ToString()).c_str(), header_page->root_page_id_);

  InternalPage *internal_page = nullptr;
  while (!page->IsLeafPage()) {
    // LOG_DEBUG("Current page id: %d (leaf page: %d)", guard.PageId(), page->IsLeafPage());
    internal_page = guard.AsMut<InternalPage>();

    // PrintPage(guard, false);
    ctx.write_set_.emplace_back(std::move(guard));

    page_id_t tmp = internal_page->FindValue(key, comparator_);

    LOG_DEBUG("Find the next page id: %d, with internal page content: \n %s", tmp, internal_page->ToString().c_str());

    guard = bpm_->FetchPageWrite(tmp);

    page = guard.AsMut<BPlusTreePage>();

    if (page->IsLeafPage()) {
      LOG_DEBUG("B+ Tree Insert key %s: fetch the leaf page (%d)", std::to_string(key.ToString()).c_str(),
                guard.PageId());
    } else {
      LOG_DEBUG("B+ Tree Insert key %s: fetch the internal page (%d)", std::to_string(key.ToString()).c_str(),
                guard.PageId());
    }
  }

  auto leaf_page = guard.AsMut<LeafPage>();
  // guard.SetDirty(true);
  page_id_t cur_page_id = guard.PageId();
  ctx.write_set_.emplace_back(std::move(guard));

  LOG_DEBUG("B+ Tree Insert key %s: find the leaf page (%d)", std::to_string(key.ToString()).c_str(), cur_page_id);

  // auto leaf_page = guard.AsMut<LeafPage>(); // 因为此时 guard 的所有权已经给了 write_set_, 所以直接使用 cast
  // auto leaf_page = reinterpret_cast<LeafPage*>(page);
  // ctx.write_set_.pop_back(); // 叶子节点不用加入 ctx, 因为在 InsertParent 马上就会被处理掉

  if (leaf_page->GetSize() < leaf_page->GetMaxSize() - 1) {
    bool res = leaf_page->Insert(key, value, comparator_);
    // PrintPage(ctx.write_set_.back(), true);
    return res;
  }

  // 这里的逻辑看还能不能再优化一下
  // split the leaf node after insert new <key, value>
  if (leaf_page->Insert(key, value, comparator_)) {
    int min_size = leaf_page->GetMinSize();
    int cur_size = leaf_page->GetSize();

    LOG_DEBUG("\n Leaf page efore Insertion: ");
    PrintPage(ctx.write_set_.back(), true);

    page_id_t new_page_id;
    BasicPageGuard new_page_guard = bpm_->NewPageGuarded(&new_page_id);
    auto new_page = new_page_guard.AsMut<LeafPage>();
    new_page->Init(leaf_max_size_);
    // new_page_guard.SetDirty(true);
    new_page_guard.Drop();

    // BasicPageGuard -> WritePageGuard
    WritePageGuard new_guard = bpm_->FetchPageWrite(new_page_id);
    new_page = new_guard.AsMut<LeafPage>();

    // split the current leaf page
    new_page->CopyHalfFrom(leaf_page->GetData(), min_size, cur_size);
    KeyType pushed_key = leaf_page->KeyAt(min_size);
    // leaf_page->EraseHalf();
    // 先暂时不管这里, 只要访问小于 size 的元素都应该没问题

    new_page->SetSize(cur_size - min_size);
    leaf_page->SetSize(min_size);
    new_page->SetNextPage(leaf_page->GetNextPageId());
    leaf_page->SetNextPage(new_page_id);

    LOG_DEBUG("Create a new leaf node %d with content %s", new_page_id, new_page->ToString().c_str());

    LOG_DEBUG("Split the node: current leaf page id: %d; new leaf page id: %d; pushed key: %s", cur_page_id,
              new_page_id, std::to_string(pushed_key.ToString()).c_str());

    LOG_DEBUG("\n Leaf page after Insertion: ");
    LOG_DEBUG("\n cur leaf page: min_size %d, cur_size %d, max_size %d", leaf_page->GetMinSize(), leaf_page->GetSize(),
              leaf_page->GetMaxSize());
    PrintPage(ctx.write_set_.back(), true);
    LOG_DEBUG("\n new leaf page: min_size %d, cur_size %d, max_size %d", new_page->GetMinSize(), new_page->GetSize(),
              new_page->GetMaxSize());
    PrintPage(new_guard, true);

    // LOG_DEBUG("B+ Tree Insert key %s: leaf node split, create a new sibling node and pass it to InsertInParent()",
    // std::to_string(key.ToString()).c_str());

    // insert into parent page
    InsertInParent(pushed_key, std::move(new_guard), ctx);

    // drop the header page guard when you want to unlock all
    ctx.write_set_.clear();
    ctx.header_page_ = std::nullopt;

    // std::cout << "\n++++++++++++++++++++++++" << std::endl;
    // Print(bpm_);
    // std::cout << "++++++++++++++++++++++++\n" << std::endl;

    return true;
  }

  // drop the header page guard when you want to unlock all
  ctx.write_set_.clear();
  ctx.header_page_.value().Drop();
  ctx.header_page_ = std::nullopt;

  return false;
}

/**
 * key: the key pushed to the parent node
 * cur_page: the old page
 * new_page: newly created page
 * ctx: keep track of the path
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(const KeyType &key, WritePageGuard &&new_page_guard, Context &ctx) {
  // LOG_DEBUG("Size of ctx: %zu", ctx.write_set_.size());

  // root_page is full, create a new root page
  page_id_t cur_page_id = ctx.write_set_.back().PageId();
  if (ctx.IsRootPage(cur_page_id)) {
    auto header_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
    BasicPageGuard new_root_page_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
    new_root_page_guard.Drop();
    // ctx.header_page_.value().SetDirty(true);

    // LOG_DEBUG("Root page id: %d => %d", ctx.root_page_id_, header_page->root_page_id_);

    // BasicPageGuard -> WritePageGuard
    WritePageGuard new_root_guard = bpm_->FetchPageWrite(header_page->root_page_id_);

    ctx.root_page_id_ = header_page->root_page_id_;
    auto new_root_page = new_root_guard.AsMut<InternalPage>();
    new_root_page->Init(internal_max_size_);

    // LOG_DEBUG("Page id compare: %d, %d", cur_page_id, cur_page_guard.PageId());

    // 这两个函数仅限于这里使用, 如果是多于 2 个元素, 就必须使用 insert
    new_root_page->SetValueAt(0, cur_page_id);
    // LOG_DEBUG("Root page index 1 update: key(%s), value(%d)", std::to_string(key.ToString()).c_str(),
    // new_page_guard.PageId());
    new_root_page->SetKeyValueAt(1, key, new_page_guard.PageId());
    new_root_page->IncreaseSize(2);

    LOG_DEBUG("Create a new leaf node %d with content %s", new_root_guard.PageId(), new_root_page->ToString().c_str());

    // LOG_DEBUG("Root page size: %d", new_root_page->GetSize());
    // PrintPage(new_root_guard, false);

    // PrintPage(new_page_guard, new_page_guard.As<BPlusTreePage>()->IsLeafPage());
    // PrintPage(ctx.write_set_.back(), ctx.write_set_.back().As<BPlusTreePage>()->IsLeafPage());

    ctx.write_set_.pop_back();
    return;
  }

  // 此时最后一个节点和 new_page_guard 是同一层的, 需要先弹出来, 找出父节点
  // ctx.write_set_.back().SetDirty(true);
  ctx.write_set_.pop_back();
  WritePageGuard &cur_page_guard = ctx.write_set_.back();
  auto cur_page = cur_page_guard.AsMut<InternalPage>();
  // cur_page_guard.SetDirty(true);

  // parent page is not full, just insert it and return
  if (cur_page->GetSize() < cur_page->GetMaxSize()) {
    cur_page->Insert(key, new_page_guard.PageId(), comparator_);
    ctx.write_set_.pop_back();
    return;
  }

  // the parent page is full, split before insertion
  page_id_t new_page_id;
  BasicPageGuard new_basic_page_guard = bpm_->NewPageGuarded(&new_page_id);
  auto new_page = new_basic_page_guard.AsMut<InternalPage>();
  new_page->Init(internal_max_size_);
  new_basic_page_guard.Drop();

  // BasicPageGuard -> WritePageGuard
  WritePageGuard new_parent_page_guard = bpm_->FetchPageWrite(new_page_id);
  new_page = new_parent_page_guard.AsMut<InternalPage>();

  int min_size = cur_page->GetMinSize();
  int cur_size = cur_page->GetSize();

  KeyType pushed_key = cur_page->KeyAt(min_size);

  std::cout << "#### min_size" << min_size << ", cur_size: " << cur_size << ", max_size: " << cur_page->GetMaxSize()
            << std::endl;
  std::cout << "#### Original pushed_key: " << pushed_key << std::endl;
  std::cout << "#### Cur page before insertion" << std::endl;
  PrintPage(cur_page_guard, cur_page->IsLeafPage());

  // insert the new key
  auto last_key = cur_page->KeyAt(min_size - 1);
  bool is_first_case = comparator_(key, pushed_key) > 0;
  bool is_second_case = comparator_(key, pushed_key) < 0 && comparator_(key, last_key) > 0;
  if (is_first_case || is_second_case) {
    new_page->CopyHalfFrom(cur_page->GetData(), min_size, cur_size);

    cur_page->SetSize(min_size);
    new_page->SetSize(cur_size - min_size);

    // new_page->Insert(key, new_parent_page_guard.PageId(), comparator_);
    new_page->Insert(key, new_page_guard.PageId(), comparator_);
    pushed_key = is_second_case ? key : pushed_key;

  } else {
    // 因为新的 key 是被插入 cur_page, 所以 new_page 会占多一位
    new_page->CopyHalfFrom(cur_page->GetData(), min_size - 1, cur_size);

    cur_page->SetSize(min_size - 1);
    new_page->SetSize(cur_size - min_size + 1);

    // cur_page->Insert(key, new_parent_page_guard.PageId(), comparator_); 又是一个愚蠢的错误
    cur_page->Insert(key, new_page_guard.PageId(), comparator_);

    // pushed_key 也要更新
    pushed_key = last_key;
  }

  std::cout << "#### pushed key: " << pushed_key << std::endl;
  std::cout << "#### Cur page after insertion" << std::endl;
  PrintPage(cur_page_guard, cur_page->IsLeafPage());

  std::cout << "#### New page after insertion" << std::endl;
  PrintPage(new_parent_page_guard, new_page->IsLeafPage());

  LOG_DEBUG("Create a new internal node %d with content %s", new_parent_page_guard.PageId(),
            new_page->ToString().c_str());
  // new_basic_page_guard.SetDirty(true);

  InsertInParent(pushed_key, std::move(new_parent_page_guard), ctx);
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
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
