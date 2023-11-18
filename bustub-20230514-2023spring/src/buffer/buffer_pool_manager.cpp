//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool

  LOG_DEBUG("BufferPoolManager: page_ size: %zu", pool_size_);

  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  // if all frames are in use and not evictable
  if (replacer_->Size() == 0 && free_list_.empty()) {
    return nullptr;
  }

  frame_id_t free_frame_id;
  if (FindFreeFrame(&free_frame_id)) {
    /**
     * Reuse the page object in pages_ array
     * You cannot create a new Page Object and replace the Page object in the array
     *  because the assignment operator overloading function is disabled
     */
    page_id_t new_page_id = AllocatePage();
    pages_[free_frame_id].page_id_ = new_page_id;
    pages_[free_frame_id].is_dirty_ = false;
    pages_[free_frame_id].pin_count_ = 1;
    pages_[free_frame_id].ResetMemory();
    *page_id = new_page_id;

    page_table_[new_page_id] = free_frame_id;

    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);

    LOG_DEBUG("New Page: allocate page_id %d in frame_id %d", *page_id, free_frame_id);

    return &pages_[free_frame_id];
  }

  return nullptr;
}

// find a free frame from free_list_ or replacer_
auto BufferPoolManager::FindFreeFrame(frame_id_t *free_frame_id) -> bool {
  if (!free_list_.empty()) {  // pick a free frame from free_list_
    *free_frame_id = free_list_.front();
    free_list_.pop_front();

    LOG_DEBUG("FindFreeFrame: Find the free frame id %d in free list", *free_frame_id);

    return true;
  }
  // LOG_DEBUG("FindFreeFrame: replacer before the evict:");
  // replacer_->PrintLists();
  if (replacer_->Evict(free_frame_id)) {  // has evictable frames in replacer_
    // Physical page that will be written back to the disk
    Page *evicted_page = &pages_[*free_frame_id];
    page_table_.erase(evicted_page->page_id_);

    if (evicted_page->IsDirty()) {  // write back the dirty page
      disk_manager_->WritePage(evicted_page->page_id_, evicted_page->GetData());
    }

    LOG_DEBUG("FindFreeFrame: find the evicted free frame id %d", *free_frame_id);
    // replacer_->PrintLists();

    return true;
  }
  return false;
}

// fetch the page from the disk
auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  LOG_DEBUG("FetchPage: page_id %d", page_id);

  // find the page_id in the buffer pool
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);  // 有可能 pin_count == 0 但是还没有被 evict

    LOG_DEBUG("FetchPage: Find the page id %d in frame id %d", page_id, frame_id);

    return &pages_[frame_id];
  }

  if (replacer_->Size() == 0 && free_list_.empty()) {
    LOG_DEBUG("FetchPage: Replacer size: %zu; Free list size: %zu", replacer_->Size(), free_list_.size());
    LOG_DEBUG("FetchPage: No available free frame for page id: %d", page_id);
    return nullptr;
  }

  frame_id_t free_frame_id;
  if (FindFreeFrame(&free_frame_id)) {
    // read the page from the disk
    pages_[free_frame_id].page_id_ = page_id;
    pages_[free_frame_id].ResetMemory();
    disk_manager_->ReadPage(page_id, pages_[free_frame_id].data_);
    pages_[free_frame_id].is_dirty_ = false;
    pages_[free_frame_id].pin_count_ = 1;

    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);

    page_table_[page_id] = free_frame_id;

    LOG_DEBUG("FetchPage: Allocate the page id %d in frame id %d", page_id, free_frame_id);

    return &pages_[free_frame_id];
  }

  LOG_DEBUG("FetchPage: Fail to find a free frame for page id: %d", page_id);

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    LOG_DEBUG("UnpinPage: couldn't find the page id %d in page_tables", page_id);
    return false;
  }

  auto frame_id = page_table_[page_id];
  auto temp_count = pages_[frame_id].pin_count_;
  if (pages_[frame_id].pin_count_ == 0) {
    LOG_DEBUG("UnpinPage: pin_coount of page_id %d is 0", page_id);
    return false;
  }

  pages_[frame_id].pin_count_--;
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  LOG_DEBUG("Upin Page: unpin page id %d with frame id %d, is_dirty %d => %d, pin_count %d => %d", page_id, frame_id,
            pages_[frame_id].is_dirty_, is_dirty, temp_count, pages_[frame_id].GetPinCount());

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  auto frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;

  // page_table_.erase(page_id);

  LOG_DEBUG("Flush Page: flush page id %d with frame id %d", page_id, frame_id);

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  for (const auto &kv : page_table_) {
    // kv: <page_id, frame_id>
    disk_manager_->WritePage(kv.first, pages_[kv.second].data_);
    pages_[kv.second].is_dirty_ = false;
  }
  page_table_.clear();
  LOG_DEBUG("Flush All Pages");
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }

  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);

  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;

  DeallocatePage(page_id);

  LOG_DEBUG("Delete Page: delete page id %d with frame id %d", page_id, frame_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  std::lock_guard<std::mutex> lock(latch_);
  LOG_DEBUG("FetchPageBasic: page_id %d", page_id);

  // find the page_id in the buffer pool
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    LOG_DEBUG("FetchPageBasic: Find the page id %d in frame id %d", page_id, frame_id);

    PrintFrames();

    return {this, &pages_[frame_id]};
  }

  if (replacer_->Size() == 0 && free_list_.empty()) {
    LOG_DEBUG("FetchPageBasic: Replacer size: %zu; Free list size: %zu", replacer_->Size(), free_list_.size());
    LOG_DEBUG("FetchPageBasic: No available free frame for page id: %d", page_id);
    return {this, nullptr};
  }

  frame_id_t free_frame_id;
  if (FindFreeFrame(&free_frame_id)) {
    // read the page from the disk
    pages_[free_frame_id].page_id_ = page_id;
    pages_[free_frame_id].ResetMemory();
    disk_manager_->ReadPage(page_id, pages_[free_frame_id].data_);
    pages_[free_frame_id].is_dirty_ = false;
    pages_[free_frame_id].pin_count_ = 1;

    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);

    page_table_[page_id] = free_frame_id;

    LOG_DEBUG("FetchPageBasic: Allocate the page id %d in frame id %d", page_id, free_frame_id);
    PrintFrames();

    return {this, &pages_[free_frame_id]};
  }

  LOG_DEBUG("FetchPageBasic: Fail to find a free frame for page id: %d", page_id);

  return {this, nullptr};
}

// fetch the page and put the read latch on the page
auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  std::unique_lock<std::mutex> lock(latch_);
  LOG_DEBUG("FetchPageRead: page_id %d", page_id);

  // find the page_id in the buffer pool
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    LOG_DEBUG("FetchPageRead: Find the page id %d in frame id %d", page_id, frame_id);
    PrintFrames();

    lock.unlock();
    pages_[frame_id].RLatch();

    return {this, &pages_[frame_id]};
  }

  if (replacer_->Size() == 0 && free_list_.empty()) {
    LOG_DEBUG("FetchPageRead: Replacer size: %zu; Free list size: %zu", replacer_->Size(), free_list_.size());
    LOG_DEBUG("FetchPageRead: No available free frame for page id: %d", page_id);
    return {this, nullptr};
  }

  frame_id_t free_frame_id;
  if (FindFreeFrame(&free_frame_id)) {
    // read the page from the disk
    pages_[free_frame_id].page_id_ = page_id;
    pages_[free_frame_id].ResetMemory();
    disk_manager_->ReadPage(page_id, pages_[free_frame_id].data_);
    pages_[free_frame_id].is_dirty_ = false;
    pages_[free_frame_id].pin_count_ = 1;

    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);

    page_table_[page_id] = free_frame_id;

    LOG_DEBUG("FetchPageRead: Allocate the page id %d in frame id %d", page_id, free_frame_id);
    PrintFrames();

    lock.unlock();
    pages_[free_frame_id].RLatch();

    return {this, &pages_[free_frame_id]};
  }

  LOG_DEBUG("FetchPageRead: Fail to find a free frame for page id: %d", page_id);

  return {this, nullptr};
}

// fetch the page and put the write latch on the page
auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  std::unique_lock<std::mutex> lock(latch_);
  LOG_DEBUG("FetchPageWrite: page_id %d", page_id);

  // find the page_id in the buffer pool
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    LOG_DEBUG("FetchPageWrite: Find the page id %d in frame id %d", page_id, frame_id);
    PrintFrames();

    lock.unlock();
    pages_[frame_id].WLatch();  // latch_ 解锁之后在加锁

    return {this, &pages_[frame_id]};
  }

  if (replacer_->Size() == 0 && free_list_.empty()) {
    LOG_DEBUG("FetchPageWrite: Replacer size: %zu; Free list size: %zu", replacer_->Size(), free_list_.size());
    LOG_DEBUG("FetchPageWrite: No available free frame for page id: %d", page_id);
    return {this, nullptr};
  }

  frame_id_t free_frame_id;
  if (FindFreeFrame(&free_frame_id)) {
    // read the page from the disk
    pages_[free_frame_id].page_id_ = page_id;
    pages_[free_frame_id].ResetMemory();
    disk_manager_->ReadPage(page_id, pages_[free_frame_id].data_);
    pages_[free_frame_id].is_dirty_ = false;
    pages_[free_frame_id].pin_count_ = 1;
    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);

    page_table_[page_id] = free_frame_id;

    LOG_DEBUG("FetchPageWrite: Allocate the page id %d in frame id %d", page_id, free_frame_id);
    PrintFrames();

    lock.unlock();
    pages_[free_frame_id].WLatch();  // latch_ 解锁之后在加锁

    return {this, &pages_[free_frame_id]};
  }

  LOG_DEBUG("FetchPageWrite: Fail to find a free frame for page id: %d", page_id);

  return {this, nullptr};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  std::lock_guard<std::mutex> lock(latch_);

  // if all frames are in use and not evictable
  if (replacer_->Size() == 0 && free_list_.empty()) {
    return {this, nullptr};
  }

  frame_id_t free_frame_id;
  if (FindFreeFrame(&free_frame_id)) {
    page_id_t new_page_id = AllocatePage();
    pages_[free_frame_id].page_id_ = new_page_id;
    pages_[free_frame_id].is_dirty_ = false;
    pages_[free_frame_id].pin_count_ = 1;
    pages_[free_frame_id].ResetMemory();
    *page_id = new_page_id;

    page_table_[new_page_id] = free_frame_id;

    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);

    LOG_DEBUG("NewPageGuarded: allocate page_id %d in frame_id %d", *page_id, free_frame_id);

    return {this, &pages_[free_frame_id]};
  }

  return {this, nullptr};
}

void BufferPoolManager::PrintFrames() {
  std::string res = "****Buffer Pool: ";
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPageId() == INVALID_PAGE_ID) {
      res += "[NULL] ";
    } else {
      res += ("[" + std::to_string(pages_[i].GetPageId()) + "(" + std::to_string(pages_[i].GetPinCount()) + ")] ");
    }
  }
  LOG_DEBUG("%s", res.c_str());
}

}  // namespace bustub
