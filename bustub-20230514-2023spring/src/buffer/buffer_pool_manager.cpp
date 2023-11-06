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
    // std::cout << "Goes Here" << std::endl;
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

    return &pages_[free_frame_id];
  }

  return nullptr;
}

// find a free frame from free_list_ or replacer_
auto BufferPoolManager::FindFreeFrame(frame_id_t *free_frame_id) -> bool {
  if (!free_list_.empty()) {  // pick a free frame from free_list_
    *free_frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  if (replacer_->Evict(free_frame_id)) {  // has evictable frames in replacer_
    // Physical page that will be written back to the disk
    Page *evicted_page = &pages_[*free_frame_id];
    page_table_.erase(evicted_page->page_id_);

    if (evicted_page->IsDirty()) {  // write back the dirty page
      disk_manager_->WritePage(evicted_page->page_id_, evicted_page->GetData());
    }
    return true;
  }
  return false;
}

// fetch the page from the disk
auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  if (replacer_->Size() == 0 && free_list_.empty()) {
    return nullptr;
  }

  // find the page_id in the buffer pool
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    return &pages_[frame_id];
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

    return &pages_[free_frame_id];
  }

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }

  pages_[frame_id].pin_count_--;
  pages_[frame_id].is_dirty_ = is_dirty;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

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
  page_table_.erase(page_id);

  // do I need to do something with pages[frame_id] or replacer_???

  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  for (const auto &kv : page_table_) {
    // kv: <page_id, frame_id>
    disk_manager_->WritePage(kv.first, pages_[kv.second].data_);
    pages_[kv.second].is_dirty_ = false;
  }
  page_table_.clear();
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
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  std::lock_guard<std::mutex> lock(latch_);

  if (replacer_->Size() == 0 && free_list_.empty()) {
    return {this, nullptr};
  }

  // page is already in the buffer pool
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    return {this, &pages_[frame_id]};
  }

  // fetch the page from the disk
  frame_id_t free_frame_id;
  if (FindFreeFrame(&free_frame_id)) {
    pages_[free_frame_id].page_id_ = page_id;
    pages_[free_frame_id].ResetMemory();
    disk_manager_->ReadPage(page_id, pages_[free_frame_id].data_);
    pages_[free_frame_id].is_dirty_ = false;
    pages_[free_frame_id].pin_count_ = 1;

    page_table_[page_id] = free_frame_id;
    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);

    return {this, &pages_[free_frame_id]};
  }

  return {this, nullptr};
}

// fetch the page and put the read latch on the page
auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  std::lock_guard<std::mutex> lock(latch_);

  if (replacer_->Size() == 0 && free_list_.empty()) {
    return {this, nullptr};
  }

  // page found in the buffer pool
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_ += 1;
    pages_[frame_id].RLatch();
    return {this, &pages_[frame_id]};
  }

  // fetch page from disk
  frame_id_t free_frame_id;
  if (FindFreeFrame(&free_frame_id)) {
    pages_[free_frame_id].page_id_ = page_id;
    pages_[free_frame_id].ResetMemory();
    disk_manager_->ReadPage(page_id, pages_[free_frame_id].data_);
    pages_[free_frame_id].is_dirty_ = false;
    pages_[free_frame_id].pin_count_ = 1;

    pages_[free_frame_id].WLatch();

    page_table_[page_id] = free_frame_id;
    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);

    return {this, &pages_[free_frame_id]};
  }

  return {this, nullptr};
}

// fetch the page and put the write latch on the page
auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  std::lock_guard<std::mutex> lock(latch_);

  if (replacer_->Size() == 0 && free_list_.empty()) {
    return {this, nullptr};
  }

  // page found in the buffer pool
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_ += 1;
    pages_[frame_id].WLatch();  // blocks if sb is holding the lock
    return {this, &pages_[frame_id]};
  }

  // fetch page from disk
  frame_id_t free_frame_id;
  if (FindFreeFrame(&free_frame_id)) {
    pages_[free_frame_id].page_id_ = page_id;
    pages_[free_frame_id].ResetMemory();
    disk_manager_->ReadPage(page_id, pages_[free_frame_id].data_);
    pages_[free_frame_id].is_dirty_ = false;
    pages_[free_frame_id].pin_count_ = 1;

    pages_[free_frame_id].WLatch();

    page_table_[page_id] = free_frame_id;
    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);

    return {this, &pages_[free_frame_id]};
  }

  return {this, nullptr};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  std::lock_guard<std::mutex> lock(latch_);

  if (replacer_->Size() == 0 && free_list_.empty()) {
    return {this, nullptr};
  }

  frame_id_t free_frame_id;
  if (FindFreeFrame(&free_frame_id)) {
    *page_id = AllocatePage();
    pages_[free_frame_id].pin_count_ = 1;
    pages_[free_frame_id].page_id_ = *page_id;
    pages_[free_frame_id].is_dirty_ = false;
    pages_[free_frame_id].ResetMemory();

    page_table_[*page_id] = free_frame_id;

    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);
    return {this, &pages_[free_frame_id]};
  }

  return {this, nullptr};
}

}  // namespace bustub
