#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

// move constructor
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  // no need to use std::move() since they are just pointers
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  // disable the old page guard
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ == nullptr) {
    return;
  }

  if (page_->GetPageId() != INVALID_PAGE_ID) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }

  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this == &that) {
    return *this;
  }

  // drop the current guarded page
  Drop();

  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  // disable the old page guard
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;

  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

// ---------------------------

// Move constructor
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }

  guard_ = std::move(that.guard_);

  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }

  guard_.page_->RUnlatch();
  guard_.Drop();

  // if (guard_.page_->GetPageId() != INVALID_PAGE_ID) {
  //   LOG_DEBUG("ReadPageGuard (%d) unpinning the page", guard_.page_->GetPageId());
  //   guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.page_->IsDirty());
  // }

  // // 应该先把 frame 中 page 的资源给释放出去, 最后在释放 frame 的锁
  // guard_.page_->RUnlatch();
  // guard_.bpm_ = nullptr;
  // guard_.page_ = nullptr;
  // guard_.is_dirty_ = false;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

// ---------------------------

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }

  guard_.page_->WUnlatch();
  guard_.Drop();

  // if (guard_.page_->GetPageId() != INVALID_PAGE_ID) {
  //   LOG_DEBUG("WritePageGuard (%d) unpinning the page", guard_.page_->GetPageId());
  //   guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.page_->IsDirty());
  // }

  // // release the write latch
  // guard_.page_->WUnlatch();
  // guard_.page_ = nullptr;
  // guard_.bpm_ = nullptr;
  // guard_.is_dirty_ = false;
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
