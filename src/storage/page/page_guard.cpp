#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr) {
    bpm_->UnpinPage(PageId(), is_dirty_);
  }
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  page_->RLatch();
  auto ret = ReadPageGuard(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
  return ret;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  page_->WLatch();
  auto ret = WritePageGuard(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
  return ret;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  auto &guard = guard_;
  if (guard.page_ != nullptr) {
    guard.bpm_->UnpinPage(guard.PageId(), guard.is_dirty_);
    guard.page_->RUnlatch();
  }
  guard.bpm_ = nullptr;
  guard.page_ = nullptr;
  guard.is_dirty_ = false;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  auto &guard = guard_;
  if (guard.page_ != nullptr) {
    guard.bpm_->UnpinPage(guard.PageId(), guard.is_dirty_);
    guard.page_->WUnlatch();
  }
  guard.bpm_ = nullptr;
  guard.page_ = nullptr;
  guard.is_dirty_ = false;
}

WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub