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
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
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

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  replacer_.~unique_ptr();
  replacer_.~unique_ptr();
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(this->latch_);
  // first get frame_id from free_list
  if (free_list_.empty()) {
    if (frame_id_t free_frame_id; replacer_->Evict(&free_frame_id)) {
      page_table_.erase(pages_[free_frame_id].GetPageId());
      if (pages_[free_frame_id].IsDirty()) {
        auto promise = disk_scheduler_->CreatePromise();
        auto future = promise.get_future();
        disk_scheduler_->Schedule(
            {true, pages_[free_frame_id].GetData(), pages_[free_frame_id].GetPageId(), std::move(promise)});
        future.get();
      }
      pages_[free_frame_id].ResetMemory();
      auto new_page_id = this->AllocatePage();

      pages_[free_frame_id].pin_count_ = 1;
      pages_[free_frame_id].page_id_ = new_page_id;
      pages_[free_frame_id].is_dirty_ = false;
      page_table_[new_page_id] = free_frame_id;
      replacer_->RecordAccess(free_frame_id);
      replacer_->SetEvictable(free_frame_id, false);
      *page_id = new_page_id;
      return &pages_[free_frame_id];
    }
  } else {
    auto free_frame_id = free_list_.front();
    free_list_.pop_front();
    auto new_page_id = this->AllocatePage();
    pages_[free_frame_id].ResetMemory();
    pages_[free_frame_id].page_id_ = new_page_id;
    pages_[free_frame_id].pin_count_ = 1;
    page_table_[new_page_id] = free_frame_id;
    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);
    *page_id = new_page_id;
    return &pages_[free_frame_id];
  }
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> lock(this->latch_);
  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    pages_[it->second].pin_count_++;
    replacer_->RecordAccess(it->second);
    replacer_->SetEvictable(it->second, false);
    return &pages_[it->second];
  }
  if (free_list_.empty()) {
    if (frame_id_t free_frame_id; replacer_->Evict(&free_frame_id)) {
      page_table_.erase(pages_[free_frame_id].GetPageId());
      if (pages_[free_frame_id].IsDirty()) {
        auto promise = disk_scheduler_->CreatePromise();
        auto future = promise.get_future();
        disk_scheduler_->Schedule(
            {true, pages_[free_frame_id].GetData(), pages_[free_frame_id].GetPageId(), std::move(promise)});
        future.get();
      }
      pages_[free_frame_id].ResetMemory();
      pages_[free_frame_id].pin_count_ = 1;
      pages_[free_frame_id].page_id_ = page_id;
      pages_[free_frame_id].is_dirty_ = false;
      page_table_[page_id] = free_frame_id;

      replacer_->RecordAccess(free_frame_id);
      replacer_->SetEvictable(free_frame_id, false);
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({false, pages_[free_frame_id].GetData(), page_id, std::move(promise)});
      future.get();
      return &pages_[free_frame_id];
    }
  } else {
    auto free_frame_id = free_list_.front();
    free_list_.pop_front();

    pages_[free_frame_id].ResetMemory();
    pages_[free_frame_id].page_id_ = page_id;
    pages_[free_frame_id].is_dirty_ = false;
    pages_[free_frame_id].pin_count_ = 1;
    page_table_[page_id] = free_frame_id;

    replacer_->RecordAccess(free_frame_id);
    replacer_->SetEvictable(free_frame_id, false);
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({false, pages_[free_frame_id].GetData(), page_id, std::move(promise)});
    future.get();
    return &pages_[free_frame_id];
  }
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> lock(this->latch_);
  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    if (pages_[it->second].GetPinCount() == 0) {
      return false;
    }
    pages_[it->second].pin_count_--;
    pages_[it->second].is_dirty_ |= is_dirty;
    if (pages_[it->second].pin_count_ == 0) {
      replacer_->SetEvictable(it->second, true);
    }
    return true;
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(this->latch_);
  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, pages_[it->second].GetData(), page_id, std::move(promise)});
    future.get();
    pages_[it->second].is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(this->latch_);
  for (auto &it : page_table_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, pages_[it.second].GetData(), it.first, std::move(promise)});
    future.get();
    pages_[it.second].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(this->latch_);
  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    if (pages_[it->second].GetPinCount() > 0) {
      return false;
    }
    replacer_->Remove(it->second);
    free_list_.push_back(it->second);
    pages_[it->second].ResetMemory();
    pages_[it->second].page_id_ = INVALID_PAGE_ID;
    pages_[it->second].pin_count_ = 0;
    pages_[it->second].is_dirty_ = false;
    page_table_.erase(page_id);
    this->DeallocatePage(page_id);
  }
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto pg_ptr = FetchPage(page_id);
  pg_ptr->RLatch();
  return {this, pg_ptr};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto pg_ptr = FetchPage(page_id);
  pg_ptr->WLatch();
  return {this, pg_ptr};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub