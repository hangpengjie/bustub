//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (!less_k_list_.empty()) {
    for (auto rit = less_k_list_.rbegin(); rit != less_k_list_.rend(); ++rit) {
      if (auto entry = node_store_.find(*rit); entry->second.is_evictable_) {
        // *frame_id = entry->second.fid_;
        *frame_id = *rit;
        less_k_list_.erase(entry->second.it_);
        node_store_.erase(entry);
        this->curr_size_--;
        return true;
      }
    }
  }
  if (!more_k_list_.empty()) {
    for (auto rit = more_k_list_.rbegin(); rit != more_k_list_.rend(); ++rit) {
      if (auto entry = node_store_.find(*rit); entry->second.is_evictable_) {
        // *frame_id = entry->second.fid_;
        *frame_id = *rit;
        more_k_list_.erase(entry->second.it_);
        node_store_.erase(entry);
        this->curr_size_--;
        return true;
      }
    }
  }
  frame_id = nullptr;
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ENSURE(frame_id >= 0, "frame_id is invalid");
  BUSTUB_ENSURE(static_cast<size_t>(frame_id) < this->replacer_size_, "frame_id is invalid");
  auto entry = node_store_.find(frame_id);
  if (entry == node_store_.end()) {
    less_k_list_.emplace_front(frame_id);
    node_store_.insert(std::make_pair(frame_id, LRUKNode{0, frame_id, false, less_k_list_.begin()}));
    // this->curr_size_++;
    entry = node_store_.find(frame_id);
  }
  entry->second.k_++;
  if (entry->second.k_ == this->k_) {
    less_k_list_.erase(entry->second.it_);
    more_k_list_.emplace_front(frame_id);
    entry->second.it_ = more_k_list_.begin();
  } else if (entry->second.k_ > this->k_) {
    more_k_list_.erase(entry->second.it_);
    more_k_list_.emplace_front(frame_id);
    entry->second.it_ = more_k_list_.begin();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ENSURE(frame_id >= 0, "frame_id is invalid");
  BUSTUB_ENSURE(static_cast<size_t>(frame_id) < this->replacer_size_, "frame_id is invalid");
  auto entry = node_store_.find(frame_id);
  if (entry == node_store_.end() || entry->second.is_evictable_ == set_evictable) {
    return;
  }
  if (set_evictable) {
    this->curr_size_++;
  } else {
    this->curr_size_--;
  }
  entry->second.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ENSURE(frame_id >= 0, "frame_id is invalid");
  BUSTUB_ENSURE((size_t)frame_id < this->replacer_size_, "frame_id is invalid");

  auto entry = node_store_.find(frame_id);
  if (entry == node_store_.end()) {
    return;
  }
  BUSTUB_ENSURE(entry->second.is_evictable_, "frame_id is not evictable");
  if (entry->second.k_ < this->k_) {
    less_k_list_.erase(entry->second.it_);
  } else {
    more_k_list_.erase(entry->second.it_);
  }
  this->curr_size_--;
  node_store_.erase(entry);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return this->curr_size_;
}

}  // namespace bustub
