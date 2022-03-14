//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (0 == Size()) {
    return false;
  }
  *frame_id = lru_list_.back();
  lru_hash_.erase(lru_list_.back());
  lru_list_.pop_back();  // remove least recently use.
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (lru_hash_.find(frame_id) != lru_hash_.end()) {
    lru_list_.erase(lru_hash_[frame_id]);
    lru_hash_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (lru_hash_.find(frame_id) != lru_hash_.end()) {
    // lru_list_.erase(lru_hash_[frame_id]);
    return;
  } else {
    if (capacity_ == Size()) {  // if the lru_list_ is full
      lru_hash_.erase(lru_list_.back());
      lru_list_.pop_back();
    }
  }
  lru_list_.push_front(frame_id);  // push frame_id to head of the lru_list_
  lru_hash_[frame_id] = lru_list_.begin();
}

size_t LRUReplacer::Size() { return lru_list_.size(); }

}  // namespace bustub
