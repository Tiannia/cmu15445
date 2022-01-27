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

LRUReplacer::LRUReplacer(size_t num_pages) : capacity(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (0 == Size()) {
    return false;
  }
  *frame_id = LRUList.back();
  LRUHash.erase(LRUList.back());
  LRUList.pop_back(); //remove least recently use.
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
std::lock_guard<std::mutex> lock(mutex_);
  if (LRUHash.find(frame_id) != LRUHash.end()) {
    LRUList.erase(LRUHash[frame_id]);
    LRUHash.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (LRUHash.find(frame_id) != LRUHash.end()) {
    //LRUList.erase(LRUHash[frame_id]);
    return;
  } else {
    if (capacity == Size()) { //if the LRUList is full
        LRUHash.erase(LRUList.back());
        LRUList.pop_back();
    }
  }
  LRUList.push_front(frame_id); //push frame_id to head of the LRUList
  LRUHash[frame_id] = LRUList.begin();
}

size_t LRUReplacer::Size() { return LRUList.size(); }

}  // namespace bustub
