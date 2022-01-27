//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (page_id == INVALID_PAGE_ID || !page_table_.count(page_id)) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].GetData());
  // LOG_DEBUG("WritePage! Page_id is %d", page_id);
  // pages_[page_table_[page_id]].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for (auto it : page_table_) {
    disk_manager_->WritePage(it.first, pages_[it.second].GetData());
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);
  page_id_t newPageId = AllocatePage();
  if (0 == free_list_.size() && 0 == replacer_->Size()) {
    return nullptr;
  }
  frame_id_t P;
  if (free_list_.size() > 0) {
    P = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&P);
    // LOG_DEBUG("frame_id is %d", P);
    // If P is dirty, we must flush it first then update P's metadata.
    if (pages_[P].IsDirty()) {
      FlushPgImp(pages_[P].GetPageId());
    }
    // remove page_id -> P in page_table
    for (const auto &it : page_table_) {
      if (it.second == P) {
        page_table_.erase(it.first);
        break;
      }
    }
  }
  pages_[P].page_id_ = newPageId;
  pages_[P].is_dirty_ = false;
  pages_[P].pin_count_ = 1;
  pages_[P].ResetMemory();
  page_table_[newPageId] = P;
  *page_id = newPageId;
  return &pages_[P];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id)) {
    frame_id_t requestedPageId = page_table_[page_id];
    pages_[requestedPageId].pin_count_ += 1;
    //·in LRUreplacer or ·not int LRUreplacer, we pin the frame_id
    replacer_->Pin(requestedPageId);
    return &pages_[requestedPageId];
  }
  if (0 == free_list_.size() && 0 == replacer_->Size()) {
    return nullptr;
  }
  frame_id_t replacementPageId;
  if (free_list_.size() > 0) {
    replacementPageId = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&replacementPageId);
    if (pages_[replacementPageId].IsDirty()) {
      FlushPgImp(pages_[replacementPageId].GetPageId());
      // LOG_DEBUG("WritePage! Page_id is %d, frame_id is %d", pages_[replacementPageId].GetPageId(),
      // replacementPageId);
    }
    for (const auto &it : page_table_) {
      if (it.second == replacementPageId) {
        page_table_.erase(it.first);
        break;
      }
    }
  }
  page_table_.insert({page_id, replacementPageId});
  pages_[replacementPageId].page_id_ = page_id;
  pages_[replacementPageId].is_dirty_ = false;
  pages_[replacementPageId].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[replacementPageId].GetData());
  // LOG_DEBUG("ReadPage! Page_id is %d", pages_[replacementPageId].GetPageId());
  return &pages_[replacementPageId];
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free
  // list.
  std::lock_guard<std::mutex> lock(latch_);
  DeallocatePage(page_id);
  if (!page_table_.count(page_id)) {
    return true;
  } else {
    frame_id_t P = page_table_[page_id];
    if (pages_[P].GetPinCount() != 0) return false;
    page_table_.erase(page_id);
    //should we flushpg into disk if it is dirty?
    pages_[P].ResetMemory();
    pages_[P].is_dirty_ = false;
    pages_[P].pin_count_ = 0;
    pages_[P].page_id_ = INVALID_PAGE_ID;
    // return it to the free list
    free_list_.emplace_back(P);
  }
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  auto P = page_table_[page_id];
  if (pages_[P].GetPinCount() <= 0) return false;
  pages_[P].is_dirty_ = is_dirty;
  pages_[P].pin_count_ -= 1;
  if (pages_[P].GetPinCount() == 0) {
    replacer_->Unpin(P);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
