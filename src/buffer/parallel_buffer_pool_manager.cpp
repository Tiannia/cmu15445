//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  BPMIs.resize(num_instances);
  for (size_t i = 0; i < num_instances; ++i) {
    BPMIs[i] = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
  }
  this->num_instances_ = num_instances;
  this->index_ = 0;
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instances_; ++i) {
    delete BPMIs[i];
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  int res = 0;
  for (size_t i = 0; i < num_instances_; ++i) {
    res += BPMIs[i]->GetPoolSize();
  }
  return res;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return BPMIs[page_id % num_instances_];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  auto BPMI = GetBufferPoolManager(page_id);
  return BPMI->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  auto BPMI = GetBufferPoolManager(page_id);
  return BPMI->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  auto BPMI = GetBufferPoolManager(page_id);
  return BPMI->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  int start_index = index_;
  int loop_index = index_;
  index_ = (index_ + 1) % num_instances_;
  Page *res = nullptr;
  while (true) {
    res = BPMIs[loop_index]->NewPage(page_id);
    if(res != nullptr) return res;
    loop_index = (loop_index + 1) % num_instances_;
    if(loop_index == start_index) return nullptr;
  }
  return res;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  auto BPMI = GetBufferPoolManager(page_id);
  return BPMI->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; ++i) {
    BPMIs[i]->FlushAllPages();
  }
}

}  // namespace bustub
