//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  // 1. Get a new directory_page and Set the page id of directory_page member.
  auto directory_page = reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->NewPage(&directory_page_id_, nullptr)->GetData());
  directory_page->SetPageId(directory_page_id_);
  // 2. Get a new bucket_page
  page_id_t bucket_page_id;
  buffer_pool_manager_->NewPage(&bucket_page_id, nullptr);
  // 3. add the bucket_page_id into the directory
  directory_page->SetBucketPageId(0, bucket_page_id);

  // 4. remember to unpin(isdirty -> true)
  buffer_pool_manager_->UnpinPage(directory_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx = Hash(key) & dir_page->GetGlobalDepthMask();
  return bucket_idx;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t page_id = dir_page->GetBucketPageId(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_, nullptr);
  if (page != nullptr) {
    auto directory_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
    return directory_page;
  }
  return nullptr;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id, nullptr);
  if (page != nullptr) {
    auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
    return bucket_page;
  }
  return nullptr;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();

  table_latch_.RLock();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  bool success = bucket_page->GetValue(key, comparator_, result);
  table_latch_.RUnlock();

  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // the lock is so complicated.
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  if (bucket_page->Insert(key, value, comparator_)) {
    table_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    return true;
  }

  if (bucket_page->IsFull()) {
    if (SplitInsert(transaction, key, value)) {
      table_latch_.WUnlock();
      buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
      buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
      return true;
    }
  }

  // duplicate KV && SplitInsert false
  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  //local depth equals to gobal depth
  if(dir_page->GetLocalDepth(bucket_idx) == dir_page->GetGlobalDepth()){
     
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  return false;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
