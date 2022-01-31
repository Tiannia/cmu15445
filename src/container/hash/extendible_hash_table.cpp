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
  // 2. Get a new bucket_page (get to konw that global depth is zero, which mean just have one bucket_page)
  page_id_t bucket_page_id;
  buffer_pool_manager_->NewPage(&bucket_page_id, nullptr);
  // 3. add the bucket_page_id into the directory
  directory_page->SetBucketPageId(0, bucket_page_id);

  // 4. remember to unpin(isdirty -> true)
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
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
inline page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  return bucket_page_id;
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
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.RUnlock();

  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // the lock is so complicated.
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  if (bucket_page->Insert(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    table_latch_.WUnlock();
    return true;
  }

  if (bucket_page->IsFull()) {
    if (SplitInsert(transaction, key, value)) {
      buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
      buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
      table_latch_.WUnlock();
      return true;
    }
  }

  // duplicate KV and SplitInsert false
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.WUnlock();
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  // local depth equals to global depth
  if (dir_page->GetLocalDepth(bucket_idx) == dir_page->GetGlobalDepth()) {
    if (dir_page->CanIncrGlobalDepth()) {
      uint32_t current_dir_array_size = dir_page->Size();
      for (uint32_t idx = 0; idx < current_dir_array_size; ++idx) {
        // set bucket_page_ids_[current_dir_array_size ~ current_dir_array_size * 2] 's page_id and local_depth
        dir_page->SetBucketPageId(idx + current_dir_array_size, dir_page->GetBucketPageId(idx));
        dir_page->SetLocalDepth(idx + current_dir_array_size, dir_page->GetLocalDepth(idx));
      }
      // then we increase the global depth.
      dir_page->IncrGlobalDepth();
    } else {
      buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
      buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
      return false;
    }
  }

  // local depth less than global depth
  if (dir_page->GetLocalDepth(bucket_idx) < dir_page->GetGlobalDepth()) {
    // We get a new bucket page
    page_id_t new_bucket_page_id;
    HASH_TABLE_BUCKET_TYPE *new_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(
        buffer_pool_manager_->NewPage(&new_bucket_page_id, nullptr)->GetData());
    uint32_t local_high_bits = dir_page->GetLocalHighBit(bucket_idx);             // ex: 1000
    uint32_t shared_bits = bucket_idx & dir_page->GetLocalDepthMask(bucket_idx);  // ex: 0101
    uint32_t current_dir_array_size = dir_page->Size();
    // Then we split the bucket page by the new high bits (0xx is old bucket page / 1xx is new bucket page).
    // ex: 1101 / 0101
    for (uint32_t idx = shared_bits; idx < current_dir_array_size; idx += local_high_bits) {
      if (idx & local_high_bits) {
        dir_page->SetBucketPageId(idx, new_bucket_page_id);
      }
      // For every index of ..x101, we increase the local depth of the directory page.
      dir_page->IncrLocalDepth(idx);
    }
    // Refresh
    memcpy(reinterpret_cast<void *>(new_bucket_page), reinterpret_cast<void *>(bucket_page), PAGE_SIZE);
    uint32_t bucket_occpuied_size = bucket_page->GetOccupiedSize();
    // loop the <key> in the bucket page.
    for (uint32_t idx = 0; idx < bucket_occpuied_size; ++idx) {
      if (bucket_page->IsReadable(idx)) {
        if (KeyToPageId(bucket_page->KeyAt(idx), dir_page) ==
            bucket_page_id) {  // if the key on old bucket page reaches the same page after remapping.
          new_bucket_page->RemoveAt(idx);
        } else {
          bucket_page->RemoveAt(idx);
        }
      }
    }
    buffer_pool_manager_->UnpinPage(new_bucket_page_id, true, nullptr);  // new bucket page is dirty.
  }
  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true,
                                  nullptr);  // change sth like global depth, local depth, bucket_page_ids_
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);  // change sth by removeat(idx)
  // try insert!
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  bool success = bucket_page->Remove(key, value, comparator_);
  if (success && bucket_page->IsEmpty()) {
    Merge(transaction, key, value);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    buffer_pool_manager_->DeletePage(bucket_page_id); //Remember to unpin before delete
    table_latch_.WUnlock();
    return success;
  }
  if (success) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  } else {
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  table_latch_.WUnlock();
  return success;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToPageId(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  uint32_t bucket_page_depth = dir_page->GetLocalDepth(bucket_idx);
  if (0 == bucket_page_depth) {
    return;
  }
  // find the image bucket page
  uint32_t image_bucket_idx = bucket_idx ^ (0x01 << (bucket_page_depth - 1));
  page_id_t image_bucket_page_id = dir_page->GetBucketPageId(image_bucket_idx);
  uint32_t image_bucket_page_depth = dir_page->GetLocalDepth(image_bucket_idx);

  // merge them if they have same local depth.
  if (bucket_page_depth == image_bucket_page_depth) {
    uint32_t mask_bits = 0x01 << (bucket_page_depth - 1);
    uint32_t shared_bits = bucket_idx & (mask_bits - 1);
    uint32_t current_dir_array_size = dir_page->Size();
    for (uint32_t idx = shared_bits; idx < current_dir_array_size; idx += mask_bits) {
      dir_page->SetBucketPageId(idx, image_bucket_page_id);
      dir_page->DecrLocalDepth(idx);
    }
    buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);  // dirty is true
  } else {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);  // dirty is false
  }
}

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
