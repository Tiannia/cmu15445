//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  uint32_t idx = 0;
  bool success{false};
  for (; idx < BUCKET_ARRAY_SIZE; ++idx) {
    if (!IsOccupied(idx)) break;
    if (IsReadable(idx) && !cmp(array_[idx].first, key)) {
      result->push_back(array_[idx].second);
      success = true;
    }
  }
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  uint32_t idx = 0;
  for (; idx < BUCKET_ARRAY_SIZE; ++idx) {
    if (!IsOccupied(idx)) break;
    if (!cmp(array_[idx].first, key) && array_[idx].second == value) return false;
  }
  if (idx == BUCKET_ARRAY_SIZE) return false;
  // LOG_DEBUG("BUCKET_ARRAY_SIZE: %d", static_cast<int>BUCKET_ARRAY_SIZE); --->496
  for (idx = 0; idx < BUCKET_ARRAY_SIZE; ++idx) {
    if (!IsReadable(idx)) {  // is empty(unreadable)
      array_[idx].first = key;
      array_[idx].second = value;
      SetReadable(idx);
      // if index->i at an unoccupied pos, we set the occupied_[pos] to 1
      SetOccupied(idx);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  uint32_t idx = 0;
  for (; idx < BUCKET_ARRAY_SIZE; ++idx) {
    if (!IsOccupied(idx)) break;
    if (!IsReadable(idx)) continue;
    if (!cmp(array_[idx].first, key) && array_[idx].second == value) {
      readable_[idx >> 0x03] &= ~(0x01 << (idx & 0x07));
      // LOG_DEBUG("VAL: %d", static_cast<char>(~(0x01 << i % 8)));
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  if (IsReadable(bucket_idx))
    return array_[bucket_idx].first;
  else
    return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  if (IsReadable(bucket_idx))
    return array_[bucket_idx].second;
  else
    return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  readable_[bucket_idx >> 0x03] &= ~(0x01 << (bucket_idx & 0x07));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  return occupied_[bucket_idx >> 0x03] >> (bucket_idx & 0x07) & 0x01;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx >> 0x03] |= 0x01 << (bucket_idx & 0x07);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return readable_[bucket_idx >> 0x03] >> (bucket_idx & 0x07) & 0x01;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  readable_[bucket_idx >> 0x03] |= 0x01 << (bucket_idx & 0x07);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  for(uint32_t idx = 0; idx < BUCKET_ARRAY_SIZE; idx++){
    if(!IsOccupied(idx)){
      return false;
    }
    if(!IsReadable(idx)){
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::GetOccupiedSize(){
  uint32_t size = 0;
  for(uint32_t idx; idx < BUCKET_ARRAY_SIZE; idx++){
    if(!IsOccupied(idx)){
      break;
    }
    size++; 
  }
  return size;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t size = 0;
  for(uint32_t idx; idx < BUCKET_ARRAY_SIZE; idx++){
    if(!IsOccupied(idx)){
      break;
    }
    if(IsReadable(idx)){
      size++;
    }
  }
  return size;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  for (uint32_t idx = 0; idx < BUCKET_ARRAY_SIZE; idx++){
    if(!IsOccupied(idx)){
      break;
    }
    if(IsReadable(idx)){
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
