//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  id_2_txn_.emplace(txn->GetTransactionId(), txn);
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(
        txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }

  if (!LockPrepare(txn, rid)) {
    return false;
  }

  LockRequestQueue *request_queue = &lock_table_.find(rid)->second;
  request_queue->request_queue_.emplace_back(txn->GetTransactionId(),
                                             LockMode::SHARED);

  if (request_queue->is_writing_) {
    DeadlockPrevent(txn, request_queue);
    request_queue->cv_.wait(lock, [request_queue, txn]() -> bool {
      return txn->GetState() == TransactionState::ABORTED ||
             !request_queue->is_writing_;
    });
  }

  CheckAborted(txn, request_queue);

  txn->GetSharedLockSet()->emplace(rid);
  request_queue->sharing_count_++;
  auto iter = GetIterator(&request_queue->request_queue_, txn);
  iter->granted_ = true;
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  id_2_txn_.emplace(txn->GetTransactionId(), txn);

  if (!LockPrepare(txn, rid)) {
    return false;
  }

  LockRequestQueue *request_queue = &lock_table_.find(rid)->second;
  request_queue->request_queue_.emplace_back(txn->GetTransactionId(),
                                             LockMode::EXCLUSIVE);

  if (request_queue->is_writing_ || request_queue->sharing_count_ > 0) {
    DeadlockPrevent(txn, request_queue);
    request_queue->cv_.wait(lock, [request_queue, txn]() -> bool {
      return txn->GetState() == TransactionState::ABORTED ||
             (!request_queue->is_writing_ &&
              request_queue->sharing_count_ == 0);
    });
  }

  CheckAborted(txn, request_queue);

  txn->GetExclusiveLockSet()->emplace(rid);
  request_queue->is_writing_ = true;
  auto iter = GetIterator(&request_queue->request_queue_, txn);
  iter->granted_ = true;

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),
                                    AbortReason::LOCK_ON_SHRINKING);
  }

  LockRequestQueue *request_queue = &lock_table_.find(rid)->second;

  if (request_queue->upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),
                                    AbortReason::UPGRADE_CONFLICT);
  }

  txn->GetSharedLockSet()->erase(rid);
  request_queue->sharing_count_--;
  auto iter = GetIterator(&request_queue->request_queue_, txn);
  iter->lock_mode_ = LockMode::EXCLUSIVE;
  iter->granted_ = false;  // cancel grant.

  if (request_queue->is_writing_ || request_queue->sharing_count_ > 0) {
    DeadlockPrevent(txn, request_queue);
    request_queue->upgrading_ =
        true;  // set upgrading to true, maybe other txn wants to upgrade.
    request_queue->cv_.wait(lock, [request_queue, txn]() -> bool {
      return txn->GetState() == TransactionState::ABORTED ||
             (!request_queue->is_writing_ &&
              request_queue->sharing_count_ == 0);
    });
  }

  CheckAborted(txn, request_queue);

  txn->GetExclusiveLockSet()->emplace(rid);
  request_queue->upgrading_ = false;
  request_queue->is_writing_ = true;
  iter = GetIterator(&request_queue->request_queue_, txn);
  iter->granted_ = true;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  LockRequestQueue *request_queue = &lock_table_.find(rid)->second;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  auto iter = GetIterator(&request_queue->request_queue_, txn);
  LockMode mode = iter->lock_mode_;
  request_queue->request_queue_.erase(iter);

  if (!(LockMode::SHARED == mode &&
        txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);  // set shrinking
  }

  if (LockMode::SHARED == mode) {
    if (--request_queue->sharing_count_ == 0) {
      request_queue->cv_.notify_all();
    }
  } else {
    request_queue->is_writing_ = false;
    request_queue->cv_.notify_all();
  }

  return true;
}

bool LockManager::LockPrepare(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),
                                    AbortReason::LOCK_ON_SHRINKING);
  }

  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid),
                        std::forward_as_tuple());
  }
  return true;
}

void LockManager::CheckAborted(Transaction *txn,
                               LockRequestQueue *request_queue) {
  if (txn->GetState() == TransactionState::ABORTED) {
    auto iter = GetIterator(&request_queue->request_queue_, txn);
    request_queue->request_queue_.erase(iter);
    throw TransactionAbortException(txn->GetTransactionId(),
                                    AbortReason::DEADLOCK);
  }
}

std::list<LockManager::LockRequest>::iterator LockManager::GetIterator(
    std::list<LockRequest> *request_queue, Transaction *txn) {
  LOG_DEBUG("txn_id is %d", txn->GetTransactionId());
  for (auto iter = request_queue->begin(); iter != request_queue->end();
       ++iter) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      return iter;
    }
  }
  return request_queue->end();
}

void LockManager::DeadlockPrevent(Transaction *txn,
                                  LockRequestQueue *request_queue) {
  for (const auto &request : request_queue->request_queue_) {
    if (request.granted_ && request.txn_id_ > txn->GetTransactionId()) {
      id_2_txn_[request.txn_id_]->SetState(TransactionState::ABORTED);
      if (request.lock_mode_ == LockMode::SHARED) {
        request_queue->sharing_count_--;
      } else {
        request_queue->is_writing_ = false;
      }
    }
  }
}

}  // namespace bustub
