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

#include <stdexcept>
#include <algorithm>

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

#define LOCKS                                                                           \
{                                                                                       \
  LockMode::SHARED, LockMode::INTENTION_SHARED, LockMode::SHARED_INTENTION_EXCLUSIVE,   \  
  LockMode::EXCLUSIVE, LockMode::INTENTION_EXCLUSIVE                                    \
}

// check if the current txn has the lock on the target table
auto LockManager::IsTableLocked(Transaction *txn, const table_oid_t &oid, const std::vector<LockMode> &lock_modes) -> std::optional<LockMode> {
  std::optional<LockMode> mode = std::nullopt;
  for (const auto& lock_mode : lock_modes) {
    switch (lock_mode) {
      case LockMode::SHARED:
        if (txn->IsTableSharedLocked(oid)) {
          mode = std::make_optional<LockMode>(LockMode::SHARED);
        }
        break;
      case LockMode::EXCLUSIVE:
        if (txn->IsTableExclusiveLocked(oid)) {
          mode = std::make_optional<LockMode>(LockMode::EXCLUSIVE);
        }
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        if (txn->IsTableIntentionExclusiveLocked(oid)) {
          mode = std::make_optional<LockMode>(LockMode::INTENTION_EXCLUSIVE);
        }
        break;
      case LockMode::INTENTION_SHARED:
        if (txn->IsTableIntentionSharedLocked(oid)) {
          mode = std::make_optional<LockMode>(LockMode::INTENTION_SHARED);
        }
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
          mode = std::make_optional<LockMode>(LockMode::SHARED_INTENTION_EXCLUSIVE);
        }
        break;
    }
    if (mode.has_value()) {
      return mode;
    }
  }
  return std::nullopt;
}

// LockRequestQueue: granted lock requests | waiting lock requests
auto LockManager::LockRequestQueue::CheckCompatibility(LockMode lock_mode, ListType::iterator lock_request_iter) -> bool {
  BUSTUB_ASSERT(lock_request_iter != request_queue_.end(), "lock request iterator should not be the end iterator");
  lock_request_iter++;

  auto conflict = [] (int mask, std::vector<LockMode>&& lock_modes) -> bool {
    std::any_of(lock_modes.begin(), lock_modes.end(), [] (int mask, LockMode lock_mode) {
      return mask & (1 << static_cast<int>(lock_mode) != 0);
    });
  };

  int mask = 0;
  for (auto it = request_queue_.begin(); it != lock_request_iter; it++) {
    const auto &request = *it;
    const auto mode = request->lock_mode_;

    // skip the mode checking if the txn state is ABORTED 
    auto *txn = TransactionManager::GetTransaction(request->txn_id_);
    if (txn->GetState() == TransactionState::ABORTED) {
      continue;
    }

    switch (mode){
      case LockMode::INTENTION_SHARED:
        if (conflict(mask, {LockMode::EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        if (conflict(mask, {LockMode::EXCLUSIVE, LockMode::SHARED, LockMode::SHARED_INTENTION_EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if (conflict(mask, {LockMode::EXCLUSIVE, LockMode::SHARED, LockMode::SHARED_INTENTION_EXCLUSIVE, LockMode::INTENTION_EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::SHARED:
        if (conflict(mask, {LockMode::EXCLUSIVE, LockMode::INTENTION_EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::EXCLUSIVE:
        if (mask != 0) {
          return false;
        }
        break;
    }
    mask |= (1 << static_cast<int>(mode));
  } 
  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // check the transaction state 
  const auto &txn_id = txn->GetTransactionId();
  const auto &txn_state = txn->GetState();
  const auto &iso_level = txn->GetIsolationLevel();

  if (txn_state == TransactionState::ABORTED || txn_state == TransactionState::COMMITTED) {
    throw std::invalid_argument("Aborted/Committed transaction should not request locks.");
  } else if (txn_state == TransactionState::SHRINKING) {
    switch (iso_level) {
      case IsolationLevel::REPEATABLE_READ:
        AbortAndThrowException(txn, AbortReason::LOCK_ON_SHRINKING);
      case IsolationLevel::READ_COMMITTED:
        if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED) {
          break;
        }
        AbortAndThrowException(txn, AbortReason::LOCK_ON_SHRINKING);
      case IsolationLevel::READ_UNCOMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
          AbortAndThrowException(txn, AbortReason::LOCK_ON_SHRINKING);
        } else {  // S/IS/SIX
          AbortAndThrowException(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        }
    }
  } else if (txn_state == TransactionState::GROWING) {
    if (iso_level == IsolationLevel::READ_UNCOMMITTED && (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      AbortAndThrowException(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }

  // granted 和 waiting 的锁都存放在同一个队列中
  // 在插入新的请求之前, 遍历当前队列查看当前事物是否在此之前有相同的请求 (相同的 <txn, oid>)
  // 如果存在这样的请求, 且 granted_=true, 就意味着当前事物已经在这个表上获取了一把锁
  // 这个时候我们要去检查当前线程持有锁的 lock_mode 是否与新的请求 lock_mode 一致
  // 一致的话就意味着当前的锁已经被持有了, 直接返回 true
  // 不一致的话就升级锁

  // check if the lock should be upgraded
  std::optional<LockMode> old_lock_mode = IsTableLocked(txn, oid, LOCKS);

  // upgrade the lock
  bool upgrade = false;
  if (old_lock_mode.has_value()) {
    if (old_lock_mode.value() == lock_mode) {  // lock is already held by this txn
      return true;
    }

    switch (old_lock_mode.value()) {
      case LockMode::INTENTION_SHARED:
        break;
      case LockMode::SHARED:
      case LockMode::INTENTION_EXCLUSIVE:
        if (lock_mode != LockMode::EXCLUSIVE || lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
          AbortAndThrowException(txn, AbortReason::INCOMPATIBLE_UPGRADE);
        }
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if (lock_mode != LockMode::EXCLUSIVE) {
          AbortAndThrowException(txn, AbortReason::INCOMPATIBLE_UPGRADE);
        }
        break;
    }

    upgrade = true;
  }

  // acquire the corresponding lock request queue of the table
  std::shared_ptr<LockRequest> lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
  table_lock_map_latch_.lock();

  // no one is holding lock on the table, create a new LockRequestQueue
  auto table_lock_queue_iter = table_lock_map_.find(oid);
  if (table_lock_queue_iter == table_lock_map_.end()) {  
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid]->request_queue_.emplace_back(lock_request);
    lock_request->granted_ = true;  // grant the lock because there is only one lock request in the queue
    table_lock_map_latch_.unlock(); 
  } else {  // find the corresponding LockRequestQueue
    auto table_lock_queue_ptr = table_lock_queue_iter->second;
    std::unique_lock<std::mutex> queue_lock(table_lock_queue_ptr->latch_);  // use along with conditional variable
    table_lock_map_latch_.unlock(); // release the latch once you find the LockRequestQueue

    auto &request_queue = table_lock_queue_ptr->request_queue_;
    decltype(request_queue.begin()) lock_request_iter;  // deltype 的用法

    if (upgrade) {
      if (table_lock_queue_ptr->upgrading_ != INVALID_TXN_ID) {  // only allow one txn to upgrade the lock in the queue
        AbortAndThrowException(txn, AbortReason::UPGRADE_CONFLICT);
      }

      // locate the iterator of the old lock request in the queue
      lock_request_iter = std::find_if(request_queue.begin(), request_queue.end(), [txn_id] (const auto &lock_req) {
        return lock_req->txn_id_ == txn_id;
      });
      // 这里的 grant_ 一定为 true, 因为如果事务此前的请求还没有被通过, 事务会被阻塞在 LockManager 中, 不可能再去获取另外一把锁
      BUSTUB_ASSERT(lock_request_iter->get()->granted_ && lock_request_iter != request_queue.end(), "Fail to find the granted lock request in the lock queue while upgrading.");

      // update the upgrading_ (indicate that txn is currently upgrading the lock)
      table_lock_queue_ptr->upgrading_ = txn_id;

      // find the next lock request iter that hasn't been granted a lock (upgraded lock should be prioritised)
      auto next_request_iter = std::find_if(lock_request_iter, request_queue.end(), [] (const auto &lock_req) {
        return lock_req->grant_ == false;
      });
      request_queue.erase(lock_request_iter);  // erase the old lock request
      lock_request_iter = request_queue.insert(next_request_iter, lock_request);  // insert at the prioritised location

      // remove the old table lock from txn
      switch (old_lock_mode.value()) {
        case LockMode::SHARED:
          txn->GetSharedTableLockSet()->erase(oid);
          break;
        case LockMode::EXCLUSIVE:
          txn->GetExclusiveTableLockSet()->erase(oid);
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          txn->GetIntentionExclusiveTableLockSet()->erase(oid);
          break;
        case LockMode::INTENTION_SHARED:
          txn->GetIntentionSharedTableLockSet()->erase(oid);
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
          break;
      }

    } else {
      lock_request_iter = request_queue.insert(request_queue.end(), lock_request);
    }

    // use cv to wait until the lock is available 
    table_lock_queue_ptr->cv_.wait(queue_lock, [&] () {
      return txn->GetState() == TransactionState::ABORTED || table_lock_queue_ptr->CheckCompatibility(lock_mode, lock_request_iter);
    });

    // if the txn is aborted in the meantime, do not grant the lock and return false
    if (txn->GetState() == TransactionState::ABORTED) {
      request_queue.erase(lock_request_iter);
      return false;
    }

    // convert the upgrading back to INVALID_TXN_ID (with latches)
    lock_request->granted_ = true;
    if (upgrade) {
      table_lock_queue_ptr->upgrading_ = INVALID_TXN_ID;
    }
    
  }

  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }

  return true;

}

// To-do
auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  const auto &txn_id = txn->GetTransactionId();
  const auto &iso_level = txn->GetIsolationLevel();
  LockMode lock_mode;

  // make sure no row lock in the table is held by this txn 
  const auto &s_lock_set = txn->GetSharedRowLockSet();
  const auto &x_lock_set = txn->GetExclusiveRowLockSet();
  if ((s_lock_set->find(oid) != s_lock_set->end() && !s_lock_set->at(oid).empty()) ||
      (x_lock_set->find(oid) != x_lock_set->end() && !x_lock_set->at(oid).empty())) {
    AbortAndThrowException(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  // make sure the table lock is held
  if (!IsTableLocked(txn, oid, LOCKS)) {  // 为什么这里需要 unlock table_lock_map_latch_ 呢?
    AbortAndThrowException(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // find the lock queue in the table lock queue
  table_lock_map_latch_.lock();
  auto table_lock_map_it = table_lock_map_.find(oid);
  assert(table_lock_map_it != table_lock_map_.end());

  // find the lock mode in the lock queue, and erase the corresponding lock request from the queue
  // 这里单独给了作用域 (unique_lock)
  {
    auto &lrque = table_lock_map_it->second;
    std::unique_lock queue_lock(lrque->latch_);
    table_lock_map_latch_.unlock();

    auto &request_queue = lrque->request_queue_;
    auto iter = std::find_if(request_queue.begin(), request_queue.end(), [txn_id] (LockRequest &lock_request) {
      return txn_id == lock_request.txn_id_;
    });
    assert(iter != request_queue.end() && iter->get()->granted_);

    lock_mode = iter->get()->lock_mode_;
    request_queue.erase(iter);
    lrque->cv_.notify_all(); // 唤醒所有阻塞在该 table 的事务, 检查能否获取锁
  }

  // update transaction state
  if (txn->GetState() == TransactionState::GROWING) {
    switch (iso_level) {
      case IsolationLevel::REPEATABLE_READ:
        if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_COMMITTED:
      case IsolationLevel::READ_UNCOMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
    }
  }

  // erase the old lock from lock set
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
  }

  return true;
}

// To-do
auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  const auto &txn_id = txn->GetTransactionId();
  const auto &txn_state = txn->GetState();
  const auto &iso_level = txn->GetIsolationLevel();

  // row lock does not support intention lock
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED
      || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    AbortAndThrowException(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  // checking if the lock_mode, isoaltion level and state match
  switch (iso_level) {
    case IsolationLevel::REPEATABLE_READ:
      // all locks are allowed in the growing state 
      // no locks are allowed in shrinking state 
      if (txn_state == TransactionState::SHRINKING) {
        AbortAndThrowException(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      // all locks are allowed in the growing state 
      // only IS, S locks are allowed in the shrinking state (for locking row, only S locks are allowed here)
      if (txn_state == TransactionState::SHRINKING) {
        if (lock_mode != LockMode::SHARED) {
          AbortAndThrowException(txn, AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      // txn is only allowed to take IX, X locks in growing phase 
      if (lock_mode == LockMode::SHARED) {
        AbortAndThrowException(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn_state == TransactionState::SHRINKING) {
        AbortAndThrowException(txn, AbortReason::LOCK_ON_SHRINKING);
      }
  }

  bool upgrade = false;
  if (lock_mode == LockMode::SHARED) {
    // lock is already held, return true 
    if (txn->IsRowSharedLocked(oid, rid)) {
      return true;
    }
    // cannot upgrade the lock when holding X lock on the row
    if (txn->IsRowExclusiveLocked(oid, rid)) {
      AbortAndThrowException(txn, AbortReason::UPGRADE_CONFLICT);
    }
    // table lock must be held before putting on the row lock
    // 对于 S lock, table 只要持有任意的锁都是可以的
    if (!IsTableLocked(txn, oid, LOCKS)) {
      AbortAndThrowException(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  } else {
    // lock is already held, return true
    if (txn->IsRowExclusiveLocked(oid, rid)) {
      return true;
    }
    // 对于 X row lock, table 需要持有 X/IX/SIX 锁
    if (!IsTableLocked(txn, oid, {LockMode::EXCLUSIVE, LockMode::INTENTION_EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE})) {
      AbortAndThrowException(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
    if (txn->IsRowSharedLocked(oid, rid)) {
      upgrade = true;
    }
  }

  row_lock_map_latch_.lock();
  auto row_lock_map_iter = row_lock_map_.find(rid);
  std::shared_ptr<LockRequestQueue> lrque;  // 提前声明, 如果对应的 lock request queue 不存在需要创建

  auto lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);
  assert(txn->GetState() != TransactionState::ABORTED);

  if (row_lock_map_iter == row_lock_map_.end()) {
    lrque = std::make_shared<LockRequestQueue>();
    row_lock_map_[rid] = lrque;
    lock_request->granted_ = true;
    lrque->request_queue_.push_back(lock_request);
    row_lock_map_latch_.unlock();
  } else {
    lrque = row_lock_map_iter->second;
    std::unique_lock lock(lrque->latch_);
    row_lock_map_latch_.unlock();

    // 提前声明, 根据后续 if 条件决定 request_iter 的值
    auto &request_queue = lrque->request_queue_;
    decltype(request_queue.begin()) request_iter;

    // check if the current lock can be upgraded
    if (upgrade) {
      if (lrque->upgrading_ != INVALID_TXN_ID) {
        AbortAndThrowException(txn, AbortReason::UPGRADE_CONFLICT);
      }

      // find the old row lock for future update
      // 你如果可以确定 lock_request 的类型的话 (std::shared_ptr<LockRequest>), 那么用 auto 会更省事一点
      request_iter = std::find_if(request_queue.begin(), request_queue.end(), [txn_id] (const auto &lock_request) {
        return lock_request->txn_id == txn_id;
      });
      assert(request_iter != request_queue.end() && (*request_iter)->lock_mode_ == LockMode::SHARED);

      lrque->upgrading_ = txn_id;

      auto iter = std::find_if(request_iter, request_queue.end(), [] (const auto &lock_request) {
        return lock_request->granted_ == false;
      });
      request_queue.erase(request_iter);
      request_queue.insert(iter, lock_request);

      txn->GetSharedLockSet()->erase(rid);
      txn->GetSharedRowLockSet()->at(oid).erase(rid);

    } else {  // if the upgrade is not needed, then just push lock request in the back
      // request_queue.push_back(lock_request);  // 不应该这么写, 后面还需要复用这个位置的迭代器
      request_iter = request_queue.insert(request_queue.end(), lock_request);
    }

    lrque->cv_.wait(lock, [&]() {
      return txn->GetState() == TransactionState::ABORTED || lrque->CheckCompatibility(lock_mode, request_iter);
    });

    if (txn->GetState() == TransactionState::ABORTED) {
      request_queue.erase(request_iter);
      return false;
    }

    lock_request->granted_ = true;
    if (upgrade) {
      lrque->upgrading_ = INVALID_TXN_ID;
    }
  
  }

  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedLockSet()->insert(rid);
    txn->GetSharedRowLockSet()->at(oid).insert(rid);
  } else {
    txn->GetExclusiveLockSet()->insert(rid);
    txn->GetExclusiveRowLockSet()->at(oid).insert(rid);
  }

  return true;
}

// To-do: include the logic related to "force"
auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  const auto &txn_id = txn->GetTransactionId();
  const auto &iso_level = txn->GetIsolationLevel();
  LockMode lock_mode;

  if (!txn->IsRowExclusiveLocked(oid, rid) && !txn->IsRowSharedLocked(oid, rid)) {
    AbortAndThrowException(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  row_lock_map_latch_.lock();
  auto row_lock_map_iter = row_lock_map_.find(rid);
  assert(row_lock_map_iter != row_lock_map_.end());

  {
    auto &lrque = row_lock_map_iter->second;
    std::unique_lock queue_lock(lrque->latch_);  // 锁好这个 request_queue 之后就可以释放 row_lock_map latch 了
    row_lock_map_latch_.unlock();

    auto &request_queue = lrque->request_queue_;
    auto iter = std::find_if(request_queue.begin(), request_queue.end(), [txn_id](const auto &lock_request) {
      return txn_id == lock_request->txn_id;
    });
    assert(iter != request_queue.end() && (*iter)->granted_);

    lock_mode = (*iter)->lock_mode_;
    request_queue.erase(iter);
    lrque->cv_.notify_all();
  }

  // update transaction state
  if (txn->GetState() == TransactionState::GROWING) {
    switch (iso_level) {
      case IsolationLevel::REPEATABLE_READ:  // row lock: X/S locks, and repeatable read allows both of them
        txn->SetState(TransactionState::SHRINKING);
        break;
      case IsolationLevel::READ_COMMITTED:
      case IsolationLevel::READ_UNCOMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
    }
  }

  // update transaction's lock set
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedLockSet()->erase(rid);
    txn->GetSharedRowLockSet()->at(oid).erase(rid);
  } else {
    txn->GetExclusiveLockSet()->erase(rid);
    txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
  }

  return true;

}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
