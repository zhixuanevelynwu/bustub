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

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {
/**
 * Acquire a lock on table_oid_t in the given lock_mode.
 * If the transaction already holds a lock on the table, upgrade the lock
 * to the specified lock_mode (if possible).
 *
 * This method should abort the transaction and throw a
 * TransactionAbortException under certain circumstances.
 * See [LOCK_NOTE] in header file.
 *
 * @param txn the transaction requesting the lock upgrade
 * @param lock_mode the lock mode for the requested lock
 * @param oid the table_oid_t of the table to be locked in lock_mode
 * @return true if the upgrade is successful, false otherwise
 */
auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // find the lock request queue for the table (create one if not exist)
  table_lock_map_latch_.lock();
  auto lock_req_on_table = table_lock_map_.find(oid);
  if (lock_req_on_table == table_lock_map_.end()) {
    lock_req_on_table = table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>()).first;
  }

  // lock the lock request queue for the table
  lock_req_on_table->second->latch_.lock();
  table_lock_map_latch_.unlock();

  // check if the txn already holds a lock on the table
  auto predicate = [&](auto &lock_req) {
    return lock_req->txn_id_ == txn->GetTransactionId() && lock_req->oid_ == oid;
  };
  auto lock_req = std::find_if(lock_req_on_table->second->request_queue_.begin(),
                               lock_req_on_table->second->request_queue_.end(), predicate);

  // if the transaction already holds a lock on the table, upgrade the lock to the specified lock_mode (if possible).
  if (lock_req != lock_req_on_table->second->request_queue_.end() &&
      CanLockUpgrade((*lock_req)->lock_mode_, lock_mode)) {
    (*lock_req)->lock_mode_ = lock_mode;
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { return true; }

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
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
