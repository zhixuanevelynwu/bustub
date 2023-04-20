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
#include <optional>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // make sure txn is in a valid state & isolation level to take lock
  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }

  // find the lock request queue for the table (create one if not exist)
  table_lock_map_latch_.lock();
  auto lock_req_on_table = table_lock_map_.find(oid);
  if (lock_req_on_table == table_lock_map_.end()) {
    lock_req_on_table = table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>()).first;
  }

  // lock the lock request queue for the table
  auto req_queue = lock_req_on_table->second;
  req_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  // iterate over each lock request on table
  std::optional<LockRequest *> to_upgrade;
  for (auto req : req_queue->request_queue_) {
    // if the transaction already holds a lock on the table, upgrade the lock to the specified lock_mode (if possible)
    if (req->txn_id_ == txn->GetTransactionId()) {
      // requested lock mode is the same as that of the lock presently held
      if (req->lock_mode_ == lock_mode) {
        return true;
      }

      // only one transaction should be allowed to upgrade its lock on a given resource
      if (req_queue->upgrading_ != INVALID_TXN_ID || !CanLockUpgrade(req->lock_mode_, lock_mode)) {
        txn_manager_->Abort(txn);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      }

      // finally, it is safe to upgrade it
      req_queue->upgrading_ = txn->GetTransactionId();
      to_upgrade = req;
      continue;
    }

    // if another transaction holds a lock on the table, check its compatibility
    if (!AreLocksCompatible(req->lock_mode_, lock_mode)) {
      // wait until the transaction holding the lock releases it
      AddEdge(txn->GetTransactionId(), req->txn_id_);
      txn->LockTxn();
    }
  }

  // at this point, there should be no incompatible lock on the table
  // either upgrade the lock or add a new lock request to the lock request queue
  if (to_upgrade) {
    auto old_lock_mode = (*to_upgrade)->lock_mode_;
    (*to_upgrade)->lock_mode_ = lock_mode;
    (*to_upgrade)->granted_ = false;
    return UpgradeLockTable(txn, old_lock_mode, lock_mode, oid);
  }
  auto new_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  req_queue->request_queue_.emplace_back(new_req);

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

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::scoped_lock lock(waits_for_latch_);
  auto wait_for_txn = waits_for_.find(t1);
  if (wait_for_txn == waits_for_.end()) {
    waits_for_.emplace(t1, std::vector<txn_id_t>(t2));
  } else {
    wait_for_txn->second.emplace_back(t2);
  }
}

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
