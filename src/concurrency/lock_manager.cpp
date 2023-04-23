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

/**
 * @brief lock table
 *
 * @param txn
 * @param lock_mode
 * @param oid
 * @return true
 * @return false
 */
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
  std::unique_lock<std::mutex> lock(req_queue->latch_);
  table_lock_map_latch_.unlock();

  // iterate over each lock request on table
  auto txn_id = txn->GetTransactionId();
  for (auto &req : req_queue->request_queue_) {
    // if the transaction already holds a lock on the table, upgrade the lock to the specified lock_mode (if possible)
    if (req->txn_id_ == txn_id) {
      // requested lock mode is the same as that of the lock presently held
      if (req->lock_mode_ == lock_mode) {
        req->granted_ = true;  // grant lock
        lock.unlock();
        return true;
      }

      // only one transaction should be allowed to upgrade its lock on a given resource
      if ((req->granted_ && !CanLockUpgrade(req->lock_mode_, lock_mode)) || req_queue->upgrading_ != INVALID_TXN_ID) {
        lock.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
        return false;
      }

      // finally, it is safe to upgrade it
      req_queue->upgrading_ = txn_id;
      UpgradeTableLockSet(txn, req->lock_mode_, lock_mode, oid);
      req->lock_mode_ = lock_mode;
      req_queue->upgrading_ = INVALID_TXN_ID;
      req->granted_ = true;  // grant upgraded lock

      lock.unlock();
      return true;
    }
  }

  // wait until all incompatible locks on the table are released
  bool has_incompatible_lock = true;
  while (has_incompatible_lock) {
    has_incompatible_lock = false;
    for (auto &req : req_queue->request_queue_) {
      if (req->txn_id_ != txn_id && req->granted_ && !AreLocksCompatible(req->lock_mode_, lock_mode)) {
        // wait until the transaction holding the lock releases it
        AddEdge(txn_id, req->txn_id_);
        req_queue->cv_.wait(lock);
        has_incompatible_lock = true;
        break;
      }
    }
  }

  // transaction is not in the queue, and there is no incompatible lock
  auto new_req = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
  // LockRequest new_req(txn_id, lock_mode, oid);
  new_req->granted_ = true;  // grant lock
  req_queue->request_queue_.push_back(new_req);
  // req_queue->request_queue_.push_back(&new_req);
  AddToTableLockSet(txn, lock_mode, oid);

  lock.unlock();
  return true;
}

/**
 * @brief unlock table
 *
 * @param txn
 * @param oid
 * @return true
 * @return false
 */
auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // find the lock request queue for the table
  table_lock_map_latch_.lock();
  auto lock_req_on_table = table_lock_map_.find(oid);
  auto txn_id = txn->GetTransactionId();

  // currently no lock on the table
  if (lock_req_on_table == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  // lock the lock request queue for the table
  auto req_queue = lock_req_on_table->second;
  std::unique_lock<std::mutex> lock(req_queue->latch_);
  table_lock_map_latch_.unlock();

  // find request made by txn on the table
  auto req_it = std::find_if(req_queue->request_queue_.begin(), req_queue->request_queue_.end(),
                             [txn_id](auto &req) { return req->txn_id_ == txn_id && req->granted_; });

  // transaction does not hold a lock on the table
  if (req_it == req_queue->request_queue_.end()) {
    lock.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  // ensure transaction does not hold lock on any rows on the table
  if (HoldsRowOnTable(txn, oid)) {
    lock.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }

  // unlocking S/X lock change the transaction state
  auto lock_mode = (*req_it)->lock_mode_;
  if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        txn->SetState(TransactionState::SHRINKING);
        break;
      case IsolationLevel::READ_COMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_UNCOMMITTED:
        // BUSTUB_ASSERT(lock_mode != LockMode::SHARED, "READ_UNCOMMITTED undefined behavior: S locks are not
        // permitted");
        txn->SetState(TransactionState::SHRINKING);
        break;
    }
  }

  // unlock the row here
  RemoveAllEdgesContaining(txn_id);
  RemoveFromTableLockSet(txn, oid);
  // (*req_it)->granted_ = false; // not sure about this
  req_queue->cv_.notify_all();  // notify waiting transactions
  req_queue->request_queue_.erase(req_it);

  lock.unlock();
  return true;
}

/**
 * @brief lock row
 *
 * @param txn
 * @param lock_mode
 * @param oid
 * @param rid
 * @return true
 * @return false
 */
auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
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
  std::unique_lock<std::mutex> lock(req_queue->latch_);
  table_lock_map_latch_.unlock();

  // ensure txn has a lock on table
  auto txn_id = txn->GetTransactionId();
  auto req_it = std::find_if(req_queue->request_queue_.begin(), req_queue->request_queue_.end(),
                             [txn_id](auto &req) { return req->txn_id_ == txn_id && req->granted_; });
  if (req_it == req_queue->request_queue_.end()) {
    lock.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }

  // ensure txn holds appropriate lock on the table
  if (!CheckAppropriateLockOnTable((*req_it)->lock_mode_, lock_mode)) {
    lock.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }

  // iterate over each lock request on table
  for (auto &req : req_queue->request_queue_) {
    // if the transaction already holds a lock on the row, upgrade the lock to the specified lock_mode (if possible)
    if (req->txn_id_ == txn_id && req->rid_ == rid) {
      // requested lock mode is the same as that of the lock presently held
      if (req->lock_mode_ == lock_mode) {
        req->granted_ = true;  // grant lock
        lock.unlock();
        return true;
      }

      // only one transaction should be allowed to upgrade its lock on a given resource
      if ((req->granted_ && !CanLockUpgrade(req->lock_mode_, lock_mode)) || req_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
        lock.unlock();
        return false;
      }

      // finally, it is safe to upgrade it
      req_queue->upgrading_ = txn_id;
      UpgradeTableLockSet(txn, req->lock_mode_, lock_mode, oid);
      req->lock_mode_ = lock_mode;
      req_queue->upgrading_ = INVALID_TXN_ID;
      req->granted_ = true;  // grant upgraded lock

      lock.unlock();
      return true;
    }
  }

  // wait until all incompatible locks on the table are released
  bool has_incompatible_lock = true;
  while (has_incompatible_lock) {
    has_incompatible_lock = false;
    for (auto &req : req_queue->request_queue_) {
      if (req->txn_id_ != txn_id && req->granted_ && !AreLocksCompatible(req->lock_mode_, lock_mode)) {
        // wait until the transaction holding the lock releases it
        AddEdge(txn_id, req->txn_id_);
        req_queue->cv_.wait(lock);
        has_incompatible_lock = true;
        break;
      }
    }
  }

  // transaction is not in the queue, and there is no incompatible lock
  auto new_req = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);
  new_req->granted_ = true;  // grant lock
  req_queue->request_queue_.push_back(new_req);
  AddToRowLockSet(txn, lock_mode, oid, rid);

  lock.unlock();
  return true;
}

/**
 * @brief unlock row
 *
 * @param txn
 * @param oid
 * @param rid
 * @param force
 * @return true
 * @return false
 */
auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  // find the lock request queue for the table
  table_lock_map_latch_.lock();
  auto lock_req_on_table = table_lock_map_.find(oid);
  auto txn_id = txn->GetTransactionId();

  // currently no lock on the table
  if (lock_req_on_table == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  // lock the lock request queue for the table
  auto req_queue = lock_req_on_table->second;
  std::unique_lock<std::mutex> lock(req_queue->latch_);
  table_lock_map_latch_.unlock();

  // find request made by txn on the table
  auto req_it =
      std::find_if(req_queue->request_queue_.begin(), req_queue->request_queue_.end(),
                   [txn_id, rid](auto &req) { return req->txn_id_ == txn_id && req->granted_ && req->rid_ == rid; });
  if (req_it == req_queue->request_queue_.end()) {
    lock.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  // unlocking S/X lock change the transaction state
  auto lock_mode = (*req_it)->lock_mode_;
  if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        txn->SetState(TransactionState::SHRINKING);
        break;
      case IsolationLevel::READ_COMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_UNCOMMITTED:
        BUSTUB_ASSERT(lock_mode != LockMode::SHARED, "READ_UNCOMMITTED undefined behavior: S locks are not permitted");
        txn->SetState(TransactionState::SHRINKING);
        break;
    }
  }

  // unlock the row here
  RemoveAllEdgesContaining(txn_id);
  RemoveFromRowLockSet(txn, oid, rid);
  req_queue->cv_.notify_all();  // notify waiting transactions
  req_queue->request_queue_.erase(req_it);

  lock.unlock();
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
  for (auto &pair : table_lock_map_) {
    auto req_queue = pair.second;
    req_queue->latch_.lock();
    req_queue->request_queue_.clear();
    req_queue->latch_.unlock();
  }
}

/** Graph API */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::scoped_lock lock(waits_for_latch_);
  auto waits_for_txn = waits_for_.find(t1);
  if (waits_for_txn == waits_for_.end()) {
    // create a new vertex
    waits_for_.emplace(t1, std::vector<txn_id_t>(t2));
  } else {
    // add edge if it is not in graph
    auto edges = waits_for_txn->second;
    if (std::find(edges.begin(), edges.end(), t2) == edges.end()) {
      edges.emplace_back(t2);
    }
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::scoped_lock lock(waits_for_latch_);
  auto waits_for_txn = waits_for_.find(t1);
  // remove edge if it is in graph
  if (waits_for_txn != waits_for_.end()) {
    auto edges = waits_for_txn->second;
    auto to_remove = std::find(edges.begin(), edges.end(), t2);
    if (to_remove != edges.end()) {
      edges.erase(to_remove);
    }
  }
}

void LockManager::RemoveAllEdgesContaining(txn_id_t t2) {
  std::scoped_lock lock(waits_for_latch_);
  for (auto &pair : waits_for_) {
    auto txns = pair.second;
    auto t2_it = std::find(txns.begin(), txns.end(), t2);
    if (t2_it != txns.end()) {
      txns.erase(t2_it);
    }
  }
}

/**
 * @brief Looks for a cycle by using depth-first search (DFS). If it finds a cycle, HasCycle should store the
 * transaction id of the youngest transaction in the cycle in txn_id and return true. Your function should return the
 * first cycle it finds. If your graph has no cycles, HasCycle should return false.
 *
 * @param txn_id
 * @return true
 * @return false
 */
auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::scoped_lock lock(waits_for_latch_);  // necessary?

  // do nothing if the graph is empty
  if (waits_for_.empty()) {
    return false;
  }

  // store all unvisited vertices
  std::unordered_set<txn_id_t> unvisited;
  for (auto &pair : waits_for_) {
    unvisited.insert(pair.first);
  }

  // helper to check if a node is visited
  auto is_visited = [unvisited](txn_id_t txn_id) -> bool { return unvisited.count(txn_id) > 0; };
  while (!unvisited.empty()) {
    // pick any unvisited vertices to start
    std::stack<txn_id_t> stack;
    stack.push(*unvisited.begin());
    while (!stack.empty()) {
      auto current = stack.top();
      stack.pop();

      // if not visited, mark as visited
      if (!is_visited(current)) {
        unvisited.erase(current);

        // visit each neighbor
        auto neighbors = waits_for_.find(current)->second;
        for (auto neighbor : neighbors) {
          // add unvisited neighbors to the stack
          if (!is_visited(neighbor)) {
            stack.push(neighbor);
          } else {  // a cycle exsits if a neighbor is visited and is not a parent of the current vertex
            auto children = waits_for_.find(neighbor)->second;
            if (std::find(children.begin(), children.end(), current) == children.end()) {
              return true;
            }
          }
        }
      }
    }
  }

  return false;
}

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
