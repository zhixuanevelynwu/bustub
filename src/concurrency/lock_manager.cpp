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
#include <algorithm>
#include <optional>
#include <stack>

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

  // find the previous lock request on this table
  auto txn_id = txn->GetTransactionId();
  auto prev_req = std::find_if(req_queue->request_queue_.begin(), req_queue->request_queue_.end(),
                               [txn_id](const std::shared_ptr<bustub::LockManager::LockRequest> &current) {
                                 return current->txn_id_ == txn_id;
                               });

  // if exists, upgrade the request
  if (prev_req != req_queue->request_queue_.end()) {
    // requested lock mode is the same as that of the lock presently held
    auto prev_lock_mode = (*prev_req)->lock_mode_;
    if (prev_lock_mode == lock_mode) {
      lock.unlock();
      return true;
    }

    // only one transaction should be allowed to upgrade its lock on a given resource
    if (req_queue->upgrading_ != INVALID_TXN_ID) {
      lock.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      return false;
    }

    // check upgrade compatibility
    if (!CanLockUpgrade(prev_lock_mode, lock_mode)) {
      lock.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }

    // upgrade lock
    req_queue->upgrading_ = txn_id;

    // remove from the txn lock set
    GetTableLockSet(txn, prev_lock_mode)->erase(oid);
    (*prev_req)->lock_mode_ = lock_mode;

    // wait until all incompatible locks on the table are released
    bool has_incompatible_lock = true;
    while (has_incompatible_lock) {
      // if txn is aborted while waiting, return false
      if (txn->GetState() == TransactionState::ABORTED) {
        req_queue->upgrading_ = INVALID_TXN_ID;
        req_queue->request_queue_.remove(*prev_req);
        req_queue->cv_.notify_all();
        return false;
      }
      has_incompatible_lock = false;
      for (auto &req : req_queue->request_queue_) {
        if (req->txn_id_ != txn_id && req->granted_ && !AreLocksCompatible(req->lock_mode_, lock_mode)) {
          // wait until the transaction holding the lock releases it
          req_queue->cv_.wait(lock);
          has_incompatible_lock = true;
          break;
        }
      }
    }

    // finally, it is safe to upgrade it
    GetTableLockSet(txn, lock_mode)->emplace(oid);
    req_queue->upgrading_ = INVALID_TXN_ID;
    lock.unlock();
    return true;
  }

  // transaction is not in the queue
  auto new_req = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
  req_queue->request_queue_.push_back(new_req);

  // wait until all incompatible locks on the table are released
  bool has_incompatible_lock = true;
  while (has_incompatible_lock) {
    // if txn is aborted while waiting, return false
    if (txn->GetState() == TransactionState::ABORTED) {
      req_queue->request_queue_.remove(new_req);
      req_queue->cv_.notify_all();
      return false;
    }
    has_incompatible_lock = false;
    for (auto &req : req_queue->request_queue_) {
      if (req->txn_id_ != txn_id && req->granted_ && !AreLocksCompatible(req->lock_mode_, lock_mode)) {
        // wait until the transaction holding the lock releases it
        req_queue->cv_.wait(lock);
        has_incompatible_lock = true;
        break;
      }
    }
  }

  // grant lock
  new_req->granted_ = true;
  GetTableLockSet(txn, lock_mode)->emplace(oid);
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

  // unlock the table here
  GetTableLockSet(txn, lock_mode)->erase(oid);
  req_queue->request_queue_.erase(req_it);
  req_queue->cv_.notify_all();  // notify waiting transactions

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

  // ensure txn has valid lock on table
  auto txn_id = txn->GetTransactionId();
  switch (lock_mode) {
    case LockMode::SHARED:
      if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableSharedLocked(oid) &&
          !txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableIntentionSharedLocked(oid) &&
          !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
        return false;
      }
      break;
    case LockMode::EXCLUSIVE:
      if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
          !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
        return false;
      }
      break;
    // can only S / X lock a row
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
      return false;
  }

  // find the lock request queue for the row (create one if not exist)
  row_lock_map_latch_.lock();
  auto lock_req_on_row = row_lock_map_.find(rid);
  if (lock_req_on_row == row_lock_map_.end()) {
    lock_req_on_row = row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>()).first;
  }
  // lock the lock request queue for the row
  auto req_queue = lock_req_on_row->second;
  std::unique_lock<std::mutex> lock(req_queue->latch_);
  row_lock_map_latch_.unlock();

  // iterate over each lock request on row
  auto prev_req = std::find_if(req_queue->request_queue_.begin(), req_queue->request_queue_.end(),
                               [txn_id](const std::shared_ptr<bustub::LockManager::LockRequest> &current) {
                                 return current->txn_id_ == txn_id;
                               });
  if (prev_req != req_queue->request_queue_.end()) {
    // requested lock mode is the same as that of the lock presently held
    auto prev_lock_mode = (*prev_req)->lock_mode_;
    if (prev_lock_mode == lock_mode) {
      lock.unlock();
      return true;
    }

    // only one transaction should be allowed to upgrade its lock on a given resource
    if (req_queue->upgrading_ != INVALID_TXN_ID) {
      lock.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      return false;
    }

    // check upgrade compatibility
    if (!CanLockUpgrade(prev_lock_mode, lock_mode)) {
      lock.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }

    // upgrade lock
    req_queue->upgrading_ = txn_id;

    // remove from the txn lock set
    RemoveFromRowLockSet(txn, prev_lock_mode, oid, rid);
    (*prev_req)->lock_mode_ = lock_mode;

    // wait until all incompatible locks on the table are released
    bool has_incompatible_lock = true;
    while (has_incompatible_lock) {
      // if txn is aborted while waiting, return false
      if (txn->GetState() == TransactionState::ABORTED) {
        req_queue->upgrading_ = INVALID_TXN_ID;
        req_queue->request_queue_.remove(*prev_req);
        req_queue->cv_.notify_all();
        return false;
      }
      has_incompatible_lock = false;
      for (auto &req : req_queue->request_queue_) {
        if (req->txn_id_ != txn_id && req->granted_ && !AreLocksCompatible(req->lock_mode_, lock_mode)) {
          // wait until the transaction holding the lock releases it
          req_queue->cv_.wait(lock);
          has_incompatible_lock = true;
          break;
        }
      }
    }

    // add back to the txn lock set
    AddToRowLockSet(txn, lock_mode, oid, rid);
    req_queue->upgrading_ = INVALID_TXN_ID;
    lock.unlock();
    return true;
  }

  // transaction is not in the queue
  auto new_req = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);
  req_queue->request_queue_.push_back(new_req);

  // wait until all incompatible locks on the table are released
  bool has_incompatible_lock = true;
  while (has_incompatible_lock) {
    // if txn is aborted while waiting, return false
    if (txn->GetState() == TransactionState::ABORTED) {
      req_queue->request_queue_.erase(
          std::remove(req_queue->request_queue_.begin(), req_queue->request_queue_.end(), new_req),
          req_queue->request_queue_.end());
      req_queue->cv_.notify_all();
      return false;
    }
    has_incompatible_lock = false;
    for (auto &req : req_queue->request_queue_) {
      if (req->txn_id_ != txn_id && req->granted_ && !AreLocksCompatible(req->lock_mode_, lock_mode)) {
        // wait until the transaction holding the lock releases it
        req_queue->cv_.wait(lock);
        has_incompatible_lock = true;
        break;
      }
    }
  }

  new_req->granted_ = true;  // grant lock
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
  // find the lock request queue for the row
  row_lock_map_latch_.lock();
  auto lock_req_on_row = row_lock_map_.find(rid);

  // currently no lock on the table
  auto txn_id = txn->GetTransactionId();
  if (lock_req_on_row == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  // lock the lock request queue for the table
  auto req_queue = lock_req_on_row->second;
  std::unique_lock<std::mutex> lock(req_queue->latch_);
  row_lock_map_latch_.unlock();

  // find request made by txn on the row
  auto req_it =
      std::find_if(req_queue->request_queue_.begin(), req_queue->request_queue_.end(), [txn_id, oid, rid](auto &req) {
        return req->txn_id_ == txn_id && req->granted_ && req->oid_ == oid && req->rid_ == rid;
      });
  if (req_it == req_queue->request_queue_.end()) {
    lock.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  // unlocking S/X lock change the transaction state
  auto lock_mode = (*req_it)->lock_mode_;
  if (!force) {  // ignore for force unlock
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
  }

  // unlock the row here
  RemoveFromRowLockSet(txn, lock_mode, oid, rid);
  req_queue->request_queue_.erase(req_it);
  req_queue->cv_.notify_all();  // notify waiting transactions

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
  auto waits_for_txn = waits_for_.find(t1);
  if (waits_for_txn == waits_for_.end()) {
    // create a new vertex
    waits_for_.emplace(t1, std::vector<txn_id_t>(1, t2));
  } else {
    // add edge if it is not in graph
    if (std::find(waits_for_txn->second.begin(), waits_for_txn->second.end(), t2) == waits_for_txn->second.end()) {
      waits_for_txn->second.emplace_back(t2);
    }
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto waits_for_txn = waits_for_.find(t1);
  // remove edge if it is in graph
  if (waits_for_txn != waits_for_.end()) {
    waits_for_txn->second.erase(std::remove(waits_for_txn->second.begin(), waits_for_txn->second.end(), t2),
                                waits_for_txn->second.end());

    // if results in an isolated vertex, erase the entire vertex
    if (waits_for_txn->second.empty()) {
      waits_for_.erase(t1);
    }
  }
}

void LockManager::RemoveAllEdgesContaining(txn_id_t t2) {
  // remove vertex t2
  waits_for_.erase(t2);

  // stores all vertices that are isolated after removing t2
  std::vector<txn_id_t> to_remove;

  // for each vertex, find and remove its connection to t2
  for (auto &pair : waits_for_) {
    pair.second.erase(std::remove(pair.second.begin(), pair.second.end(), t2), pair.second.end());
    if (pair.second.empty()) {
      to_remove.emplace_back(pair.first);
    }
  }

  // if results in an isolated vertex, erase the entire vertex
  for (auto &txn_id : to_remove) {
    waits_for_.erase(txn_id);
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
  // do nothing if the graph is empty
  if (waits_for_.empty()) {
    return false;
  }

  // sort waits_for_ graph in deterministic order
  for (auto &vertex : waits_for_) {
    std::sort(vertex.second.begin(), vertex.second.end(), [](const auto &a, const auto &b) { return a > b; });
  }

  // sort vertices in deterministic order
  std::vector<txn_id_t> vertices;
  vertices.reserve(waits_for_.size());
  for (auto &vertex : waits_for_) {
    vertices.emplace_back(vertex.first);
  }
  std::sort(vertices.begin(), vertices.end());
  std::unordered_set<txn_id_t> visited;

  // recursively visit each vertex once
  for (auto vertex : vertices) {
    if (visited.count(vertex) > 0) {
      continue;
    }

    // initialize data structures
    std::deque<txn_id_t> ancestors;
    std::stack<txn_id_t> stack;
    stack.push(vertex);

    while (!stack.empty()) {
      auto current = stack.top();
      visited.insert(current);  // mark as visited
      ancestors.push_back(current);
      stack.pop();

      // visit each neighbor
      auto neighbors = waits_for_.find(current);
      if (neighbors == waits_for_.end()) {  // no neighbors
        continue;
      }

      for (auto neighbor : neighbors->second) {
        // add unvisited neighbors to the stack
        if (visited.count(neighbor) == 0) {
          stack.push(neighbor);
        } else if (std::find(ancestors.begin(), ancestors.end(), neighbor) != ancestors.end()) {
          // a cycle exsits if a neighbor is an ancestor of the current vertex
          auto youngest_txn_id = neighbor;
          while (!ancestors.empty()) {
            auto id = ancestors.back();
            if (id == neighbor) {
              break;
            }
            ancestors.pop_back();
            youngest_txn_id = std::max(youngest_txn_id, id);
          }
          // write to txn_id
          *txn_id = youngest_txn_id;
          return true;
        }
      }
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (auto &vertex : waits_for_) {
    auto t1 = vertex.first;
    for (auto t2 : vertex.second) {
      edges.emplace_back(t1, t2);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      // build waits_for_ graph
      table_lock_map_latch_.lock();
      for (auto &pair : table_lock_map_) {
        auto reqs_on_table = pair.second;
        for (auto &waiting_req : reqs_on_table->request_queue_) {
          // ungranted requests should be waiting for all granted ones
          if (!waiting_req->granted_) {
            for (auto &req : reqs_on_table->request_queue_) {
              if (req->granted_) {
                AddEdge(waiting_req->txn_id_, req->txn_id_);
              }
            }
          }
        }
      }
      table_lock_map_latch_.unlock();

      row_lock_map_latch_.lock();
      for (auto &pair : row_lock_map_) {
        auto reqs_on_row = pair.second;
        for (auto &waiting_req : reqs_on_row->request_queue_) {
          if (!waiting_req->granted_) {
            // ungranted requests should be waiting for all granted ones
            for (auto &req : reqs_on_row->request_queue_) {
              if (req->granted_) {
                AddEdge(waiting_req->txn_id_, req->txn_id_);
              }
            }
          }
        }
      }
      row_lock_map_latch_.unlock();

      txn_id_t youngest_txn_in_cycle;
      while (HasCycle(&youngest_txn_in_cycle)) {
        RemoveAllEdgesContaining(youngest_txn_in_cycle);
        auto txn = txn_manager_->GetTransaction(youngest_txn_in_cycle);
        txn_manager_->Abort(txn);
      }
      waits_for_.clear();
    }
  }
}
}  // namespace bustub
