//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid) {}

    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    /** Whether the lock has been granted or not */
    bool granted_{false};
  };

  class LockRequestQueue {
   public:
    /** List of lock requests for the same resource (table or row) */
    std::list<std::shared_ptr<LockRequest>> request_queue_;
    // std::list<LockRequest *> request_queue_;
    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction (if any) */
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */
    std::mutex latch_;
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() = default;

  void StartDeadlockDetection() {
    BUSTUB_ENSURE(txn_manager_ != nullptr, "txn_manager_ is not set.")
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    UnlockAll();

    enable_cycle_detection_ = false;

    if (cycle_detection_thread_ != nullptr) {
      cycle_detection_thread_->join();
      delete cycle_detection_thread_;
    }
  }

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime, do not grant the lock and return false.
   *
   *
   * MULTIPLE TRANSACTIONS:
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests, all should be granted at the same time
   *    as long as FIFO is honoured.
   *
   * SUPPORTED LOCK MODES:
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   *
   *
   * ISOLATION LEVEL:
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   *
   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *
   *    REPEATABLE_READ:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        No locks are allowed in the SHRINKING state
   *
   *    READ_COMMITTED:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        Only IS, S locks are allowed in the SHRINKING state
   *
   *    READ_UNCOMMITTED:
   *        The transaction is required to take only IX, X locks.
   *        X, IX locks are allowed in the GROWING state.
   *        S, IS, SIX locks are never allowed
   *
   *
   * MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   *
   *
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *
   *    A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
   *
   *    While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
   *
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update its
   *    lock sets appropriately (check transaction.h)
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *
   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   *
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   *
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state.
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

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
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @param force unlock the tuple regardless of isolation level, not changing the transaction state
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force = false) -> bool;

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * When a transaction t2 is unlocked, we want to remove it from the waits for graph
   * @param t2
   */
  auto RemoveAllEdgesContaining(txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;

  TransactionManager *txn_manager_;

 private:
  /** Spring 2023 */
  /* You are allowed to modify all functions below. */

  auto GetTableLockSet(Transaction *txn, LockMode lock_mode) -> std::shared_ptr<std::unordered_set<table_oid_t>> {
    switch (lock_mode) {
      case LockMode::SHARED:
        return txn->GetSharedTableLockSet();
      case LockMode::EXCLUSIVE:
        return txn->GetExclusiveTableLockSet();
      case LockMode::INTENTION_SHARED:
        return txn->GetIntentionSharedTableLockSet();
      case LockMode::INTENTION_EXCLUSIVE:
        return txn->GetIntentionExclusiveTableLockSet();
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        return txn->GetSharedIntentionExclusiveTableLockSet();
    }
  }

  auto GetRowLockSet(Transaction *txn, LockMode lock_mode)
      -> std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> {
    // BUSTUB_ASSERT(lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE, "Invalid intention lock on
    // row");
    if (lock_mode == LockMode::SHARED) {
      return txn->GetSharedRowLockSet();
    }
    return txn->GetExclusiveRowLockSet();
  }

  auto HoldsRowOnTable(Transaction *txn, table_oid_t oid) -> bool {
    auto row_slock_set = txn->GetSharedRowLockSet();
    auto slocked_rows = row_slock_set->find(oid);
    if (slocked_rows != row_slock_set->end() && !slocked_rows->second.empty()) {
      return true;
    }

    auto row_xlock_set = txn->GetExclusiveRowLockSet();
    auto xlocked_rows = row_xlock_set->find(oid);
    return xlocked_rows != row_xlock_set->end() && !xlocked_rows->second.empty();
  }

  /** Book Keeping Helpers */
  auto AddToRowLockSet(Transaction *txn, LockMode lock_mode, table_oid_t oid, RID rid) -> void {
    auto row_lock_set = GetRowLockSet(txn, lock_mode);
    auto pair = row_lock_set->find(oid);
    if (pair != row_lock_set->end()) {
      pair->second.emplace(rid);
    } else {
      row_lock_set->insert({oid, std::unordered_set<RID>{rid}});
    }
  }

  auto RemoveFromRowLockSet(Transaction *txn, LockMode lock_mode, table_oid_t oid, RID rid) -> void {
    auto row_lock_set = GetRowLockSet(txn, lock_mode);
    auto pair = row_lock_set->find(oid);
    if (pair != row_lock_set->end()) {
      pair->second.erase(rid);
      if (pair->second.empty()) {
        row_lock_set->erase(oid);
      }
    }
  }

  auto UpgradeTableLockSet(Transaction *txn, LockMode old_lock_mode, LockMode lock_mode, const table_oid_t &oid)
      -> void {
    // erase table from txn's original lock set
    auto old_lock_set = GetTableLockSet(txn, old_lock_mode);
    old_lock_set->erase(oid);

    // add table to txn's new lock set
    auto lock_set = GetTableLockSet(txn, lock_mode);
    lock_set->emplace(oid);
  }

  auto UpgradeRowLockSet(Transaction *txn, LockMode old_lock_mode, LockMode lock_mode, const table_oid_t &oid,
                         const RID &rid) -> void {
    // erase table from txn's original lock set
    auto old_lock_set = GetRowLockSet(txn, old_lock_mode);
    auto old_locked_rows = old_lock_set->find(oid);
    if (old_locked_rows != old_lock_set->end()) {
      old_locked_rows->second.erase(rid);
      if (old_locked_rows->second.empty()) {
        old_lock_set->erase(oid);
      }
    }

    // add table to txn's new lock set
    auto lock_set = GetRowLockSet(txn, lock_mode);
    auto locked_rows = lock_set->find(oid);
    if (locked_rows != lock_set->end()) {
      locked_rows->second.emplace(rid);
    } else {
      lock_set->emplace(oid, std::unordered_set<RID>{rid});
    }
  }
  /** End Book Keeping Helpers */

  auto UpgradeLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool;

  auto UpgradeLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  void GrantNewLocksIfPossible(LockRequestQueue *req_queue);

  /**
   * @brief Checks if a transaction can take lock based on its isolation level and state
   *
   * @param txn
   * @param lock_mode
   * @return true
   * @return false
   */
  auto CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        // all locks are allowed during the growing state
        if (txn->GetState() == TransactionState::GROWING) {
          return true;
        }
        break;

      case IsolationLevel::READ_COMMITTED:
        // all locks are allowed during the growing state
        if (txn->GetState() == TransactionState::GROWING) {
          return true;
        }

        // IS/S lock are allowed during shrinking state
        if (txn->GetState() == TransactionState::SHRINKING &&
            (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED)) {
          return true;
        }
        break;

      case IsolationLevel::READ_UNCOMMITTED:
        // S/IS/SIX locks are not allowed at all
        if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
            lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
          txn->SetState(TransactionState::ABORTED);

          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
          return false;
        }

        // X/IX are allowed during growing state
        if (txn->GetState() == TransactionState::GROWING) {
          return true;
        }
        break;
    }

    // abort the transaction otherwise
    txn->SetState(TransactionState::ABORTED);

    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  /**
   * @brief Checks if two locks are compatible
   * S -> [IS, S]
   * IS -> [IS, IX, S, SIX]
   * IX -> [IS, IX]
   * SIX -> [IS]
   * @param l1 the lock which T1 holds on the target resource
   * @param l2 the lock which T2 wants on the target resource
   * @return true
   * @return false
   */
  auto AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
    switch (l1) {
      case LockMode::SHARED:
        return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED;
      case LockMode::INTENTION_SHARED:
        return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE || l2 == LockMode::SHARED ||
               l2 == LockMode::SHARED_INTENTION_EXCLUSIVE;
      case LockMode::INTENTION_EXCLUSIVE:
        return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        return l2 == LockMode::INTENTION_SHARED;
      case LockMode::EXCLUSIVE:
      default:
        return false;
    }
  }

  /**
   * @brief checks if the lock upgrade is possible
   * namely, check if it is one of the following
   * S -> [X, SIX]
   * IS -> [S, X, IX, SIX]
   * IX -> [X, SIX]
   * SIX -> [X]
   * @param curr_lock_mode
   * @param requested_lock_mode
   * @return true
   * @return false
   */
  auto CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
    switch (curr_lock_mode) {
      case LockMode::SHARED:
        return requested_lock_mode == LockMode::EXCLUSIVE ||
               requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
      case LockMode::INTENTION_SHARED:
        return requested_lock_mode == LockMode::SHARED || requested_lock_mode == LockMode::EXCLUSIVE ||
               requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
               requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
      case LockMode::INTENTION_EXCLUSIVE:
        return requested_lock_mode == LockMode::EXCLUSIVE ||
               requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        return requested_lock_mode == LockMode::EXCLUSIVE;
      case LockMode::EXCLUSIVE:
      default:
        return false;
    }
  }

  auto FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                 std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool;
  void UnlockAll();

  /** Graph API Helper */

  void PrintGraph() {
    std::cout << "================ start_waits_for_graph ================" << std::endl;
    std::cout << "size: " << waits_for_.size() << " " << std::endl;
    for (auto &pair : waits_for_) {
      std::cout << pair.first << " -> ";
      for (auto t2 : pair.second) {
        std::cout << t2 << " ";
      }
      std::cout << std::endl;
    }
    std::cout << "================ end___waits_for_graph ================" << std::endl;
  }

  /** Structure that holds lock requests for a given table oid */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;
};

}  // namespace bustub

template <>
struct fmt::formatter<bustub::LockManager::LockMode> : formatter<std::string_view> {
  // parse is inherited from formatter<string_view>.
  template <typename FormatContext>
  auto format(bustub::LockManager::LockMode x, FormatContext &ctx) const {
    string_view name = "unknown";
    switch (x) {
      case bustub::LockManager::LockMode::EXCLUSIVE:
        name = "EXCLUSIVE";
        break;
      case bustub::LockManager::LockMode::INTENTION_EXCLUSIVE:
        name = "INTENTION_EXCLUSIVE";
        break;
      case bustub::LockManager::LockMode::SHARED:
        name = "SHARED";
        break;
      case bustub::LockManager::LockMode::INTENTION_SHARED:
        name = "INTENTION_SHARED";
        break;
      case bustub::LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
        name = "SHARED_INTENTION_EXCLUSIVE";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
