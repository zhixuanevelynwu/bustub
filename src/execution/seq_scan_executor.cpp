//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_->MakeEagerIterator()) {}

void SeqScanExecutor::Init() {
  auto txn = exec_ctx_->GetTransaction();
  auto lock_mgr = exec_ctx_->GetLockManager();
  auto isolation = txn->GetIsolationLevel();
  auto oid = plan_->GetTableOid();
  if (exec_ctx_->IsDelete()) {
    if (!lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid)) {
      throw ExecutionException("seqscan <delete>: failed acquiring IX lock on table");
    }
  } else if (isolation != IsolationLevel::READ_UNCOMMITTED) {
    if (!lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, oid)) {
      throw ExecutionException("seqscan: failed acquiring IS lock on table");
    }
  }
}

/*
Get the current position of the table iterator.
Lock the tuple as needed for the isolation level.
Fetch the tuple. Check tuple meta, and if you have implemented filter pushdown to scan, check the predicate.
If the tuple should not be read by this transaction, force unlock the row. Otherwise, unlock the row as needed for the
isolation level. If the current operation is delete (by checking executor context IsDelete(), which will be set to true
for DELETE and UPDATE), you should assume all tuples scanned will be deleted, and you should take X locks on the table
and tuple as necessary in step 2.
*/
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto txn = exec_ctx_->GetTransaction();
  auto lock_mgr = exec_ctx_->GetLockManager();
  auto isolation = txn->GetIsolationLevel();
  auto oid = plan_->GetTableOid();

  while (!iter_.IsEnd()) {
    auto r = iter_.GetRID();

    // lock based on context
    if (exec_ctx_->IsDelete()) {  // take X lock if current op is delete
      if (!lock_mgr->LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, r)) {
        throw ExecutionException("seqscan <delete>: failed acquiring X lock");
      }
    } else if (isolation != IsolationLevel::READ_UNCOMMITTED) {  // Slock the tuple except for READ_UNCOMMIT
      if (!lock_mgr->LockRow(txn, LockManager::LockMode::SHARED, oid, r)) {
        throw ExecutionException("seqscan: failed acquiring S lock");
      }
    }

    // store tuple data
    auto [m, t] = iter_.GetTuple();
    ++iter_;

    // tuple deleted -> force unlock
    if (m.is_deleted_) {
      lock_mgr->UnlockRow(txn, oid, r);
    } else {
      // release lock immediately for READ_UNCOMMITED
      if (isolation == IsolationLevel::READ_COMMITTED) {
        lock_mgr->UnlockRow(txn, oid, r);
      }
      // write tuple to output
      *tuple = t;
      *rid = r;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
