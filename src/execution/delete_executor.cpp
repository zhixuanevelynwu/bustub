//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple t;
  RID r;
  auto catalog = exec_ctx_->GetCatalog();
  auto table_meta = catalog->GetTable(plan_->TableOid());
  auto indexes = catalog->GetTableIndexes(table_meta->name_);
  // Delete tuples from table
  int count = 0;
  while (child_executor_->Next(&t, &r)) {
    const TupleMeta old_meta{INVALID_TXN_ID, INVALID_TXN_ID, true};
    table_meta->table_->UpdateTupleMeta(old_meta, r);
    // Update indexes (if any)
    for (auto index_meta : indexes) {
      auto key = t.KeyFromTuple(table_meta->schema_, index_meta->key_schema_, index_meta->index_->GetKeyAttrs());
      index_meta->index_->DeleteEntry(key, r, nullptr);
    }
    count++;
  }
  // Emit number of inserted rows
  std::vector<Value> vec(1, Value(INTEGER, count));
  const std::vector<Column> cols(1, Column("count", INTEGER));
  const auto s = Schema(cols);
  *tuple = Tuple(vec, &s);
  if (!deleted_) {
    deleted_ = true;
    return true;
  }
  return false;
}

}  // namespace bustub
