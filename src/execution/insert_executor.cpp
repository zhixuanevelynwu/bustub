//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

/**
 * @brief The InsertExecutor inserts tuples into a table and updates any affected indexes. It has exactly one child
 * producing values to be inserted into the table. The planner will ensure that the values have the same schema as the
 * table. The executor will produce a single tuple of integer type as the output, indicating how many rows have been
 * inserted into the table. Remember to update indexes when inserting into the table, if there are indexes associated
 * with it.
 *
 */
namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
 */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple t;
  RID r;
  auto catalog = exec_ctx_->GetCatalog();
  auto table_meta = catalog->GetTable(plan_->TableOid());
  auto indexes = catalog->GetTableIndexes(table_meta->name_);
  // Insert tuples into table
  int count = 0;
  while (child_executor_->Next(&t, &r)) {
    const TupleMeta tuple_meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    const auto new_rid = table_meta->table_->InsertTuple(tuple_meta, t);
    BUSTUB_ASSERT(new_rid, "InsertTuple() should not return nullptr.");
    // Update indexes (if any)
    for (auto index_meta : indexes) {
      auto key = t.KeyFromTuple(table_meta->schema_, index_meta->key_schema_, index_meta->index_->GetKeyAttrs());
      index_meta->index_->InsertEntry(key, *new_rid, nullptr);
    }
    count++;
  }
  // Emit number of inserted rows
  std::vector<Value> vec(1, Value(INTEGER, count));
  const std::vector<Column> cols(1, Column("count", INTEGER));
  const auto s = Schema(cols);
  *tuple = Tuple(vec, &s);
  if (!inserted_) {
    inserted_ = true;
    return true;
  }
  return false;
}
}  // namespace bustub
