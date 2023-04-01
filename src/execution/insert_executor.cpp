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
    : AbstractExecutor(exec_ctx), plan_(plan), child_exec_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_exec_->Init(); }

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
 * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
 */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int count = 0;
  Tuple t;
  RID r;
  auto table_meta = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  while (child_exec_->Next(&t, &r)) {
    TupleMeta m;
    m.is_deleted_ = false;
    table_meta->table_->InsertTuple(m, t);
    count++;
  }
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
