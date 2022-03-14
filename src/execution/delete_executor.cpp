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

#include "execution/executors/delete_executor.h"

#include <memory>

namespace bustub {

DeleteExecutor::DeleteExecutor(
    ExecutorContext *exec_ctx, const DeletePlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_array_ =
      exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool is_deleted = false;
  if (child_executor_->Next(tuple, rid)) {
    if (table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      is_deleted = true;
    }
  }
  if (is_deleted && !index_info_array_.empty()) {
    for (auto index_info : index_info_array_) {
      const auto index_key =
          tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                              index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(index_key, *rid,
                                      exec_ctx_->GetTransaction());
    }
  }
  return is_deleted;
}

}  // namespace bustub
