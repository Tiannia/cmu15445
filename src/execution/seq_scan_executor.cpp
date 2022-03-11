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

#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx,
                                 const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(nullptr, RID{}, nullptr),
      end_(nullptr, RID{}, nullptr) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid());
  
  uint32_t columnCount = plan_->OutputSchema()->GetColumnCount();
  out_schema_idx_.reserve(columnCount);
  try {
    for (uint32_t i = 0; i < columnCount; ++i) {
      auto col_name = plan_->OutputSchema()->GetColumn(i).GetName();
      out_schema_idx_.push_back(table_info_->schema_.GetColIdx(col_name));
    }
  } catch (const std::logic_error &error) {
    for (uint32_t i = 0; i < columnCount; ++i) {
      out_schema_idx_.push_back(i);
    }
  }

  if (plan_->GetPredicate() != nullptr) {
    predicate_ = plan_->GetPredicate();
  } else {
    is_alloc_ = true;
    predicate_ =
        new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
  }
}

SeqScanExecutor::~SeqScanExecutor() {
  if (is_alloc_) {
    delete predicate_;
  }
  predicate_ = nullptr;
}

void SeqScanExecutor::Init() {
  iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table_info_->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != end_) {
    auto temp = iter_++;
    auto value = predicate_->Evaluate(&(*temp), &table_info_->schema_);
    if (value.GetAs<bool>()) {
      // only keep the columns of the output schema
      std::vector<Value> res;
      res.reserve(out_schema_idx_.size());
      for (auto idx : out_schema_idx_) {
        res.push_back(temp->GetValue(&table_info_->schema_, idx));
      }
      *tuple = Tuple(res, plan_->OutputSchema());
      *rid = temp->GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
