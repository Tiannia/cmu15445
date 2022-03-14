//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/aggregation_executor.h"

#include <memory>
#include <vector>

#include "execution/expressions/aggregate_value_expression.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(
    ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {
  auto key = std::move(aht_.GenerateInitialAggregateValue().aggregates_);
  bool is_group_by = !plan_->GetGroupBys().empty();
  const auto &agg_exprs = plan_->GetAggregates();
  Tuple tuple;
  RID rid;
  child_->Init();
  while (child_->Next(&tuple, &rid)) {
    if (is_group_by) {
      const auto &group_bys = plan_->GetGroupBys();
      key.clear();
      key.reserve(group_bys.size());
      for (auto group_by : group_bys) {
        key.push_back(group_by->Evaluate(&tuple, child_->GetOutputSchema()));
      }
    }
    std::vector<Value> values;
    values.reserve(agg_exprs.size());
    for (auto agg_expr : agg_exprs) {
      values.push_back(agg_expr->Evaluate(&tuple, child_->GetOutputSchema()));
    }
    aht_.InsertCombine(AggregateKey{key}, AggregateValue{values});
  }
}

//Because we insert {key, value} into aht_, so the iterator should be inited again.
void AggregationExecutor::Init() { aht_iterator_ = aht_.Begin(); }

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  Value value{INVALID};
  while (aht_iterator_ != aht_.End()) { //Traversal hash table
    auto temp_iter = aht_iterator_;
    ++aht_iterator_;
    if (plan_->GetHaving() != nullptr) {
      value = plan_->GetHaving()->EvaluateAggregate(
          temp_iter.Key().group_bys_, temp_iter.Val().aggregates_);
    }
    if (value.GetAs<bool>()) {
      std::vector<Value> values;
      values.reserve(plan_->OutputSchema()->GetColumnCount());
      for (const auto &column : plan_->OutputSchema()->GetColumns()) {
        auto agg_expr = reinterpret_cast<const AggregateValueExpression *>(
            column.GetExpr());
        values.push_back(agg_expr->EvaluateAggregate(
            temp_iter.Key().group_bys_, temp_iter.Val().aggregates_));
      }
      *tuple = Tuple(values, plan_->OutputSchema());
      return true;
    }
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const {
  return child_.get();
}

}  // namespace bustub
