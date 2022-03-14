//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(
    ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&left_executor,
    std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_executor)),
      right_child_executor_(std::move(right_executor)) {
  if (plan_->Predicate() != nullptr) {
    predicate_ = plan_->Predicate();
  } else {
    is_alloc_ = true;
    predicate_ =
        new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
  }
}

NestedLoopJoinExecutor::~NestedLoopJoinExecutor() {
  if (is_alloc_) {
    delete predicate_;
  }
  predicate_ = nullptr;
}

void NestedLoopJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();
  is_left_selected_ = left_child_executor_->Next(&left_tuple_, &left_rid_);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!is_left_selected_) {
    return false;
  }
  Tuple right_tuple;
  RID right_rid;
  while (true) {
    while (!right_child_executor_->Next(&right_tuple, &right_rid)) {
			//as right end of loop, we should move left point to next tuple and start again.
      if (!left_child_executor_->Next(&left_tuple_, &left_rid_)) {
        return false;
      }
      right_child_executor_->Init();
    }
    auto value = predicate_->EvaluateJoin(
        &left_tuple_, plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
        plan_->GetRightPlan()->OutputSchema());

    if (value.GetAs<bool>()) {
      std::vector<Value> values;
      values.reserve(plan_->OutputSchema()->GetColumnCount());
      for (const auto &column : plan_->OutputSchema()->GetColumns()) {
        auto column_expr =
            reinterpret_cast<const ColumnValueExpression *>(column.GetExpr());
        if (column_expr->GetTupleIdx() == 0) {
          values.push_back(left_tuple_.GetValue(
              plan_->GetLeftPlan()->OutputSchema(), column_expr->GetColIdx()));
        } else {
          values.push_back(right_tuple.GetValue(
              plan_->GetRightPlan()->OutputSchema(), column_expr->GetColIdx()));
        }
      }
      *tuple = Tuple(values, plan_->OutputSchema());
      *rid = left_tuple_.GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
