//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  ~SeqScanExecutor() override;

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the sequential scan */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
  /** Metadata identifying the table that should be seqscan */
  TableInfo* table_info_{Catalog::NULL_TABLE_INFO};
  /** Point to the position of the tuple currently being scanned */
  TableIterator iter_;
  /** Point to the next position of the last tuple */  
  TableIterator end_;
  /** Determine whether to return the tuples */
  mutable const AbstractExpression *predicate_{nullptr};
  /** Whether to allocate memory for the predicate_ */
  bool is_alloc_{false};
  /** The idx of each column of the out schema in the origin schema */
  std::vector<uint32_t> out_schema_idx_;
};
}  // namespace bustub
