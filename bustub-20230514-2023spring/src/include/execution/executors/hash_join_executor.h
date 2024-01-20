//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct JoinKey {
  std::vector<Value> join_keys_;
  JoinKey() = default;

  auto operator==(const JoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.join_keys_.size(); ++i) {
      if (join_keys_[i].CompareEquals(other.join_keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {
/** Implements std::hash on JoinKey */
template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.join_keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {

struct TupleBucket {
  std::vector<Tuple> tuple_bucket_;
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  void InsertJoinKey(const JoinKey &join_key, Tuple &tuple) {
    not_joined_.insert(join_key);
    if (ht_.count(join_key) == 0) {
      ht_.emplace(join_key, TupleBucket{{tuple}});  // 这里用 emplace 可以提高效率
      return;
    }
    ht_[join_key].tuple_bucket_.emplace_back(tuple);
  }

  // std::unordered_map<JoinKey, TupleBucket>::const_iterator
  auto GetTupleBucket(const JoinKey &join_key) -> std::optional<std::vector<Tuple>> {
    auto iter = ht_.find(join_key);
    if (iter != ht_.end()) {
      return iter->second.tuple_bucket_;
    }
    return std::nullopt;
  }

  auto GenerateJoinKey(const AbstractPlanNodeRef &plan, const std::vector<AbstractExpressionRef> &exprs, Tuple &tuple)
      -> JoinKey {
    JoinKey join_keys;
    for (auto &expr : exprs) {
      // std::cout << expr->Evaluate(&tuple, left_plan->OutputSchema()).ToString() << std::endl;
      Value left_key = expr->Evaluate(&tuple, plan->OutputSchema());
      join_keys.join_keys_.emplace_back(left_key);
    }
    return join_keys;
  }

 private:
  void OutputTuple(const Schema &left_table_schema, const Schema &right_table_schema, Tuple *left_tuple, Tuple *tuple,
                   bool matched);

  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;

  std::unordered_map<JoinKey, TupleBucket> ht_{};
  std::unordered_set<JoinKey> not_joined_;
  std::optional<std::vector<Tuple>> tuple_bucket_;

  // Tuple *right_tuple_;
  Tuple right_tuple_;
  bool right_finished_{false};
  bool build_{false};
  u_int32_t cur_index_{0};
};

}  // namespace bustub
