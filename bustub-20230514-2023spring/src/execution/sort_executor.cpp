#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), sorted_(false) {
      std::cout << "Create sort executor" << std::endl;
    }

void SortExecutor::Init() {
  cur_ = 0;

  std::cout << "Sort Init()" << std::endl;
  
  if (sorted_) {  // 不需要重新创建 ordered_tuple_, 但是记得不要把 Init() 放到前面
    return;
  }

  child_executor_->Init();
  ordered_tuples_.clear();

  auto &order_bys = plan_->GetOrderBy();
  auto &schema = child_executor_->GetOutputSchema();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    ordered_tuples_.emplace_back(tuple, rid);
  }

  auto sort_by = [&order_bys, &schema] (std::pair<Tuple, RID>& t1, std::pair<Tuple, RID>& t2) -> bool {
    for (const auto & order_by : order_bys) {
      Value val1 = order_by.second->Evaluate(&t1.first, schema);
      Value val2 = order_by.second->Evaluate(&t2.first, schema);

      if (val1.CompareEquals(val2) == CmpBool::CmpTrue) {  // continue if they equal in this val
        continue;
      }

      return order_by.first == OrderByType::DEFAULT || order_by.first == OrderByType::ASC
              ? val1.CompareLessThan(val2) == CmpBool::CmpTrue
              : val1.CompareGreaterThan(val2) == CmpBool::CmpTrue;
    }
    return false; // return false if they equal in every val (namely themself)
  };
  sorted_ = true;
  std::sort(ordered_tuples_.begin(), ordered_tuples_.end(), sort_by);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_ >= ordered_tuples_.size()) {
    return false;
  }
  auto & result = ordered_tuples_.at(cur_);
  *tuple = result.first;
  *rid = result.second;
  ++cur_;
  return true;
}

}  // namespace bustub

/**
 *  for (const auto &order_by : plan_->GetOrderBy()) {
      // 这里的 expression 就是 Column Value Expression (也就是说指的是哪一列)
      // 每一个用来排序的列有 asc / desc
      std::cout << "Expression: " << order_by.second->ToString() << std::endl; 
    }
*/