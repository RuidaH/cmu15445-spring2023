#include "execution/executors/topn_executor.h"
#include <algorithm>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  std::cout << plan_->ToString() << std::endl;

  cur_ = 0;

  if (sorted_) {
    return;
  }

  child_executor_->Init();
  heap_.clear();
  auto &order_bys = plan_->GetOrderBy();
  auto &schema = child_executor_->GetOutputSchema();

  auto comp = [&order_bys, &schema](std::pair<Tuple, RID> &t1, std::pair<Tuple, RID> &t2) -> bool {
    for (const auto &order_by : order_bys) {
      Value val1 = order_by.second->Evaluate(&t1.first, schema);
      Value val2 = order_by.second->Evaluate(&t2.first, schema);

      if (val1.CompareEquals(val2) == CmpBool::CmpTrue) {
        continue;
      }

      return order_by.first == OrderByType::DEFAULT || order_by.first == OrderByType::ASC
                 ? val1.CompareLessThan(val2) == CmpBool::CmpTrue
                 : val1.CompareGreaterThan(val2) == CmpBool::CmpTrue;
    }
    return false;
  };

  Tuple tuple;
  RID rid;
  size_t limit = GetNumInHeap();
  while (child_executor_->Next(&tuple, &rid)) {
    auto tmp = std::make_pair(tuple, rid);
    if (heap_.size() == limit) {
      if (comp(tmp, heap_[0])) {
        std::pop_heap(heap_.begin(), heap_.end(), comp);
        heap_[limit - 1] = tmp;
        std::push_heap(heap_.begin(), heap_.end(), comp);
      }
    } else {
      heap_.emplace_back(tuple, rid);
      std::push_heap(heap_.begin(), heap_.end(), comp);
    }
  }

  sorted_ = true;
  std::sort_heap(heap_.begin(), heap_.end(), comp);
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_ >= heap_.size()) {
    return false;
  }
  auto &result = heap_.at(cur_);
  *tuple = result.first;
  *rid = result.second;
  ++cur_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return plan_->GetN(); };

}  // namespace bustub
