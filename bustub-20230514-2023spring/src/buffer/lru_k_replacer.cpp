//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (curr_size_ == 0) {  // no evictable frames
    return false;
  }

  bool remove_from_default_list = false;
  bool remove_from_k_list = false;
  if (!default_list_.empty()) {
    remove_from_default_list = RemoveNode(frame_id, &default_list_, &default_map_);
  }

  if (!remove_from_default_list && !k_list_.empty()) {
    remove_from_k_list = RemoveNode(frame_id, &k_list_, &k_map_);
  }

  // PrintLists();

  LOG_DEBUG("Evicting frame_id: %d", *frame_id);

  return remove_from_default_list || remove_from_k_list;
}

// larger TS is in the front of the list, smaller TS is in the back of the list
// we need to remove the smallest TS in the list (max(Current TS - k_TS))
auto LRUKReplacer::RemoveNode(frame_id_t *frame_id, std::list<LRUKNode *> *lst,
                              std::unordered_map<frame_id_t, LRUKNode *> *map) -> bool {
  auto r_it = lst->rbegin();
  while (r_it != lst->rend()) {
    if ((*r_it)->GetEvictable()) {  // backward iterate to find an evictable frame
      LRUKNode *node = (*r_it);
      (*frame_id) = node->GetFrameId();
      map->erase((*frame_id));
      lst->erase(std::next(r_it).base());
      --curr_size_;

      delete node;
      return true;
    }
    ++r_it;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id > static_cast<int32_t>(replacer_size_)) {
    return;
  }

  auto default_iter = default_map_.find(frame_id);
  auto k_iter = k_map_.find(frame_id);
  if (default_iter == default_map_.end() && k_iter == k_map_.end()) {  // create a new node
    auto node = new LRUKNode(frame_id, k_);
    node->InsertTimeStamp(current_timestamp_);
    default_map_[frame_id] = node;
    default_list_.push_front(node);
  } else {
    if (default_iter != default_map_.end()) {  // in the default_list_
      auto node = default_map_[frame_id];
      node->InsertTimeStamp(current_timestamp_);
      default_list_.remove(node);

      if (node->GetNumOfReferences() >= k_) {
        node->UpdateKDistance();
        default_map_.erase(frame_id);
        k_map_[frame_id] = node;
        InsertKNode(node);
      } else {  // push the node to the front
        default_list_.push_front(node);
      }

    } else {  // in the k_list_
      LRUKNode *node = k_map_[frame_id];
      node->InsertTimeStamp(current_timestamp_);
      node->UpdateKDistance();
      k_list_.remove(node);
      InsertKNode(node);
    }
  }
  ++current_timestamp_;

  LOG_DEBUG("Record Accessing frame_id: %d", frame_id);

  PrintLists();
}

// start the scan from the front of the list
void LRUKReplacer::InsertKNode(LRUKNode *node) {
  if (k_list_.empty()) {
    k_list_.push_back(node);
    return;
  }
  auto it = k_list_.begin();
  size_t k_dist = node->GetKDistance();
  while (it != k_list_.end() && k_dist < (*it)->GetKDistance()) {
    ++it;
  }
  k_list_.insert(it, node);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  auto default_iter = default_map_.find(frame_id);
  auto k_iter = k_map_.find(frame_id);

  if (default_iter == default_map_.end() && k_iter == k_map_.end()) {
    throw std::runtime_error("Frame_id does not exist in the replacer");
  }

  // node will either in the default_map_ or k_map_
  LRUKNode *node = default_iter != default_map_.end() ? default_map_[frame_id] : k_map_[frame_id];

  // LRUKNode *node = nullptr;
  // if (default_iter != default_map_.end()) {
  //   node = default_map_[frame_id];
  // } else if (k_iter != k_map_.end()) {
  //   node = k_map_[frame_id];
  // }

  LOG_DEBUG("Set evictable frame_id: %d from %d to %d", frame_id, node->GetEvictable(), set_evictable);

  if (node->GetEvictable() && !set_evictable) {
    --curr_size_;
  } else if (!node->GetEvictable() && set_evictable) {
    ++curr_size_;
  }

  node->SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto default_iter = default_map_.find(frame_id);
  auto k_iter = k_map_.find(frame_id);
  if (default_iter == default_map_.end() && k_iter == k_map_.end()) {
    return;
  }

  LRUKNode *node = nullptr;
  if (default_iter != default_map_.end()) {
    node = default_map_[frame_id];
    if (!node->GetEvictable()) {
      throw std::runtime_error("Cannot remove a non-evictable frame");
    }
    default_map_.erase(frame_id);
    default_list_.remove(node);

  } else if (k_iter != k_map_.end()) {
    node = k_map_[frame_id];
    if (!node->GetEvictable()) {
      throw std::runtime_error("Cannot remove a non-evictable frame");
    }
    k_map_.erase(frame_id);
    k_list_.remove(node);
  }

  LOG_DEBUG("Removing frame_id: %d", frame_id);

  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

void LRUKReplacer::PrintLists() {
  LOG_DEBUG("Num of evictable frames: %zu", curr_size_);
  // LOG_DEBUG("Default List: (type: {frame_id}{is_evictable}[distance, history])");
  std::string str1;
  for (LRUKNode *node : default_list_) {
    if (node->GetEvictable()) {
      str1 += "{" + std::to_string(node->GetFrameId()) + "}√: " + std::to_string(node->GetKDistance()) +
              node->GetHistory() + " ";
    } else {
      str1 += "{" + std::to_string(node->GetFrameId()) + "}x: " + std::to_string(node->GetKDistance()) +
              node->GetHistory() + " ";
    }
  }
  LOG_DEBUG("Default List: %s", str1.c_str());

  std::string str2;
  for (LRUKNode *node : k_list_) {
    if (node->GetEvictable()) {
      str2 += "{" + std::to_string(node->GetFrameId()) + "}√: " + std::to_string(node->GetKDistance()) +
              node->GetHistory() + " ";
    } else {
      str2 += "{" + std::to_string(node->GetFrameId()) + "}x: " + std::to_string(node->GetKDistance()) +
              node->GetHistory() + " ";
    }
  }
  LOG_DEBUG("K List: %s\n", str2.c_str());
}

}  // namespace bustub
