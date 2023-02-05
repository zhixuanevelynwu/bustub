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

namespace bustub {

auto inf = std::numeric_limits<size_t>::max();

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * @brief Evict the frame with largest backward k-distance compared to all other evictable frames being tracked by the
 * Replacer. Backward k-distance is computed as the difference in time between current timestamp and the timestamp of
 * kth previous access. Store the frame id in the output parameter and return True. If there are no evictable frames
 * return False.
 * When multiple frames have +inf backward k-distance, the replacer evicts the frame with the earliest overall timestamp
 * (i.e., the frame whose least-recent recorded access is the overall least recent access, overall, out of all frames).
 *
 * @param frame_id
 * @return true
 * @return false
 */
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // Check if there's any frames to evict
  if (node_store_.empty() || replacer_size_ == 0) {
    return false;
  }
  // Keep track of the frame with the argest backward k-distance
  auto pair = std::make_shared<std::pair<size_t, LRUKNode>>();
  // Walk through the node map to compute the backward k-distance of each node
  for (auto const &[id, node] : node_store_) {
    // Only look at evictable frames
    if (!node.is_evictable_) {
      continue;
    }
    // For frames with less than k records, set k_dist to +inf
    auto k_dist = inf;
    if (node.k_ >= k_) {
      // Find the kth previous access
      auto iter = node.history_.end();
      std::advance(iter, -k_);
      // Compute backward k-distance
      k_dist = current_timestamp_ - *iter;
    }
    // Update frame
    if (pair == nullptr || (k_dist == pair->first && *(pair->second.history_.begin()) > *(node.history_.begin())) ||
        pair->first < k_dist) {
      pair = std::make_shared<std::pair<size_t, LRUKNode>>(k_dist, node);
    }
  }
  // Found the frame -> write down its id and evict it
  if (pair != nullptr) {
    *frame_id = pair->second.fid_;
    node_store_.erase(*frame_id);
    replacer_size_--;
    return true;
  }
  // No evictable frame found
  return false;
}

/**
 * @brief Record that given frame id is accessed at current timestamp. This method should be called after a page is
 * pinned in the BufferPoolManager.
 *
 * @param frame_id
 * @param access_type
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub
