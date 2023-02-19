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

/**
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // Initialize node store
  for (size_t i = 0; i < num_frames; i++) {
    node_store_[i] = LRUKNode();
    node_store_[i].fid_ = i;
  }
}

/**
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
 * based on LRU.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @param[out] frame_id id of frame that is evicted.
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // Check if there're any frames available
  if (node_store_.empty() || curr_size_ == 0) {
    return false;
  }
  // Keep track of the frame with the largest backward k-distance
  auto pair = std::make_shared<std::pair<size_t, LRUKNode *>>(-1, nullptr);
  // Walk through the node map to compute the backward k-distance of each node
  for (auto &[id, node] : node_store_) {
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
    if (pair->first < k_dist || pair->second == nullptr ||
        (k_dist == pair->first && *(pair->second->history_.begin()) > *(node.history_.begin()))) {
      pair->first = k_dist;
      pair->second = &node;
    }
  }
  // Found the frame
  if (pair != nullptr) {
    pair->second->Reset();
    *frame_id = pair->second->fid_;
    curr_size_--;
    return true;
  }
  // No evictable frame found
  return false;
}

/**
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT((size_t)frame_id < replacer_size_, "Invalid frame id");
  // Find the current frame and update its history
  auto pair = node_store_.find(frame_id);
  BUSTUB_ASSERT(pair->second.k_ == 0, "Frame not found");
  pair->second.history_.emplace_back(current_timestamp_);
  pair->second.k_++;
  current_timestamp_++;
}

/**
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT((size_t)frame_id < replacer_size_, "Invalid frame id");
  // Find and update the frame
  auto pair = node_store_.find(frame_id);
  if (pair->second.k_ == 0) {
    return;  // Node not initialized -> not found
  }
  if (pair->second.is_evictable_ && !set_evictable) {
    curr_size_--;
  } else if (!pair->second.is_evictable_ && set_evictable) {
    curr_size_++;
  }
  pair->second.is_evictable_ = set_evictable;
}

/**
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT((size_t)frame_id < replacer_size_, "Invalid frame id");
  // Find the current frame
  auto pair = node_store_.find(frame_id);
  // If specified frame is not found, directly return from this function.
  if (pair->second.k_ == 0) {
    return;  // Node not initialized -> not found
  }
  BUSTUB_ASSERT(pair->second.is_evictable_, "Frame is not evictable");
  // Remove the frame's access history
  pair->second.Reset();
  // Decrement replacer size
  curr_size_--;
}

/**
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
