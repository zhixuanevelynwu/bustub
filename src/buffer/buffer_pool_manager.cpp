//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <memory>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

/**
 * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
 * are currently in use and not evictable (in another word, pinned).
 *
 * You should pick the replacement frame from either the free list or the replacer (always find from the free list
 * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
 * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
 *
 * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
 * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
 * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
 *
 * @param[out] page_id id of created page
 * @return nullptr if no new pages could be created, otherwise pointer to new page
 */
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // If no free frame in buffer and no frame can be evicted
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  // Pick replacement frame
  // First check if there's any free frames to use
  auto frame_id = PickReplacementFrame();
  // Create a new page in the buffer pool
  pages_[frame_id].ResetMemory();
  *page_id = AllocatePage();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;  // Since this is a new page, pin count should be 1
  // Update it in the page table
  page_table_[*page_id] = frame_id;
  // Record the access history
  replacer_->RecordAccess(frame_id);
  return &pages_[frame_id];
}

/**
 * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
 * but all frames are currently in use and not evictable (in another word, pinned).
 *
 * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
 * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
 * and replace the old page in the frame. Similar to NewPage(), if the old page is dirty, you need to write it back
 * to disk and update the metadata of the new page
 *
 * In addition, remember to disable eviction and record the access history of the frame like you did for NewPage().
 *
 * @param page_id id of page to be fetched
 * @param access_type type of access to the page, only needed for leaderboard tests.
 * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
 */
auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // Search for the page in buffer pool
  auto pair = page_table_.find(page_id);
  if (pair != page_table_.end()) {
    pages_[pair->second].pin_count_++;
    return &pages_[pair->second];
  }
  // If no free frame in buffer and no frame can be evicted
  if (free_list_.empty() && replacer_->Size() <= 0) {
    return nullptr;
  }
  // Page not in buffer -> pick a replacement frame
  auto frame_id = PickReplacementFrame();
  auto old_page_id = pages_[frame_id].page_id_;
  // Find the old page and update its metadata
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  // Update the new page in the page table
  page_table_.erase(old_page_id);
  page_table_[page_id] = frame_id;
  pages_[frame_id].pin_count_++;
  // Write disk content to buffer pool
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  // Record the access history of the frame
  replacer_->RecordAccess(frame_id);
  return &pages_[frame_id];
}

/**
 * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its
 *  count is already
 * 0, return false.
 *
 * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
 * Also, set the dirty flag on the page to indicate if the page was modified.
 *
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @param access_type type of access to the page, only needed for leaderboard tests.
 * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
 */
auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // Search for the page in buffer pool
  auto pair = page_table_.find(page_id);
  if (pair != page_table_.end()) {
    auto frame_id = pair->second;
    // If the pin count is already 0, return false
    if (pages_[frame_id].pin_count_ <= 0) {
      return false;
    }
    // Otherwise, decrement the page's pin count and set its dirty flag
    pages_[frame_id].pin_count_--;
    pages_[frame_id].is_dirty_ = is_dirty;
    // If the pin count reaches 0, set the frame as evictable
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    return true;
  }
  // Page not in the buffer pool
  return false;
}

/**
 * @brief Flush the target page to disk.
 *
 * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
 * Unset the dirty flag of the page after flushing.
 *
 * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
 * @return false if the page could not be found in the page table, true otherwise
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "Invalid page id");
  // Find the page in the buffer pool
  auto pair = page_table_.find(page_id);
  if (pair != page_table_.end()) {
    // Write to the disk then reset the dirty flag
    auto frame_id = pair->second;
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
    return true;
  }
  // Page not found
  return false;
}

/**
 * @brief Flush all the pages in the buffer pool to disk.
 */
void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      // Write to the disk then reset the dirty flag
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
      pages_[i].is_dirty_ = false;
    }
  }
}

/**
 * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
 * page is pinned and cannot be deleted, return false immediately.
 *
 * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
 * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
 * imitate freeing the page on the disk.
 *
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // Find the page in the buffer pool
  auto pair = page_table_.find(page_id);
  if (pair != page_table_.end()) {
    auto frame_id = pair->second;
    // If the page is pinned, return false immediately
    if (pages_[frame_id].pin_count_ > 0) {
      return false;
    }
    // Delete the page from the page table
    page_table_.erase(page_id);
    // Stop traking the frame in the replacer and add the frame back to the free list
    replacer_->Remove(frame_id);
    free_list_.emplace_back(frame_id);
    // Reset the page's memory and metadata
    pages_[frame_id].ResetMemory();
    // Free the page on the disk
    DeallocatePage(page_id);
    return true;
  }
  // Page not found -> do nothing and return true
  return true;
}

/**
 * @brief Allocate a page on disk. Caller should acquire the latch before calling this function.
 * @return the id of the allocated page
 */
auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

/**
 * @brief PageGuard wrappers for FetchPage
 *
 * Functionality should be the same as FetchPage, except
 * that, depending on the function called, a guard is returned.
 * If FetchPageRead or FetchPageWrite is called, it is expected that
 * the returned page already has a read or write latch held, respectively.
 *
 * @param page_id, the id of the page to fetch
 * @return PageGuard holding the fetched page
 */
auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  // Return a basic page guard initialized by the fetched page
  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  // Acquire the page read latch
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  // Acquire the page write latch
  page->WLatch();
  return {this, page};
}

/**
 * @brief PageGuard wrapper for NewPage
 *
 * Functionality should be the same as NewPage, except that
 * instead of returning a pointer to a page, you return a
 * BasicPageGuard structure.
 *
 * @param[out] page_id, the id of the new page
 * @return BasicPageGuard holding a new page
 */
auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

// TODO(student): You may add additional private members and helper functions
/**
 * @brief Pick the replacement frame from either the free list or the replacer (always find from the free list
 * first). Write the replaced page to the disk if the page is dirty.
 *
 * @return frame_id  The replacement frame id
 */
auto BufferPoolManager::PickReplacementFrame() -> frame_id_t {
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    // Use the first frame in the list as replacement
    frame_id = free_list_.front();
    // Remove the frame from free list
    free_list_.pop_front();
  } else {
    // No free frames left -> evict one from the pool
    replacer_->Evict(&frame_id);
    // If the replacing page is dirty, write it back to the disk first
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
      pages_[frame_id].is_dirty_ = false;
    }
    // Erase the old (page_id, frame_id) pair from page table
    page_table_.erase(pages_[frame_id].page_id_);
  }
  return frame_id;
}

}  // namespace bustub
