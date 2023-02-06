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
  // TODO(students): remove this line after you have implemented the buffer pool manager
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");

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
  // Check if there's an available frame for the new page
  if (replacer_->Size() == 0 && free_list_.empty()) {
    return nullptr;
  }
  // Pick replacement frame
  // First check if there's any free frames to use
  auto frame_id = PickReplacementFrame();
  // Create a new page in the buffer pool
  auto new_page = std::make_shared<Page>();
  new_page->page_id_ = AllocatePage();  // {{Acquire a latch here}}
  // Update it in the page table
  page_table_[new_page->page_id_] = frame_id;
  // Record the access history of the frame and unpin the frame
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, true);
  return new_page.get();
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
  // Search for the page in buffer pool
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == page_id) {
      return &pages_[i];
    }
  }
  // Page not in buffer -> pick a replacement frame
  // Check if there's an available frame for the new page
  if (replacer_->Size() == 0 && free_list_.empty()) {
    return nullptr;
  }
  auto frame_id = PickReplacementFrame();
  // Load the page into the buffer pool
  auto new_page = std::make_shared<Page>();
  new_page->page_id_ = page_id;  // {{Acquire a latch here}}
  // Update it in the page table
  page_table_[page_id] = frame_id;
  // Write disk content to buffer pool
  disk_manager_->ReadPage(new_page->page_id_, new_page->GetData());
  // Record the access history of the frame and unpin the frame
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, true);
  return new_page.get();
}

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
    replacer_->SetEvictable(frame_id, false);
  } else {
    // No free frames left -> evict one from the pool
    replacer_->Evict(&frame_id);
    replacer_->SetEvictable(frame_id, false);
    // Find the page we need to evict
    page_id_t old_page_id;
    for (auto const &[pid, fid] : page_table_) {
      if (fid == frame_id) {
        old_page_id = pid;
      }
    }
    Page *old_page;
    for (size_t i = 0; i < pool_size_; i++) {
      if (pages_[i].page_id_ == old_page_id) {
        old_page = &pages_[i];
      }
    }
    // If the replacing page is dirty, write it back to the disk first
    if (old_page->IsDirty()) {
      disk_manager_->WritePage(old_page_id, old_page->data_);
    }
  }
  return frame_id;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { return false; }

void BufferPoolManager::FlushAllPages() {}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { return false; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
