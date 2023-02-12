#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

/**
 * @brief Move constructor for BasicPageGuard
 *
 * When you call BasicPageGuard(std::move(other_guard)), you
 * expect that the new guard will behave exactly like the other
 * one. In addition, the old page guard should not be usable. For
 * example, it should not be possible to call .Drop() on both page
 * guards and have the pin count decrease by 2.
 */
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  // Set this page guard the same as the provided page guard
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  // The provided page guard becomes unusable
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

/**
 * @brief Drop a page guard
 *
 * Dropping a page guard should clear all contents
 * (so that the page guard is no longer useful), and
 * it should tell the BPM that we are done using this page,
 * per the specification in the writeup.
 */
void BasicPageGuard::Drop() {
  // Tell the BPM that we are done using this page
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  // The page guard becomes unusable
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

/**
 * @brief Move assignment for BasicPageGuard
 *
 * Similar to a move constructor, except that the move
 * assignment assumes that BasicPageGuard already has a page
 * being guarded. Think carefully about what should happen when
 * a guard replaces its held page with a different one, given
 * the purpose of a page guard.
 */
auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // If already has a page being guarded, unpin that page first
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  // The original page guard becomes unusable
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
  return *this;
}

/**
 * @brief Destructor for BasicPageGuard
 *
 * When a page guard goes out of scope, it should behave as if
 * the page guard was dropped.
 */
BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

/** TODO(P1): Add implementation
 *
 * @brief Move constructor for ReadPageGuard
 *
 * Very similar to BasicPageGuard. You want to create
 * a ReadPageGuard using another ReadPageGuard. Think
 * about if there's any way you can make this easier for yourself...
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

/**
 * @brief Move assignment for ReadPageGuard
 *
 * Very similar to BasicPageGuard. Given another ReadPageGuard,
 * replace the contents of this one with that one.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  guard_ = std::move(that.guard_);
  return *this;
}

/**
 * @brief Drop a ReadPageGuard
 *
 * ReadPageGuard's Drop should behave similarly to BasicPageGuard,
 * except that ReadPageGuard has an additional resource - the latch!
 * However, you should think VERY carefully about in which order you
 * want to release these resources.
 */
void ReadPageGuard::Drop() {
  // Tell the BPM that we are done using this page
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  }
  // Unlatch the page
  guard_.page_->RUnlatch();
  // The page guard becomes unusable
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
  guard_.is_dirty_ = false;
}

/**
 * @brief Destructor for ReadPageGuard
 *
 * Just like with BasicPageGuard, this should behave
 * as if you were dropping the guard.
 */
ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

/**
 * @brief Move constructor for WritePageGuard
 *
 * Very similar to BasicPageGuard. You want to create
 * a WritePageGuard using another WritePageGuard. Think
 * about if there's any way you can make this easier for yourself...
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

/**
 * @brief Move assignment for WritePageGuard
 *
 * Very similar to BasicPageGuard. Given another WritePageGuard,
 * replace the contents of this one with that one.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  guard_ = std::move(that.guard_);
  return *this;
}

/**
 * @brief Drop a WritePageGuard
 *
 * WritePageGuard's Drop should behave similarly to BasicPageGuard,
 * except that WritePageGuard has an additional resource - the latch!
 * However, you should think VERY carefully about in which order you
 * want to release these resources.
 */
void WritePageGuard::Drop() {
  // Tell the BPM that we are done using this page
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  }
  // Unlatch the page
  guard_.page_->WUnlatch();
  // The page guard becomes unusable
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
  guard_.is_dirty_ = false;
}

/**
 * @brief Destructor for WritePageGuard
 *
 * Just like with BasicPageGuard, this should behave
 * as if you were dropping the guard.
 */
WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
