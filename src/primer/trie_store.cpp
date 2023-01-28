#include "primer/trie_store.h"
#include <cstddef>
#include "common/exception.h"

namespace bustub {

// Pseudo-code:
// (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
//     trie while holding the root lock.
// (2) Lookup the value in the trie.
// (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
//     root. Otherwise, return std::nullopt.
template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  root_lock_.lock();
  auto root = root_;
  root_lock_.unlock();
  auto val = root.Get<T>(key);
  if (val) {
    return ValueGuard<T>(root, *val);
  }
  return std::nullopt;
}

// You will need to ensure there is only one writer at a time. Think of how you can achieve this.
// The logic should be somehow similar to `TrieStore::Get`.
template <class T>
void TrieStore::Put(std::string_view key, T value) {
  write_lock_.lock();
  root_ = root_.Put(key, std::move(value));
  write_lock_.unlock();
}

// You will need to ensure there is only one writer at a time. Think of how you can achieve this.
// The logic should be somehow similar to `TrieStore::Get`.
void TrieStore::Remove(std::string_view key) {
  write_lock_.lock();
  root_ = root_.Remove(key);
  write_lock_.unlock();
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
