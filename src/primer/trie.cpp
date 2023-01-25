#include "primer/trie.h"
#include <cstddef>
#include <memory>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (root_ == nullptr) {
    return nullptr;
  }
  // Walk through the trie to find the node corresponding to the key
  auto current_node = root_;
  for (char c : key) {
    if (!current_node->children_.count(c)) {
      return nullptr;
    }
    current_node = current_node->children_.at(c);
  }
  // At end of the key, check if it is a value node
  if (current_node && current_node->is_value_node_) {
    auto node_with_val = dynamic_cast<const TrieNodeWithValue<T> *>(current_node.get());
    return node_with_val ? node_with_val->value_.get() : nullptr;
  }
  // Nothing's found
  return nullptr;
}

// Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
// You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
// exists, you should create a new `TrieNodeWithValue`.
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  std::shared_ptr<TrieNode> new_root = root_ != nullptr ? root_->Clone() : std::make_shared<TrieNode>(TrieNode());

  // Walk through the trie to insert new key
  auto new_root_ptr = std::shared_ptr<TrieNode>(std::move(new_root));
  auto current_node = new_root_ptr;

  // Get intermediate nodes
  int len = key.size();
  for (int i = 0; i < len; i++) {
    char c = key[i];
    if (!current_node->children_.count(c)) {
      // Insert
      std::shared_ptr<TrieNode> tmp;
      if (i == len - 1) {
        tmp = std::make_shared<TrieNode>(TrieNodeWithValue(std::make_shared<T>(std::move(value))));
      } else {
        tmp = std::make_shared<TrieNode>(TrieNode());
      }
      current_node->children_.insert({c, tmp});
      current_node = tmp;
    } else {
      // Clone
      auto cloned_child = current_node->children_[c]->Clone();
      // If is the last char, create a node with value
      // Otherwise, copy a trie node
      std::shared_ptr<TrieNode> cloned_child_ptr;
      if (i == len - 1) {
        auto tmp = TrieNodeWithValue(cloned_child->children_, std::make_shared<T>(std::move(value)));
        cloned_child_ptr = std::make_shared<TrieNode>(tmp);
      } else {
        cloned_child_ptr = std::shared_ptr<TrieNode>(std::move(cloned_child));
      }
      current_node->children_[c] = cloned_child_ptr;
      current_node = cloned_child_ptr;
    }
  }

  Trie new_trie = Trie(new_root_ptr);
  return new_trie;
}

// You should walk through the trie and remove nodes if necessary.
// If the node doesn't contain a value any more,
// you should convert it to `TrieNode`.
// If a node doesn't have children any more, you should remove it.
auto Trie::Remove(std::string_view key) const -> Trie { return *this; }

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
