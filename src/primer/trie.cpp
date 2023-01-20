#include "primer/trie.h"
#include <cstddef>
#include <memory>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
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
  // Walk through the trie to insert new key
  auto new_root = std::make_shared<TrieNode>(TrieNode(root_->children_));
  auto current_node = new_root;
  for (char c : key) {
    if (!current_node->children_.count(c)) {
      current_node->children_.insert({c, std::make_shared<TrieNode>(TrieNode())});
    }
    current_node = current_node->children_.at(c);
  }
  // Insert value to the last node
  auto node_with_val = dynamic_cast<TrieNodeWithValue<T> *>(current_node.get());
  node_with_val->value_ = std::make_shared<T>(std::move(value));
  node_with_val->is_value_node_ = true;
  // With the new root, create a new Trie
  Trie new_trie = Trie(new_root);
  return new_trie;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

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
