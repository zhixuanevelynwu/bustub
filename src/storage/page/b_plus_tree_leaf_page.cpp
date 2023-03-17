//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetSize(0);
  this->SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

/**
 * @brief Get value at index
 *
 * @param index
 * @return ValueType
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

/**
 * @brief Insert a new element to the array. Shift all elements after index by 1. Before calling this function, it is
 * the caller's job to figure out the correct index to maintain a sorted order. Increments size of the leaf by one on
 * success.
 *
 * @param key
 * @param value
 * @param index
 * @return true
 * @return false  if leaf is full
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAt(KeyType key, ValueType value, int index) -> bool {
  for (int i = GetSize(); i > index; --i) {
    array_[i] = array_[i - 1];
  }
  array_[index] = MappingType(key, value);
  IncreaseSize(1);
  return true;
}

/**
 * @brief Spill half of what this page have to another page
 *
 * @return KeyType  the middle key
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Spill(BPlusTreeLeafPage *leaf2, page_id_t leaf2_id) -> KeyType {
  int mid = GetSize() / 2;
  auto mid_key = KeyAt(mid);
  for (int i = mid; i < this->GetSize(); ++i) {
    leaf2->InsertAt(KeyAt(i), ValueAt(i), i - mid);
  }
  this->next_page_id_ = leaf2_id;
  this->SetSize(mid);
  return mid_key;
}

/**
 * @brief Remove a key from the array
 *
 * @param key
 * @param index
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAt(int index) {
  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/**
 * @brief Redistribute keys among two leaves. This function always assumes the neighbor has more keys.
 *
 * @param neighbor
 * @param is_left if the provided neighbor param is on the left side of the leaf
 * @return KeyType
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Redistribute(BPlusTreeLeafPage *neighbor, bool is_left) -> KeyType {
  KeyType new_key;
  if (is_left) {
    // The neighbor is smaller than this. Borrow the largest key from the neighbor
    new_key = neighbor->KeyAt(neighbor->GetSize() - 1);
    this->InsertAt(new_key, neighbor->ValueAt(neighbor->GetSize() - 1), 0);
    neighbor->RemoveAt(neighbor->GetSize() - 1);
  } else {
    this->InsertAt(neighbor->KeyAt(0), neighbor->ValueAt(0), this->GetSize() - 1);
    neighbor->RemoveAt(0);
    new_key = neighbor->KeyAt(0);
  }
  return new_key;
}

/**
 * @brief Merges a leaf page with its neighbor. Assumes neighbor is larger than the leaf page.
 *
 * @param neighbor
 * @param is_left
 * @return KeyType
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(BPlusTreeLeafPage *neighbor) -> KeyType {
  auto original_size = this->GetSize();
  auto neighbor_size = neighbor->GetSize();
  KeyType to_remove = neighbor->KeyAt(0);
  this->IncreaseSize(neighbor_size);
  for (int i = original_size; i < this->GetSize(); i++) {
    this->array_[i] = neighbor->array_[i - original_size];
  }
  this->next_page_id_ = neighbor->next_page_id_;
  neighbor->SetSize(0);
  return to_remove;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
