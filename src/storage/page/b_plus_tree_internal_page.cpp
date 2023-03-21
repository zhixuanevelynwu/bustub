//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetSize(0);
  this->SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt(KeyType key, ValueType value, int index) -> bool {
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Spill(BPlusTreeInternalPage *node2, int mid_index) -> KeyType {
  auto mid_key = KeyAt(mid_index);
  for (int i = mid_index; i < this->GetSize(); ++i) {
    node2->InsertAt(KeyAt(i), ValueAt(i), i - mid_index);
  }
  this->SetSize(mid_index);
  return mid_key;
}

/**
 * @brief Remove a key from the array
 *
 * @param key
 * @param index
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAt(int index) {
  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/**
 * @brief Get neighbors
 *
 * @param index
 * @return std::pair<page_id_t, page_id_t>
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetNeighbors(int index) const -> std::pair<page_id_t, page_id_t> {
  auto left = index > 0 ? ValueAt(index - 1) : INVALID_PAGE_ID;
  auto right = index < GetSize() - 1 ? ValueAt(index + 1) : INVALID_PAGE_ID;
  return std::pair<page_id_t, page_id_t>(left, right);
}

/**
 * @brief Redistribute keys among two leaves. This function always assumes the neighbor has more keys.
 *
 * @param neighbor
 * @param is_left if the provided neighbor param is on the left side of the leaf
 * @return KeyType
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::BorrowFrom(BPlusTreeInternalPage *neighbor, bool is_left) -> KeyType {
  KeyType new_key;
  if (is_left) {
    // The neighbor is smaller than this. Borrow the largest key from the neighbor
    new_key = neighbor->KeyAt(neighbor->GetSize() - 1);
    this->InsertAt(new_key, neighbor->ValueAt(neighbor->GetSize() - 1), 0);
    neighbor->RemoveAt(neighbor->GetSize() - 1);
  } else {
    this->InsertAt(neighbor->KeyAt(0), neighbor->ValueAt(0), this->GetSize());
    neighbor->RemoveAt(0);
    new_key = neighbor->KeyAt(0);
  }
  return new_key;
}

/**
 * @brief Merges a page with its neighbor. Assumes neighbor is larger than the leaf page.
 *
 * @param neighbor
 * @param is_left
 * @return KeyType
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(BPlusTreeInternalPage *neighbor) -> KeyType {
  auto original_size = this->GetSize();
  auto neighbor_size = neighbor->GetSize();
  KeyType to_remove = neighbor->KeyAt(0);
  this->IncreaseSize(neighbor_size);
  for (int i = original_size; i < this->GetSize(); i++) {
    this->array_[i] = neighbor->array_[i - original_size];
  }
  neighbor->SetSize(0);
  return to_remove;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
