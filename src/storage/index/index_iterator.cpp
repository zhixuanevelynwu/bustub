/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t current_pid, int index)
    : bpm_(bpm), current_pid_(current_pid), index_(index){};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){};  // NOLINT

// Return whether this iterator is pointing at the last key/value pair.
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  ReadPageGuard read_guard = bpm_->FetchPageRead(current_pid_);
  auto page = read_guard.As<BPlusTreePage>();
  auto leaf_page = reinterpret_cast<const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page);
  return (leaf_page->GetNextPageId() == INVALID_PAGE_ID) && (leaf_page->GetSize() - 1 == index_);
}

// Return the key/value pair this iterator is currently pointing at.
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  ReadPageGuard read_guard = bpm_->FetchPageRead(current_pid_);
  auto page = read_guard.As<BPlusTreePage>();
  auto leaf_page = reinterpret_cast<const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page);
  return leaf_page->At(index_);
}

// Move to the next key/value pair.
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  ReadPageGuard read_guard = bpm_->FetchPageRead(current_pid_);
  auto page = read_guard.As<BPlusTreePage>();
  auto leaf_page = reinterpret_cast<const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page);

  if (index_ < leaf_page->GetSize() - 1) {
    index_++;
  } else if (leaf_page->GetNextPageId() != INVALID_PAGE_ID) {
    current_pid_ = leaf_page->GetNextPageId();
    index_ = 0;
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
