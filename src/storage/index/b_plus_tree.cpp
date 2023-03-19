#include <sstream>
#include <string>

#include <stack>
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Returns true if this B+ tree has no keys and values.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return GetRootPageId() == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  Context ctx;
  (void)ctx;
  if (IsEmpty()) {
    return false;
  }
  auto root_pid = GetRootPageId();
  ReadPageGuard current_guard = bpm_->FetchPageRead(root_pid);
  auto current = current_guard.As<BPlusTreePage>();
  while (current->GetPageType() == IndexPageType::INTERNAL_PAGE) {
    auto node = reinterpret_cast<const InternalPage *>(current);
    int index = 0;
    while (comparator_(key, node->KeyAt(index + 1)) >= 0 && index < current->GetSize() - 1) {
      index++;
    }
    current_guard = bpm_->FetchPageRead(node->ValueAt(index));
    current = current_guard.As<BPlusTreePage>();
  }

  auto leaf = reinterpret_cast<const LeafPage *>(current);
  for (int i = 0; i < leaf->GetSize(); i++) {
    auto current_key = leaf->KeyAt(i);
    if (comparator_(key, current_key) == 0) {
      if (result != nullptr) {
        result->push_back(leaf->ValueAt(i));
      }
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  Context ctx;
  (void)ctx;
  auto root_pid = GetRootPageId();
  if (root_pid == INVALID_PAGE_ID) {
    StartNewTree(key, value);
    return true;
  }
  std::stack<WritePageGuard> parents;  // keep track of parent latches
  parents.push(bpm_->FetchPageWrite(root_pid));
  auto current = (parents.top()).As<BPlusTreePage>();
  page_id_t current_pid = root_pid;
  // sink to the corresponding leaf
  while (!current->IsLeafPage()) {
    auto current_internal = reinterpret_cast<const InternalPage *>(current);
    int index = 0;
    while (comparator_(key, current_internal->KeyAt(index + 1)) >= 0 && index < current->GetSize() - 1) {
      index++;
    }
    current_pid = current_internal->ValueAt(index);
    auto current_read_guard = bpm_->FetchPageRead(current_pid);
    current = current_read_guard.As<BPlusTreePage>();
    // child is safe -> release parents here
    if (current->GetSize() < current->GetMaxSize()) {
      while (!parents.empty()) {
        parents.pop();
      }
    }
    current_read_guard.Drop();
    parents.push(bpm_->FetchPageWrite(current_pid));
  }
  // insert at leaf
  auto leaf = reinterpret_cast<LeafPage *>((parents.top()).AsMut<BPlusTreePage>());
  if (!InsertToLeaf(leaf, key, value)) {
    return false;
  }
  // handle overflow
  std::shared_ptr<std::pair<KeyType, page_id_t>> mid_pair = nullptr;
  if (leaf->GetSize() > leaf_max_size_) {
    mid_pair = SplitLeaf(leaf);
    parents.pop();
    // insert spilled key/value pair to parents
    while (!parents.empty()) {
      auto parent = reinterpret_cast<InternalPage *>(parents.top().AsMut<BPlusTreePage>());
      InsertToInternal(parent, mid_pair->first, mid_pair->second);
      if (parent->GetSize() > internal_max_size_) {
        mid_pair = SplitInternal(parent);
      } else {
        mid_pair = nullptr;
        break;
      }
      parents.pop();
    }
  }
  // need to change root
  if (mid_pair != nullptr) {
    page_id_t new_root_pid;
    bpm_->NewPageGuarded(&new_root_pid);
    WritePageGuard new_root_guard = bpm_->FetchPageWrite(new_root_pid);
    auto new_root = reinterpret_cast<InternalPage *>(new_root_guard.AsMut<BPlusTreePage>());
    new_root->Init(internal_max_size_);
    new_root->InsertAt(mid_pair->first, root_pid, 0);
    new_root->InsertAt(mid_pair->first, mid_pair->second, 1);
    new_root_guard.Drop();
    SetRootPageId(new_root_pid);
  }
  return true;
}

/*****************************************************************************
 * INSERTION HELPER FUNCTIONS
 *****************************************************************************/
/**
 * @brief Inserts a key value pair to leaf.
 * @note  The provided leaf must be write latched.
 *
 * @param leaf
 * @param key
 * @param value
 * @return INDEX_TEMPLATE_ARGUMENTS
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertToLeaf(LeafPage *leaf, KeyType key, ValueType value) -> bool {
  int index = 0;
  while (index < leaf->GetSize() && comparator_(key, leaf->KeyAt(index)) > 0) {
    index++;
  }
  // Check if key already exist
  if (comparator_(key, leaf->KeyAt(index)) == 0) {
    return false;
  }
  leaf->InsertAt(key, value, index);
  return true;
}

/**
 * @brief Inserts a key pid pair to internal page
 *
 * @param node
 * @param key
 * @param value
 * @return INDEX_TEMPLATE_ARGUMENTS
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertToInternal(InternalPage *parent, KeyType key, page_id_t value) {
  int index = 1;
  while (comparator_(key, parent->KeyAt(index)) > 0 && index < parent->GetSize()) {
    index++;
  }
  parent->InsertAt(key, value, index);
}

/**
 * @brief Splits a leaf into 2
 *
 * @param leaf_pid
 * @return std::shared_ptr<std::pair<KeyType, page_id_t>>
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *leaf) -> std::shared_ptr<std::pair<KeyType, page_id_t>> {
  page_id_t leaf2_pid;
  auto leaf_page = bpm_->NewPageGuarded(&leaf2_pid);
  WritePageGuard leaf2_guard = bpm_->FetchPageWrite(leaf2_pid);
  auto leaf2 = reinterpret_cast<LeafPage *>(leaf2_guard.AsMut<BPlusTreePage>());
  leaf2->Init(leaf_max_size_);

  auto mid_key = leaf->Spill(leaf2, leaf2_pid);
  return std::make_shared<std::pair<KeyType, page_id_t>>(mid_key, leaf2_pid);
}

/**
 * @brief Splits an internal page into 2
 *
 * @param node_pid
 * @return std::shared_ptr<std::pair<KeyType, page_id_t>>
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *node) -> std::shared_ptr<std::pair<KeyType, page_id_t>> {
  page_id_t node2_pid;
  auto node_page = bpm_->NewPageGuarded(&node2_pid);
  WritePageGuard node2_guard = bpm_->FetchPageWrite(node2_pid);
  auto node2 = reinterpret_cast<InternalPage *>(node2_guard.AsMut<BPlusTreePage>());
  node2->Init(internal_max_size_);

  auto mid_key = node->Spill(node2);
  return std::make_shared<std::pair<KeyType, page_id_t>>(mid_key, node2_pid);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // declaration of context instance.
  Context ctx;
  (void)ctx;

  auto root_pid = GetRootPageId();
  if (root_pid == INVALID_PAGE_ID) {
    return;
  }

  RemoveFrom(root_pid, key);

  // update root if needed
  ReadPageGuard root_guard = bpm_->FetchPageRead(root_pid);
  auto root = root_guard.As<BPlusTreePage>();
  if (root->GetSize() == 0) {
    SetRootPageId(INVALID_PAGE_ID);
  }
  if (root->GetSize() == 1 && root->GetPageType() == IndexPageType::INTERNAL_PAGE) {
    auto root_internal = reinterpret_cast<const InternalPage *>(root);
    SetRootPageId(root_internal->ValueAt(0));
  }
}

/*****************************************************************************
 * Remove Helper
 *****************************************************************************/
/**
 * @brief Remove helper
 *
 * @param current
 * @param key
 * @return INDEX_TEMPLATE_ARGUMENTS
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFrom(page_id_t root_pid, const KeyType &key) {
  // keep track of parents
  std::stack<std::pair<page_id_t, int>> parents;
  ReadPageGuard current_guard = bpm_->FetchPageRead(root_pid);
  auto current = current_guard.As<const BPlusTreePage>();
  page_id_t current_pid = root_pid;
  while (current->GetPageType() == IndexPageType::INTERNAL_PAGE) {
    auto node = reinterpret_cast<const InternalPage *>(current);
    int index = 0;
    while (comparator_(key, node->KeyAt(index + 1)) >= 0 && index < current->GetSize() - 1) {
      index++;
    }
    parents.push(std::pair<page_id_t, int>(current_pid, index));
    current_pid = node->ValueAt(index);
    ReadPageGuard current_guard = bpm_->FetchPageRead(current_pid);
    current = current_guard.As<BPlusTreePage>();
  }
  current_guard.Drop();

  // Delete from leaf
  WritePageGuard leaf_guard = bpm_->FetchPageWrite(current_pid);
  auto leaf = reinterpret_cast<LeafPage *>(leaf_guard.AsMut<BPlusTreePage>());
  RemoveFromLeaf(leaf, key);
  if (leaf->GetSize() >= leaf->GetMinSize()) {  // return directly if no underflow
    return;
  }
  leaf_guard.Drop();

  // Handle underflow
  page_id_t parent_pid;
  int index;
  if (!parents.empty()) {
    // Gets current neighbors by consulting the immediate parent
    parent_pid = parents.top().first;
    index = parents.top().second;
    parents.pop();
    auto neighbors = GetNeighbors(parent_pid, index);  // case when both are invalid
    if (neighbors.first == INVALID_PAGE_ID && neighbors.second == INVALID_PAGE_ID) {
      RemoveFromInternal(parent_pid, index);
    } else if (neighbors.first != INVALID_PAGE_ID) {       // See if it has a left neighbor
      if (GetPageSize(neighbors.first) > LeafMinSize()) {  // See if we can redistribute keys
        auto new_key = RedistributeLeaves(current_pid, neighbors.first, true);
        SetKeyInternal(parent_pid, new_key, index);
        return;
      }
      MergeLeaves(neighbors.first, current_pid);
      RemoveFromInternal(parent_pid, index);
    } else {
      if (GetPageSize(neighbors.second) > LeafMinSize()) {  // See if we can redistribute keys
        auto new_key = RedistributeLeaves(current_pid, neighbors.second, false);
        SetKeyInternal(parent_pid, new_key, index + 1);
        return;
      }
      MergeLeaves(current_pid, neighbors.second);
      RemoveFromInternal(parent_pid, index + 1);
    }
    current_pid = parent_pid;
  }
  // Update parents accordingly
  while (!parents.empty()) {
    if (GetPageSize(current_pid) >= InternalMinSize()) {
      return;  // Did not underflow. No more operation.
    }
    // Gets its neighbors by consulting the immediate parent
    parent_pid = parents.top().first;
    index = parents.top().second;
    parents.pop();
    auto neighbors = GetNeighbors(parent_pid, index);
    if (neighbors.first == INVALID_PAGE_ID && neighbors.second == INVALID_PAGE_ID) {
      RemoveFromInternal(parent_pid, index);
    } else if (neighbors.first != INVALID_PAGE_ID) {           // See if it has a left neighbor
      if (GetPageSize(neighbors.first) > InternalMinSize()) {  // See if we can redistribute keys
        auto new_key = RedistributeInternals(current_pid, neighbors.first, true);
        SetKeyInternal(parent_pid, new_key, index);
        return;
      }
      MergeInternals(neighbors.first, current_pid);
      RemoveFromInternal(parent_pid, index);
    } else {
      if (GetPageSize(neighbors.second) > InternalMinSize()) {  // See if we can redistribute keys
        auto new_key = RedistributeInternals(current_pid, neighbors.second, false);
        SetKeyInternal(parent_pid, new_key, index + 1);
        return;
      }
      MergeInternals(current_pid, neighbors.second);
      RemoveFromInternal(parent_pid, index + 1);
    }
    current_pid = parent_pid;
  }
}

/**
 * @brief Removes a key from the leaf
 *
 * @param leaf
 * @param key
 * @return INDEX_TEMPLATE_ARGUMENTS
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromLeaf(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf, KeyType key) {
  int index = 0;
  while (index < leaf->GetSize() && comparator_(key, leaf->KeyAt(index)) > 0) {
    index++;
  }
  if (comparator_(key, leaf->KeyAt(index)) != 0) {
    return;
  }
  leaf->RemoveAt(index);
}

/**
 * @brief Removes a key from the internal page
 *
 * @param node
 * @param key
 * @return INDEX_TEMPLATE_ARGUMENTS
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromInternal(page_id_t pid, int index) {
  WritePageGuard parent_write_guard = bpm_->FetchPageWrite(pid);
  auto parent_write = reinterpret_cast<InternalPage *>(parent_write_guard.AsMut<BPlusTreePage>());
  parent_write->RemoveAt(index);
}

/**
 * @brief Redistributes keys among 2 leaf pages
 *
 * @param leaf_pid
 * @param neighbor_pid
 * @param is_left if the provided neighbor param is on the left side of the leaf
 * @return page_id_t the new key to update upward
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RedistributeLeaves(page_id_t leaf_pid, page_id_t neighbor_pid, bool is_left) -> KeyType {
  // don't want leaf to be evicted here
  WritePageGuard leaf_guard = bpm_->FetchPageWrite(leaf_pid);
  auto leaf = reinterpret_cast<LeafPage *>(leaf_guard.AsMut<BPlusTreePage>());

  WritePageGuard neighbor_guard = bpm_->FetchPageWrite(neighbor_pid);
  auto neighbor = reinterpret_cast<LeafPage *>(neighbor_guard.AsMut<BPlusTreePage>());

  return leaf->Redistribute(neighbor, is_left);
}

/**
 * @brief
 *
 * @param left_pid
 * @param right_pid
 * @return KeyType
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeLeaves(page_id_t left_pid, page_id_t right_pid) -> KeyType {
  // don't want leaf to be evicted here
  WritePageGuard left_guard = bpm_->FetchPageWrite(left_pid);
  auto left = reinterpret_cast<LeafPage *>(left_guard.AsMut<BPlusTreePage>());

  WritePageGuard right_guard = bpm_->FetchPageWrite(right_pid);
  auto right = reinterpret_cast<LeafPage *>(right_guard.AsMut<BPlusTreePage>());

  return left->Merge(right);
}

/**
 * @brief Redistributes keys among 2 leaf pages
 *
 * @param leaf_pid
 * @param neighbor_pid
 * @param is_left if the provided neighbor param is on the left side of the leaf
 * @return page_id_t the new key to update upward
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RedistributeInternals(page_id_t internal_pid, page_id_t neighbor_pid, bool is_left) -> KeyType {
  WritePageGuard internal_guard = bpm_->FetchPageWrite(internal_pid);
  auto internal = reinterpret_cast<InternalPage *>(internal_guard.AsMut<BPlusTreePage>());

  WritePageGuard neighbor_guard = bpm_->FetchPageWrite(neighbor_pid);
  auto neighbor = reinterpret_cast<InternalPage *>(neighbor_guard.AsMut<BPlusTreePage>());

  return internal->Redistribute(neighbor, is_left);
}

/**
 * @brief
 *
 * @param left_pid
 * @param right_pid
 * @return KeyType
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeInternals(page_id_t left_pid, page_id_t right_pid) -> KeyType {
  WritePageGuard left_guard = bpm_->FetchPageWrite(left_pid);
  auto left = reinterpret_cast<InternalPage *>(left_guard.AsMut<BPlusTreePage>());

  WritePageGuard right_guard = bpm_->FetchPageWrite(right_pid);
  auto right = reinterpret_cast<InternalPage *>(right_guard.AsMut<BPlusTreePage>());

  return left->Merge(right);
}

/**
 * @brief
 *
 * @param parent_pid
 * @param index
 * @return std::pair<page_id_t, page_id_t>
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetNeighbors(page_id_t parent_pid, int index) -> std::pair<page_id_t, page_id_t> {
  ReadPageGuard parent_read_guard = bpm_->FetchPageRead(parent_pid);
  auto parent = reinterpret_cast<const InternalPage *>(parent_read_guard.As<BPlusTreePage>());
  return parent->GetNeighbors(index);
}

/**
 * @brief
 *
 * @param pid
 * @param key
 * @param index
 * @return INDEX_TEMPLATE_ARGUMENTS
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetKeyInternal(page_id_t pid, KeyType key, int index) {
  WritePageGuard write_guard = bpm_->FetchPageWrite(pid);
  auto write = reinterpret_cast<InternalPage *>(write_guard.AsMut<BPlusTreePage>());
  write->SetKeyAt(index, key);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetPageSize(page_id_t pid) const -> int {
  ReadPageGuard guard = bpm_->FetchPageRead(pid);
  auto page = guard.As<BPlusTreePage>();
  return page->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalMinSize() const -> int { return internal_max_size_ / 2; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafMinSize() const -> int { return leaf_max_size_ / 2; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto current_pid = GetRootPageId();
  ReadPageGuard current_guard = bpm_->FetchPageRead(current_pid);
  auto current = current_guard.As<BPlusTreePage>();
  while (current->GetPageType() == IndexPageType::INTERNAL_PAGE) {
    auto node = reinterpret_cast<const InternalPage *>(current);
    current_pid = node->ValueAt(0);
    current_guard = bpm_->FetchPageRead(current_pid);
    current = current_guard.As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(bpm_, current_pid, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto current_pid = GetRootPageId();
  ReadPageGuard current_guard = bpm_->FetchPageRead(current_pid);
  auto current = current_guard.As<BPlusTreePage>();
  int index = 0;
  while (current->GetPageType() == IndexPageType::INTERNAL_PAGE) {
    auto node = reinterpret_cast<const InternalPage *>(current);
    while (comparator_(key, node->KeyAt(index + 1)) >= 0 && index < current->GetSize() - 1) {
      index++;
    }
    current_pid = node->ValueAt(index);
    current_guard = bpm_->FetchPageRead(current_pid);
    current = current_guard.As<BPlusTreePage>();
  }
  auto leaf = reinterpret_cast<const LeafPage *>(current);
  for (index = 0; index < leaf->GetSize(); index++) {
    auto current_key = leaf->KeyAt(index);
    if (comparator_(key, current_key) <= 0) {
      break;
    }
  }
  return INDEXITERATOR_TYPE(bpm_, current_pid, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  auto current_pid = GetRootPageId();
  ReadPageGuard current_guard = bpm_->FetchPageRead(current_pid);
  auto current = current_guard.As<BPlusTreePage>();
  while (current->GetPageType() == IndexPageType::INTERNAL_PAGE) {
    auto node = reinterpret_cast<const InternalPage *>(current);
    current_pid = node->ValueAt(current->GetSize() - 1);
    current_guard = bpm_->FetchPageRead(current_pid);
    current = current_guard.As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(bpm_, current_pid, current->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  auto root_pid = header_page->root_page_id_;
  return root_pid;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPageId(page_id_t page_id) {
  WritePageGuard write_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = write_guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(KeyType key, ValueType value) {
  page_id_t leaf_pid;
  auto leaf_page = bpm_->NewPageGuarded(&leaf_pid);
  WritePageGuard leaf_guard = bpm_->FetchPageWrite(leaf_pid);
  auto leaf = reinterpret_cast<LeafPage *>(leaf_guard.AsMut<BPlusTreePage>());
  leaf->Init(leaf_max_size_);
  SetRootPageId(leaf_pid);
  leaf->InsertAt(key, value, 0);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
