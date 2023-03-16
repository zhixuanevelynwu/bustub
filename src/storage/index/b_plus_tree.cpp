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
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  BasicPageGuard guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}
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
  auto root = GetBPlusTreePage(root_pid);
  auto current = root;
  while (current->GetPageType() == IndexPageType::INTERNAL_PAGE) {
    auto node = reinterpret_cast<const InternalPage *>(current);
    int index = 0;
    while (comparator_(key, node->KeyAt(index + 1)) >= 0 && index < current->GetSize() - 1) {
      index++;
    }
    current = GetBPlusTreePage(node->ValueAt(index));
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

  if (GetValue(key, nullptr, txn)) {
    return false;
  }

  auto root_pid = GetRootPageId();
  if (root_pid == INVALID_PAGE_ID) {
    StartNewTree(key, value);
    return true;
  }

  auto root = GetBPlusTreePage(root_pid);
  auto mid_pair = InsertHelper(root, key, value, txn);
  if (mid_pair != nullptr) {  // need to change root
    page_id_t root2_pid;
    auto root2 = CreateInternalPage(&root2_pid);
    root2->InsertAt(mid_pair->first, root_pid, 0);
    root2->InsertAt(mid_pair->first, mid_pair->second, 1);
    SetRootPageId(root2_pid);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertHelper(BPlusTreePage *current, const KeyType &key, const ValueType &value, Transaction *txn)
    -> std::shared_ptr<std::pair<KeyType, page_id_t>> {
  // keep track of parents
  std::stack<page_id_t> parents;
  page_id_t current_pid = GetRootPageId();
  if (current->GetPageType() == IndexPageType::INTERNAL_PAGE) {
    parents.push(GetRootPageId());
  }
  while (current->GetPageType() == IndexPageType::INTERNAL_PAGE) {
    auto node = reinterpret_cast<InternalPage *>(current);
    int index = 0;
    while (comparator_(key, node->KeyAt(index + 1)) >= 0 && index < current->GetSize() - 1) {
      index++;
    }
    current_pid = node->ValueAt(index);
    current = GetBPlusTreePage(current_pid);
    if (current->GetPageType() == IndexPageType::INTERNAL_PAGE) {
      parents.push(current_pid);
    }
  }
  // insert at leaf
  auto leaf = reinterpret_cast<LeafPage *>(current);
  InsertToLeaf(leaf, key, value);
  if (leaf->GetSize() > leaf->GetMaxSize()) {
    auto mid_pair = SplitLeaf(current_pid);
    while (!parents.empty()) {
      auto parent = reinterpret_cast<InternalPage *>(GetBPlusTreePage(parents.top()));
      BUSTUB_ASSERT(parent->GetPageType() == IndexPageType::INTERNAL_PAGE, "Wrong parent type");
      InsertToInternal(parent, mid_pair->first, mid_pair->second);
      if (parent->GetSize() > parent->GetMaxSize()) {
        mid_pair = SplitInternal(parents.top());
      } else {
        mid_pair = nullptr;
        break;
      }
      parents.pop();
    }
    return mid_pair;
  }
  return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertToLeaf(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf, KeyType key,
                                  ValueType value) {
  int index = 0;
  while (index < leaf->GetSize() && comparator_(key, leaf->KeyAt(index)) > 0) {
    index++;
  }
  leaf->InsertAt(key, value, index);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertToInternal(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *node, KeyType key,
                                      page_id_t value) {
  int index = 1;
  while (comparator_(key, node->KeyAt(index)) > 0 && index < node->GetSize()) {
    index++;
  }
  node->InsertAt(key, value, index);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(page_id_t leaf_pid) -> std::shared_ptr<std::pair<KeyType, page_id_t>> {
  // don't want leaf to be evicted here
  WritePageGuard leaf_guard = bpm_->FetchPageWrite(leaf_pid);
  auto leaf = reinterpret_cast<LeafPage *>(leaf_guard.AsMut<BPlusTreePage>());

  page_id_t leaf2_pid;
  auto leaf_page = bpm_->NewPageGuarded(&leaf2_pid);
  WritePageGuard leaf2_guard = bpm_->FetchPageWrite(leaf2_pid);
  auto leaf2 = reinterpret_cast<LeafPage *>(leaf2_guard.AsMut<BPlusTreePage>());
  leaf2->Init(leaf_max_size_);

  auto mid_key = leaf->Spill(leaf2, leaf2_pid);
  return std::make_shared<std::pair<KeyType, page_id_t>>(mid_key, leaf2_pid);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(page_id_t node_pid) -> std::shared_ptr<std::pair<KeyType, page_id_t>> {
  // same here
  WritePageGuard node_guard = bpm_->FetchPageWrite(node_pid);
  auto node = reinterpret_cast<InternalPage *>(node_guard.AsMut<BPlusTreePage>());

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
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  BasicPageGuard header_guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
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
auto BPLUSTREE_TYPE::GetBPlusTreePage(page_id_t page_id) -> BPlusTreePage * {
  BasicPageGuard guard = bpm_->FetchPageBasic(page_id);
  auto page = guard.AsMut<BPlusTreePage>();
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(KeyType key, ValueType value) {
  page_id_t leaf_pid;
  auto leaf = CreateLeafPage(&leaf_pid);
  SetRootPageId(leaf_pid);
  leaf->InsertAt(key, value, 0);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateLeafPage(page_id_t *leaf_pid) -> BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> * {
  auto leaf_page = bpm_->NewPageGuarded(leaf_pid);
  BasicPageGuard leaf_guard = bpm_->FetchPageBasic(*leaf_pid);
  auto leaf = reinterpret_cast<LeafPage *>(leaf_guard.AsMut<BPlusTreePage>());
  leaf->Init(leaf_max_size_);
  return leaf;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateInternalPage(page_id_t *node_pid)
    -> BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> * {
  auto node_page = bpm_->NewPageGuarded(node_pid);
  BasicPageGuard node_guard = bpm_->FetchPageBasic(*node_pid);
  auto node = reinterpret_cast<InternalPage *>(node_guard.AsMut<BPlusTreePage>());
  node->Init(internal_max_size_);
  return node;
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
