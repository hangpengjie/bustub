#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  auto head = root_;
  if (head == nullptr) {
    return nullptr;
  }
  if (key.empty()) {
    if (head->is_value_node_) {
      auto ret = dynamic_cast<const TrieNodeWithValue<T> *>(head.get());
      if (ret == nullptr) {
        return nullptr;
      }
      return ret->value_.get();
    }
    return nullptr;
  }
  for (char c : key) {
    auto nxt = head->children_.find(c);
    if (nxt != head->children_.end()) {
      head = nxt->second;
    } else {
      return nullptr;
    }
  }
  if (head->is_value_node_) {
    auto ret = dynamic_cast<const TrieNodeWithValue<T> *>(head.get());
    if (ret == nullptr) {
      return nullptr;
    }
    return ret->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  std::shared_ptr<TrieNode> n_root = nullptr;
  if (root_ == nullptr) {
    n_root = std::make_shared<TrieNode>();
  } else {
    n_root = std::shared_ptr<TrieNode>(root_->Clone());
  }
  if (key.empty()) {
    n_root = std::make_shared<TrieNodeWithValue<T>>(n_root->children_, std::make_shared<T>(std::move(value)));
    return Trie(n_root);
  }
  auto n_head = n_root;
  for (std::size_t i = 0, sz = key.size(); i < sz - 1; ++i) {
    auto c = key[i];
    auto pre = n_head;

    if (n_head->children_.find(c) == n_head->children_.end()) {
      n_head = std::make_shared<TrieNode>();
    } else {
      n_head = std::shared_ptr<TrieNode>(n_head->children_[c]->Clone());
    }

    pre->children_[c] = n_head;
  }
  auto pre = n_head;
  char c = key.back();
  n_head = n_head->children_.find(c) == n_head->children_.end()
               ? std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)))
               : std::make_shared<TrieNodeWithValue<T>>(n_head->children_[c]->children_,
                                                        std::make_shared<T>(std::move(value)));
  pre->children_[c] = n_head;
  return Trie(n_root);

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if (root_ == nullptr) {
    return Trie(nullptr);
  }
  auto n_root = std::shared_ptr<TrieNode>(root_->Clone());
  if (key.empty()) {
    if (n_root->children_.empty()) {
      return Trie(nullptr);
    }
    if (n_root->is_value_node_) {
      n_root = std::make_shared<TrieNode>(n_root->children_);
    }
    return Trie(n_root);
  }
  std::vector<decltype(n_root)> path;
  auto n_head = n_root;
  for (char c : key) {
    auto pre = n_head;
    path.push_back(pre);
    if (n_head->children_.find(c) == n_head->children_.end()) {
      return Trie(n_root);
    }
    n_head = std::shared_ptr<TrieNode>(n_head->children_[c]->Clone());
    pre->children_[c] = n_head;
  }
  if (n_head->is_value_node_) {
    int idx = key.size() - 1;
    n_head = std::make_shared<TrieNode>(n_head->children_);
    path.back()->children_[key[idx]] = n_head;
    while (idx >= 0 && n_head->children_.empty() && !n_head->is_value_node_) {
      auto c = key[idx--];
      path.back()->children_.erase(c);
      n_head = path.back();
      path.pop_back();
    }
    if (idx == -1) {
      return Trie(nullptr);
    }
  }
  return Trie(n_root);
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
