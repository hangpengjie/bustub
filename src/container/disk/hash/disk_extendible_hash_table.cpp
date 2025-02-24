//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  index_name_ = name;
  page_id_t page_id = INVALID_PAGE_ID;
  auto header_page = bpm_->NewPageGuarded(&page_id).UpgradeWrite().AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);
  header_page_id_ = page_id;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto h = Hash(key);
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();

  uint32_t directory_index = header_page->HashToDirectoryIndex(h);
  if (int directory_page_id = header_page->GetDirectoryPageId(directory_index); directory_page_id != INVALID_PAGE_ID) {
    ReadPageGuard directory_guard = bpm_->FetchPageRead(directory_page_id);
    auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();

    auto bucket_idx = directory_page->HashToBucketIndex(h);
    if (auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx); bucket_page_id != INVALID_PAGE_ID) {
      auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
      auto bucket_page = bucket_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
      V tmp;
      if (bucket_page->Lookup(key, tmp, cmp_)) {
        result->push_back(tmp);
        return true;
      }
    }
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto h = Hash(key);
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();

  uint32_t directory_index = header_page->HashToDirectoryIndex(h);
  if (int directory_page_id = header_page->GetDirectoryPageId(directory_index); directory_page_id != INVALID_PAGE_ID) {
    // Directory exists
    WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
    auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
    auto bucket_idx = directory_page->HashToBucketIndex(h);
    if (int bucket_page_id = directory_page->GetBucketPageId(bucket_idx); bucket_page_id != INVALID_PAGE_ID) {
      header_guard.Drop();
      WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
      auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      if (V tmp; bucket_page->Lookup(key, tmp, cmp_)) {  // Key already exists
        return false;
      }
      if (!bucket_page->IsFull()) {
        return bucket_page->Insert(key, value, cmp_);
      }
      // Bucket is full
      while (bucket_page->IsFull()) {
        if (directory_page->GetGlobalDepth() == directory_page->GetLocalDepth(bucket_idx)) {
          if (directory_page->GetGlobalDepth() == directory_max_depth_) {
            // The directory is full
            return false;
          }
          directory_page->IncrGlobalDepth();
        }
        page_id_t new_bucket_page_id = INVALID_PAGE_ID;
        auto new_bucket_page =
            bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite().AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
        new_bucket_page->Init(bucket_max_size_);
        directory_page->IncrLocalDepth(bucket_idx);
        auto new_local_depth = directory_page->GetLocalDepth(bucket_idx);
        auto local_depth_mask = directory_page->GetLocalDepthMask(bucket_idx);
        // directory rehash
        UpdateDirectoryMapping(directory_page, bucket_idx, new_bucket_page_id, new_local_depth, local_depth_mask);
        auto new_bucket_idx = bucket_idx & (local_depth_mask >> 1);
        // Migrate entries
        MigrateEntries(bucket_page, new_bucket_page, new_bucket_idx, local_depth_mask);

        // Insert new entry
        bucket_idx = directory_page->HashToBucketIndex(h);
        bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
        // Update bucket_page
        if (bucket_page_id == new_bucket_page_id) {
          bucket_page = new_bucket_page;
        }
      }
      return bucket_page->Insert(key, value, cmp_);
    }
    // Bucket does not exist
    return InsertToNewBucket(directory_page, bucket_idx, key, value);
  }
  // Directory does not exist
  return InsertToNewDirectory(header_page, directory_index, h, key, value);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  for (uint32_t i = 0; i < old_bucket->Size(); ++i) {
    auto key = old_bucket->KeyAt(i);
    auto value = old_bucket->ValueAt(i);
    if ((Hash(key) & local_depth_mask) == new_bucket_idx) {
      new_bucket->Insert(key, value, cmp_);
      old_bucket->RemoveAt(i);
      --i;
    }
  }
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto directory_page = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite().AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, directory_page_id);
  return InsertToNewBucket(directory_page, directory_page->HashToBucketIndex(hash), key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  auto bucket_page = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite().AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  directory->SetLocalDepth(bucket_idx, 0);
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t old_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  auto new_first_bucket_idx = old_bucket_idx & (local_depth_mask >> 1);
  auto prime_idx = new_first_bucket_idx | (1 << (new_local_depth - 1));
  uint32_t distance = 1 << new_local_depth;

  for (uint32_t i = new_first_bucket_idx; i < directory->Size(); i += distance, prime_idx += distance) {
    directory->SetBucketPageId(i, new_bucket_page_id);
    directory->SetLocalDepth(i, new_local_depth);
    directory->SetLocalDepth(prime_idx, new_local_depth);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto h = Hash(key);
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  uint32_t directory_index = header_page->HashToDirectoryIndex(h);
  if (int directory_page_id = header_page->GetDirectoryPageId(directory_index); directory_page_id != INVALID_PAGE_ID) {
    // Directory exists
    WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
    auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
    uint32_t bucket_idx = directory_page->HashToBucketIndex(h);
    if (int bucket_page_id = directory_page->GetBucketPageId(bucket_idx); bucket_page_id != INVALID_PAGE_ID) {
      // Bucket exists
      auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
      auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      bool result = bucket_page->Remove(key, cmp_);
      while (bucket_page->IsEmpty()) {
        bucket_guard.Drop();
        uint32_t bucket_local_depth = directory_page->GetLocalDepth(bucket_idx);
        // auto bucket_page_id =  directory_page->GetBucketPageId(bucket_idx)
        if (bucket_local_depth == 0) {
          break;
        }
        auto merge_bucket_idx = directory_page->GetSplitImageIndex(bucket_idx);
        auto merge_bucket_local_depth = directory_page->GetLocalDepth(merge_bucket_idx);
        auto merge_bucket_page_id = directory_page->GetBucketPageId(merge_bucket_idx);
        if (bucket_local_depth == merge_bucket_local_depth) {
          // the buckets has the same local depth
          uint32_t new_local_depth = bucket_local_depth - 1;
          for (uint32_t i = bucket_idx & (directory_page->GetLocalDepthMask(bucket_idx) >> 1);
               i < directory_page->Size(); i += (1 << new_local_depth)) {
            directory_page->SetBucketPageId(i, merge_bucket_page_id);
            directory_page->SetLocalDepth(i, new_local_depth);
          }
          bpm_->DeletePage(bucket_page_id);
          if (new_local_depth == 0) {
            break;
          }
          auto split_bucket_idx = directory_page->GetSplitImageIndex(merge_bucket_idx);
          auto split_bucket_page_id = directory_page->GetBucketPageId(split_bucket_idx);
          if (split_bucket_page_id == INVALID_PAGE_ID) {
            break;
          }
          bucket_guard = bpm_->FetchPageWrite(split_bucket_page_id);
          bucket_idx = split_bucket_idx;
          bucket_page_id = split_bucket_page_id;
          bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
        } else {
          // because local depth is different
          break;
        }
      }
      while (directory_page->CanShrink()) {
        directory_page->DecrGlobalDepth();
      }
      return result;
    }
  }
  return false;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
