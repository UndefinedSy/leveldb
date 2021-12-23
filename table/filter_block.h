// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// FilterBlockBuilder 用于构建一个 Table 的所有 filters
// FilterBlockBuilder 会生成一个 string, 并作为一个特殊的 block 存储在 Table 中
// 
// 对 FilterBlockBuilder 的调用顺序必须符合如下 regexp 所表示的顺序:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder
{
public:
    explicit FilterBlockBuilder(const FilterPolicy*);

    FilterBlockBuilder(const FilterBlockBuilder&) = delete;
    FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

    void StartBlock(uint64_t block_offset);
    void AddKey(const Slice& key);
    Slice Finish();

private:
    void GenerateFilter();

    const FilterPolicy* policy_;
    std::string keys_;             // Flattened key contents
    std::vector<size_t> start_;    // 每个 key 在 `keys_` 中的 Starting offset
    std::string result_;           // 目前为止计算出的 filter data
    std::vector<Slice> tmp_keys_;  // 用于 policy_->CreateFilter() 的参数
    std::vector<uint32_t> filter_offsets_;  // 各 filters 在 `result_` 中的位置
};

class FilterBlockReader
{
public:
    // REQUIRES: *this 处于 live 时, "contents" 和 *policy 必须保持 live
    FilterBlockReader(const FilterPolicy* policy, const Slice& contents);

    bool KeyMayMatch(uint64_t block_offset, const Slice& key);

private:
    const FilterPolicy* policy_;
    const char* data_;    // 指向 filter data (即 block-start)
    const char* offset_;  // 指向 offset array 的起始 (即 block-end)
    size_t num_;          // offset array 中的 entries 数量
    size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
