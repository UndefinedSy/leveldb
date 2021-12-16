// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <cstddef>
#include <cstdint>

#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;
class Comparator;

// Block 相当于是 Block Reader, 用于 Decodes block builder 所生成的 blocks
class Block {
public:
    // Initialize the block with the specified contents.
    explicit Block(const BlockContents& contents);

    Block(const Block&) = delete;
    Block& operator=(const Block&) = delete;

    ~Block();

    size_t size() const { return size_; }
    Iterator* NewIterator(const Comparator* comparator);

private:
    class Iter;

    // 解析该 Block 的最后一个 uint32, 即 num_restarts
    uint32_t NumRestarts() const;

    const char* data_;
    size_t size_;              // 该 Block 的大小
    uint32_t restart_offset_;  // Offset in data_ of restart array
    bool owned_;               // Block owns data_[], 即 Block 应负责 delete[]
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
