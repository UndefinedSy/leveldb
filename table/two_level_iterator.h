// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_
#define STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_

#include "leveldb/iterator.h"

namespace leveldb {

struct ReadOptions;

// Return a new two level iterator.  A two-level iterator contains an
// index iterator whose values point to a sequence of blocks where
// each block is itself a sequence of key,value pairs.  The returned
// two-level iterator yields the concatenation of all key/value pairs
// in the sequence of blocks.  Takes ownership of "index_iter" and
// will delete it when no longer needed.
//
// Uses a supplied function to convert an index_iter value into
// an iterator over the contents of the corresponding block.
/**
 * Return a new two-level iterator.
 * two-level iterator 包含一个 index iterator,
 * 其 values 指向一连串的 blocks, 每个 block 是一系列的 kv pairs
 * 返回的 two-level iterator 可以生成按照 blocks sequence 的所有 kv pairs 的串联
 * two-level iterator 持有 index_iter 的所有权，并应在不再需要时将其删除。
 * 
 * two-level iterator 会使用所提供的 func 来将 index_iter value 转换为
 * 遍历对应 block contents 的 iterator
 */
Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    Iterator* (*block_function)(void* arg, const ReadOptions& options, const Slice& index_value),
    void* arg,
    const ReadOptions& options);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_
