// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_MERGER_H_
#define STORAGE_LEVELDB_TABLE_MERGER_H_

namespace leveldb {

class Comparator;
class Iterator;

// Return an iterator that provided the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
//
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// REQUIRES: n >= 0
/**
 * 返回一个 iterator, 该 iterator 提供 children[0,n-1] 中数据的 union
 * 这个 iterator 拥有子 iters 的所有权, 并在其自身被析构时会析构子 iterator
 * 
 * 所生成的 iterator 不会负责对数据做去重, 即如果一个 key 出现在 K 个子 iters 中, 则会得到其 K 次
 * @param comparator[IN]
 * @param children[IN]
 * @param n[IN]
 * 
 */
Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_MERGER_H_
