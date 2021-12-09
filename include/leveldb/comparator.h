// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_COMPARATOR_H_
#define STORAGE_LEVELDB_INCLUDE_COMPARATOR_H_

#include <string>

#include "leveldb/export.h"

namespace leveldb {

class Slice;

// A Comparator object provides a total order across slices that are
// used as keys in an sstable or a database.  A Comparator implementation
// must be thread-safe since leveldb may invoke its methods concurrently
// from multiple threads.
// Comparator 对象用于提供一个在 sstable 或 db 中作为 key 的 Slice 的顺序
// Comparator implementation 必须是线程安全的
class LEVELDB_EXPORT Comparator {
public:
    virtual ~Comparator();

    // Three-way comparison.  Returns value:
    //   < 0  iff "a" < "b",
    //   == 0 iff "a" == "b",
    //   > 0  iff "a" > "b"
    virtual int Compare(const Slice& a, const Slice& b) const = 0;

    // The name of the comparator.
	// 用于检查 Comparator 是否不匹配
	// 即使用一个 Comparator 创建一个 DB，又使用一个不同的 Comparator 访问
    //
	// 每当 Comparator 的实现发生变更，且会导致任何两个 keys 的相对顺序发生变化时
	// 应该切换到另一个新的 Name
    //
    // 以 "leveldb" 起始的 Names 是保留字，不应被使用
    virtual const char* Name() const = 0;

    // Advanced functions: these are used to reduce the space requirements
    // for internal data structures like index blocks.

    // If *start < limit, changes *start to a short string in [start,limit).
    // Simple comparator implementations may return with *start unchanged,
    // i.e., an implementation of this method that does nothing is correct.
    virtual void FindShortestSeparator(std::string* start,
                                      const Slice& limit) const = 0;

    // Changes *key to a short string >= *key.
    // Simple comparator implementations may return with *key unchanged,
    // i.e., an implementation of this method that does nothing is correct.
    virtual void FindShortSuccessor(std::string* key) const = 0;
};

// Return a builtin comparator that uses lexicographic byte-wise
// ordering.  The result remains the property of this module and
// must not be deleted.
LEVELDB_EXPORT const Comparator* BytewiseComparator();

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_COMPARATOR_H_
