// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A database can be configured with a custom FilterPolicy object.
// This object is responsible for creating a small filter from a set
// of keys.  These filters are stored in leveldb and are consulted
// automatically by leveldb to decide whether or not to read some
// information from disk. In many cases, a filter can cut down the
// number of disk seeks form a handful to a single disk seek per
// DB::Get() call.
//
// Most people will want to use the builtin bloom filter support (see
// NewBloomFilterPolicy() below).

#ifndef STORAGE_LEVELDB_INCLUDE_FILTER_POLICY_H_
#define STORAGE_LEVELDB_INCLUDE_FILTER_POLICY_H_

#include <string>

#include "leveldb/export.h"

namespace leveldb {

class Slice;

class LEVELDB_EXPORT FilterPolicy {
public:
    virtual ~FilterPolicy();

    // 返回该 filter policy 的 name
    // 注意，如果该 filter 的编码以不兼容的方式发生了改变，则这个方法返回的名称必须改变
    // 否则，旧的不兼容的 filters 可能被传递给新的 type 的相应的 methods
    virtual const char* Name() const = 0;

    // keys[0,n-1] 包含一个 keys list(可能重复), 这些 keys 按用户提供的 comparator 排序
    // 该方法将一个 summarize keys[0,n-1] 的 filter 给 append 到 *dst
    // 
    // 警告：不要改变 *dst 的初始内容, 总是应该将新构建的 filter 给 append 到 *dst
    virtual void CreateFilter(const Slice* keys, int n,
                              std::string* dst) const = 0;

    virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const = 0;
};

// Return a new filter policy that uses a bloom filter with approximately
// the specified number of bits per key.  A good value for bits_per_key
// is 10, which yields a filter with ~ 1% false positive rate.
//
// Callers must delete the result after any database that is using the
// result has been closed.
//
// Note: if you are using a custom comparator that ignores some parts
// of the keys being compared, you must not use NewBloomFilterPolicy()
// and must provide your own FilterPolicy that also ignores the
// corresponding parts of the keys.  For example, if the comparator
// ignores trailing spaces, it would be incorrect to use a
// FilterPolicy (like NewBloomFilterPolicy) that does not ignore
// trailing spaces in keys.
LEVELDB_EXPORT const FilterPolicy* NewBloomFilterPolicy(int bits_per_key);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_FILTER_POLICY_H_
