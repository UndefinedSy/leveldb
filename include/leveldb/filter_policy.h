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


// 返回一个新的使用 bloom filter 的 filter policy
// 该 filter policy 对于每个 key 大约会有 bit_per_key 个 bits
// bits_per_key 一个很好的值是 10，这样的 filter policy 的 false positive rate ~1%
// 
// 调用者必须在任何使用该 result 的 Database 关闭后删除该 result
// 
// 注意：如果使用的自定义 comparator 会忽略掉 keys 的某些部分
// 则不能使用 NewBloomFilterPolicy(), 必须提供自定义 FilterPolicy, 以忽略 keys 的相应部分
// 例如，一个 comparator 会忽略尾部的空格，那么使用一个不会忽略键 keys 中尾部空格
// 的 FilterPolicy（如 NewBloomFilterPolicy()）是不正确的
LEVELDB_EXPORT const FilterPolicy* NewBloomFilterPolicy(int bits_per_key);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_FILTER_POLICY_H_
