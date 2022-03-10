// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
#define STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_

#include <string>

#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {

class Slice;

class LEVELDB_EXPORT WriteBatch
{
public:
    class LEVELDB_EXPORT Handler {
    public:
        virtual ~Handler();
        virtual void Put(const Slice& key, const Slice& value) = 0;
        virtual void Delete(const Slice& key) = 0;
    };

    WriteBatch();

    // Intentionally copyable.
    WriteBatch(const WriteBatch&) = default;
    WriteBatch& operator=(const WriteBatch&) = default;

    ~WriteBatch();

    // 将 key->value 的 mapping 存储到数据库中
    void Put(const Slice& key, const Slice& value);

    // 如果数据库中包含有一个 key 的 mapping 则擦除该记录
    // 否则不会做任何操作
    void Delete(const Slice& key);

    // 清除该 batch 中所有 buffered updates
    void Clear();

    // The size of the database changes caused by this batch.
    //
    // This number is tied to implementation details, and may change across
    // releases. It is intended for LevelDB usage metrics.
    size_t ApproximateSize() const;

    // 将 source 种的 batch 拷贝到该 WriteBatch 中
    // 
    // 其时间复杂度为 O(source size). 常数因子比对 source batch 调用 Iterate()
    // 并使用一个 Handler 将 operations 复制到该 batch 要更好
    void Append(const WriteBatch& source);

    // Support for iterating over the contents of a batch.
    Status Iterate(Handler* handler) const;

private:
    friend class WriteBatchInternal;

    std::string rep_;  // See comment in write_batch.cc for the format of rep_
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
