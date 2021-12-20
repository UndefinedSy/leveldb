// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.

// TableBuilder 提供了构建 Table 的接口, 即 Table 的 Writer
// (Table 是一个 immutable 的, 有序的 key-values)
// 
// 多线程可以在不需要 external synchronization 的情况下对一个 TableBuilder 调用 const methods
// 但是如果任何一个线程可能会调用一个 non-const method，
// 则所有访问该 TableBuilder 的线程必须使用 external synchronization


#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb {

class BlockBuilder;
class BlockHandle;
class WritableFile;

class LEVELDB_EXPORT TableBuilder {
public:

    // 创建一个 builder，用于将其正在构建的 table 的 contents 存储到 *file 中
    // Builder 不会去 close file, 调用者应在调用 Finish() 后自己关闭该文件
    TableBuilder(const Options& options, WritableFile* file);

    TableBuilder(const TableBuilder&) = delete;
    TableBuilder& operator=(const TableBuilder&) = delete;

    // REQUIRES: Either Finish() or Abandon() has been called.
    ~TableBuilder();

    // Change the options used by this builder
    // 注意：只有部分 options 可在构造后进行改变
    // 如果某个 option field 是不允许动态修改的，并尝试通过该调用修改对应 field 
    // 则将返回一个 error，且不会改变任何字段
    Status ChangeOptions(const Options& options);

    // Add <key>, <value> to the table being constructed.
    // REQUIRES: 入参 key 通过 comparator 应该是排在任何之前已有 key 之后的
    // REQUIRES: Finish(), Abandon() have not been called
    void Add(const Slice& key, const Slice& value);

    // Advanced operation: flush any buffered key/value pairs to file.
    // Flush 可以用来确保两个相邻的 entries 不会出现在同一个 data block 中。
    // 大多数的 user case 下都应该不需要使用这种方法
    // REQUIRES: Finish(), Abandon() have not been called
    void Flush();

    // Return non-ok iff some error has been detected.
    Status status() const;

    // Finish building the table.
    // 在此函数调用返回后，应停止使用传递给该 builder 构造函数的 file
    // REQUIRES: Finish(), Abandon() have not been called
    Status Finish();

    // Abandon() 用于表示丢弃该 Builder 的 contents
    // 在此函数调用返回后，应停止使用传递给该 builder 构造函数的 file
    // 如果调用者不打算调用 Finish()，则必须在析构该 Builder 之前调用 Abandon()
    // REQUIRES: Finish(), Abandon() have not been called
    void Abandon();

    // Number of calls to Add() so far.
    uint64_t NumEntries() const;

    // Size of the file generated so far.  If invoked after a successful
    // Finish() call, returns the size of the final generated file.
    uint64_t FileSize() const;

private:
    bool ok() const { return status().ok(); }
    void WriteBlock(BlockBuilder* block, BlockHandle* handle);
    void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle);

    struct Rep;
    Rep* rep_;  // Table Builder 都封在一个 Rep 中
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_
