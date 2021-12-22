// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include <cstdint>
#include <string>

#include "db/dbformat.h"
#include "leveldb/cache.h"
#include "leveldb/table.h"
#include "port/port.h"

namespace leveldb {

class Env;

class TableCache
{
public:
    TableCache(const std::string& dbname, const Options& options, int entries);
    ~TableCache();

    // 返回一个 file_number 所对应的 iterator（对应文件的 length 必须恰好是 file_size）
    // 如果 tableptr 是一个 non-null，将 *tableptr 指向返回的 iter 对应的 Table 对象
    // 如果该 iter 底下没有 Table 对象则将 tableptr 置为 nullptr
    // cache 拥有这个返回的 *tableptr 的所有权
    // *tableptr 对象不应被删除, 只要返回的 iter 仍然 live, 则 *tableptr 应是 valid
    Iterator* NewIterator(const ReadOptions& options,
                          uint64_t file_number, uint64_t file_size,
                          Table** tableptr = nullptr);

    // 如果对指定文件中的 internal key "k" 的 seek 找到一个 entry
    // 则调用 (*handle_result)(arg, found_key, found_value)。
    Status Get(const ReadOptions& options, uint64_t file_number,
                uint64_t file_size, const Slice& k, void* arg,
                void (*handle_result)(void*, const Slice&, const Slice&));

    // Evict any entry for the specified file number
    void Evict(uint64_t file_number);

private:
    Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);

    Env* const env_;
    const std::string dbname_;
    const Options& options_;
    Cache* cache_;  // LRU by default
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_
