// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/iterator.h"

namespace leveldb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;


// Table 是一个从 strings 到 strings 的有序映射
// Tables 是 immutable 且 persistent 的.
// Table 是线程安全的, 不需要 external synchronization.
// Table 对象可以理解为 Table 的 Reader
class LEVELDB_EXPORT Table {
public:
    // 尝试打开存储在 file 中, 范围在 [0...file_size] bytes 的 table,
    // 并读取必要的 metadata entries，以便从 table 中检索数据.
    // 
    // 如果成功，返回 ok 并将 *table 置为新打开的 table. client 应在不再需要时删除 *table。
    // 如果初始化 table 时出现错误，*table 会置为 nullptr 并返回 non-ok status.
    // table 不占用 *source 的 ownership，但是 client 必须确保 source 在返回的 table 的有效期内保持 live
    // 
    // *file must remain live while this Table is in use.
    static Status Open(const Options& options,
                       RandomAccessFile* file, uint64_t file_size,
                       Table** table);

    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;

    ~Table();

    // 返回一个读取 table contents 的 iterator
    // NewIterator() 返回的 Iterator 最初是无效的, caller 需在使用它之前调用 Seek()
    Iterator* NewIterator(const ReadOptions&) const;

    // 返回入参的 key 的 data 在文件中的 approximate byte offset
    // 返回值是以 file bytes 为单位的，因此包括了对数据做压缩等操作的效果
    // 如果找到了会返回 key 所在的 datablock 的 offset
    // 否则返回 metaindex block 的 offset
    uint64_t ApproximateOffsetOf(const Slice& key) const;

private:
    friend class TableCache;
    struct Rep;

    static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

    explicit Table(Rep* rep) : rep_(rep) {}

    // 在调用 Seek(key) 之后对找到的 entry 调用 (*handle_result)(arg, ...)
    // 如果 filter policy 显示 key 不存在则不能进行这样的调用
    // Used by TableCache
    Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
                       void (*handle_result)(void* arg, const Slice& k, const Slice& v));

    void ReadMeta(const Footer& footer);
    void ReadFilter(const Slice& filter_handle_value);

    Rep* const rep_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_
