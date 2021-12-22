// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
    RandomAccessFile* file;
    Table* table;
};

// Used by FindTable()
static void
DeleteEntry(const Slice& key, void* value) {
    TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
    delete tf->table;
    delete tf->file;
    delete tf;
}

/**
 * Used by NewIterator()
 * @param arg1, LRUCache
 * @param arg2, Cache handle
 */
static void
UnrefEntry(void* arg1, void* arg2) {
    Cache* cache = reinterpret_cast<Cache*>(arg1);
    Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
    cache->Release(h);
}

/**
 * @param entries, LRUCache size
 */
TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env)
    , dbname_(dbname)
    , options_(options)
    , cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

/**
 * 获取 file_number 对应的 cache handle
 * 这里 key 是 file_number, value 是这个 number 对应的 *file 和 *table
 * FindTable 后, <key>:<*file, *table> 会保证存储到 LRUCache
 */
Status
TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                      Cache::Handle** handle)
{
    Status s;
    char buf[sizeof(file_number)];
    EncodeFixed64(buf, file_number);
    Slice key(buf, sizeof(buf));
    // 找到 key 对应的 LRUCache 的分片
    *handle = cache_->Lookup(key);
    // 如果直接在 cache 中找到对应的 table 就直接返回了

    // 未能找到 file_number 对应的 cache
    if (*handle == nullptr)
    {
        // open ldb file
        std::string fname = TableFileName(dbname_, file_number);
        RandomAccessFile* file = nullptr;
        s = env_->NewRandomAccessFile(fname, &file);
        // open sst file if failed to open ldb file
        if (!s.ok())
        {
            std::string old_fname = SSTTableFileName(dbname_, file_number);
            if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
                s = Status::OK();
            }
        }

        Table* table = nullptr;
        // 打开了 ldb or sst file, open table object
        if (s.ok())
        {
            s = Table::Open(options_, file, file_size, &table);
        }

        // 如果未能 open file / open table
        if (!s.ok())
        {
            assert(table == nullptr);
            delete file;
            // 这里不会缓存 error result
            // 如果这个错误时暂时的, 或者有人会 repair 这个文件, 则之后可以恢复
        }
        else
        {
            TableAndFile* tf = new TableAndFile;
            tf->file = file;
            tf->table = table;
            // cache 的是 key 和其所在的 file & table
            *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
        }
    }
    return s;
}

/**
 * 尝试获取传入 file_number 对应的 table
 * @param tableptr[OUT], non-nullptr 时会被置为指向 file_number 对应 table
 * @return 成功时返回一个 file_number 对应 table 的 iter; 否则返回 Error iter
 */
Iterator*
TableCache::NewIterator(const ReadOptions& options,
                        uint64_t file_number, uint64_t file_size,
                        Table** tableptr)
{
    if (tableptr != nullptr)
        *tableptr = nullptr;

    Cache::Handle* handle = nullptr;
    Status s = FindTable(file_number, file_size, &handle);
    if (!s.ok())
        return NewErrorIterator(s);

    Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;

    Iterator* result = table->NewIterator(options);
    result->RegisterCleanup(&UnrefEntry, cache_, handle);
    
    if (tableptr != nullptr)
        *tableptr = table;
    return result;
}

/**
 * 尝试查找 file_number 对应的 table, 并在对应 table 上 Seek(k)
 * 如果找到了则会对 Seek 到的 kv 调用 handle_result
 */
Status
TableCache::Get(const ReadOptions& options,
                uint64_t file_number, uint64_t file_size,
                const Slice& k,
                void* arg,
                void (*handle_result)(void*, const Slice&, const Slice&))
{
    Cache::Handle* handle = nullptr;
    Status s = FindTable(file_number, file_size, &handle);
    if (s.ok())
    {
        Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
        s = t->InternalGet(options, k, arg, handle_result);
        cache_->Release(handle);
    }
    return s;
}

void
TableCache::Evict(uint64_t file_number)
{
    char buf[sizeof(file_number)];
    EncodeFixed64(buf, file_number);
    cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
