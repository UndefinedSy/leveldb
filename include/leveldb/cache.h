// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

// Cache 有 internal sync 机制，是多线程安全的。
// Cache 会自动驱逐 entries。
// Value 对于 Cache 的容量有一定的 charge。
// 例如，一个值为可变长字符串的 Cache，可以使用该字符串的长度作为其 charge。

// leveldb 提供了一个内建的缓存实现，其使用了一个 LRU 的驱逐策略。
// Clients 可以使用自己的 Cache 实现（如 scan-resistance、自定义的驱逐策略、可变的缓存大小等）。

#ifndef STORAGE_LEVELDB_INCLUDE_CACHE_H_
#define STORAGE_LEVELDB_INCLUDE_CACHE_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/slice.h"

namespace leveldb {

class LEVELDB_EXPORT Cache;

// Create a new cache with a fixed size capacity.  This implementation
// of Cache uses a least-recently-used eviction policy.
LEVELDB_EXPORT Cache* NewLRUCache(size_t capacity);

class LEVELDB_EXPORT Cache {
public:
    Cache() = default;

    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;

    // Destroys all existing entries by calling the "deleter"
    // function that was passed to the constructor.
    virtual ~Cache();

    // Opaque handle to an entry stored in the cache.
    struct Handle {};

	// 该操作将 key->value 的 mapping 插入到 cache 中，
	// 并根据总 cache 容量为其分配指定的 charge。
	// 
	// 该操作返回一个对应于该 mapping 的句柄。
	// 当调用者不再需要这个 mapping 时，必须调用 this->Release(handle)。
	// 
	// 当这个被插入的 entry 不再被需要时，key 和 value 将传递给 "deleter"。
    virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                          void (*deleter)(const Slice& key, void* value)) = 0;

    // If the cache has no mapping for "key", returns nullptr.
    //
    // Else return a handle that corresponds to the mapping.  The caller
    // must call this->Release(handle) when the returned mapping is no
    // longer needed.
    virtual Handle* Lookup(const Slice& key) = 0;

    // Release a mapping returned by a previous Lookup().
    // REQUIRES: handle must not have been released yet.
    // REQUIRES: handle must have been returned by a method on *this.
    virtual void Release(Handle* handle) = 0;

    // Return the value encapsulated in a handle returned by a
    // successful Lookup().
    // REQUIRES: handle must not have been released yet.
    // REQUIRES: handle must have been returned by a method on *this.
    virtual void* Value(Handle* handle) = 0;

    // If the cache contains entry for key, erase it.  Note that the
    // underlying entry will be kept around until all existing handles
    // to it have been released.
    virtual void Erase(const Slice& key) = 0;

    // Return a new numeric id.  May be used by multiple clients who are
    // sharing the same cache to partition the key space.  Typically the
    // client will allocate a new id at startup and prepend the id to
    // its cache keys.
    virtual uint64_t NewId() = 0;

    // Remove all cache entries that are not actively in use.  Memory-constrained
    // applications may wish to call this method to reduce memory usage.
    // Default implementation of Prune() does nothing.  Subclasses are strongly
    // encouraged to override the default implementation.  A future release of
    // leveldb may change Prune() to a pure abstract method.
    virtual void Prune() {}

    // Return an estimate of the combined charges of all elements stored in the
    // cache.
    virtual size_t TotalCharge() const = 0;

private:
	void LRU_Remove(Handle* e);
	void LRU_Append(Handle* e);
	void Unref(Handle* e);

	struct Rep;
	Rep* rep_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_CACHE_H_
