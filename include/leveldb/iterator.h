// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.
//
// Multiple threads can invoke const methods on an Iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Iterator must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_ITERATOR_H_
#define STORAGE_LEVELDB_INCLUDE_ITERATOR_H_

#include "leveldb/export.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class LEVELDB_EXPORT Iterator
{
public:
    Iterator();

    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;

    virtual ~Iterator();

	// iterator 或者是定位到一个 kv pair 上的，或这是 invalid
	// 如果 iterator 是 valid 则返回 true
    virtual bool Valid() const = 0;

	// 定位到数据源中 firt key 的位置
	// 如果数据源不是 empty, 则该 iterator 在本调用后是 Valid()
    virtual void SeekToFirst() = 0;

    // 定位到数据源中 last key 的位置
	// 如果数据源不是 empty, 则该 iterator 在本调用后是 Valid()
    virtual void SeekToLast() = 0;

	// 定位到数据源中 >= taget 的第一个 key 的位置
	// 如果源文件中包含一个 >= taget 的 entry，则 iterator 在本调用后是 Valid()
    virtual void Seek(const Slice& target) = 0;

    // Moves to the next entry in the source.  After this call, Valid() is
    // true iff the iterator was not positioned at the last entry in the source.
    // REQUIRES: Valid()
    virtual void Next() = 0;

    // Moves to the previous entry in the source.  After this call, Valid() is
    // true iff the iterator was not positioned at the first entry in source.
    // REQUIRES: Valid()
    virtual void Prev() = 0;


	// 返回当前 entry 的 key
	// 返回的 slice 底层的存储只在下次修改该 iterator 之前有效
	// REQUIRES: Valid()
    virtual Slice key() const = 0;

	// 返回当前 entry 的 value
	// 返回的 slice 底层的存储只在下次修改该 iterator 之前有效
    // REQUIRES: Valid()
    virtual Slice value() const = 0;

    // If an error has occurred, return it.  Else return an ok status.
    virtual Status status() const = 0;

	// 允许 Client 注册三元组 "function/arg1/arg2"，当这个 iterator 析构时将调用该 Func
	// 
	// 注意，这个方法不是一个抽象函数，因此 clients 不应该 override
    using CleanupFunction = void (*)(void* arg1, void* arg2);
    void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);

private:
	// Cleanup functions are stored in a single-linked list.
	// The list's head node is inlined in the iterator.
	struct CleanupNode
	{
		// True if the node is not used. Only head nodes might be unused.
		bool IsEmpty() const { return function == nullptr; }
		// Invokes the cleanup function.
		void Run()
		{
			assert(function != nullptr);
			(*function)(arg1, arg2);
		}

		// The head node is used if the function pointer is not null.
		CleanupFunction function;
		void* arg1;
		void* arg2;
		CleanupNode* next;
	};
	CleanupNode cleanup_head_;
};

// Return an empty iterator (yields nothing).
LEVELDB_EXPORT Iterator* NewEmptyIterator();

// Return an empty iterator with the specified status.
LEVELDB_EXPORT Iterator* NewErrorIterator(const Status& status);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_ITERATOR_H_
