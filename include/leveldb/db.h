// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_DB_H_
#define STORAGE_LEVELDB_INCLUDE_DB_H_

#include <cstdint>
#include <cstdio>

#include "leveldb/export.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"

namespace leveldb {

// Update CMakeLists.txt if you change these
static const int kMajorVersion = 1;
static const int kMinorVersion = 23;

struct Options;
struct ReadOptions;
struct WriteOptions;
class WriteBatch;

// Abstract handle to particular state of a DB.
// A Snapshot is an immutable object and can therefore be safely
// accessed from multiple threads without any external synchronization.
class LEVELDB_EXPORT Snapshot {
protected:
    virtual ~Snapshot();
};

// A range of keys
struct LEVELDB_EXPORT Range {
	Range() = default;
	Range(const Slice& s, const Slice& l) : start(s), limit(l) {}

	Slice start;  // Included in the range
	Slice limit;  // Not included in the range
};


// DB 是并发访问安全的，不需要任何外部的 synchronization
class LEVELDB_EXPORT DB {
public:
	// 用于打开一个以 name 标识的 DB，调用者应该在不需要 DB 时 delete *dbptr
	// 
	// @param name[IN], 要打开的 DB 的 name
	// @param dbptr[OUT], 存储一个指向 heap-allocated DB 的指针
	// @return 发生错误是会返回一个 non-OK status，并且 dbptr 会是一个 nullptr
	static Status Open(const Options& options,
					   const std::string& name, DB** dbptr);

	DB() = default;

	DB(const DB&) = delete;
	DB& operator=(const DB&) = delete;

	virtual ~DB();

	// Set the database entry for "key" to "value".
	// Returns OK on success, and a non-OK status on error.
	// Note: consider setting options.sync = true.
	virtual Status Put(const WriteOptions& options,
					   const Slice& key, const Slice& value) = 0;

	// Remove the database entry (if any) for "key".
	// Returns OK on success, and a non-OK status on error.
	// It is not an error if "key" did not exist in the database.
	// Note: consider setting options.sync = true.
	// 当 DB 中不存在传入的 key 时不被认为是一个 error
	virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;

	// Apply the specified updates to the database.
	// Returns OK on success, non-OK on failure.
	// Note: consider setting options.sync = true.
	virtual Status Write(const WriteOptions& options, WriteBatch* updates) = 0;

	// If the database contains an entry for "key" store the
	// corresponding value in *value and return OK.
	//
	// If there is no entry for "key" leave *value unchanged and return
	// a status for which Status::IsNotFound() returns true.
	//
	// May return some other Status on an error.
	// @param key[IN]
	// @param value[OUT], 如果存在 key，则会在 value 中存入对应的 value
	// 					  如果 key 不存在，则 *value 不会**修改**
	// @return 当未发生错误，且 key 存在时返回 OK
	// 		   当未发生错误，且 key 不存在时返回状态 Status::IsNotFound() 为 true
	// 		   其他错误会有别的 Status
	virtual Status Get(const ReadOptions& options,
					   const Slice& key, std::string* value) = 0;

	// 获取一个用于遍历 DB 的 iterator，调用者应在不再使用时 delete iterator
	// 在 DB 被删除之前应该先将返回的 iterator 删除。
	//
	// @return, 返回一个遍历 DB 的 heap-allocated iterator
	// 			NewIterator() 的结果最初是 invalid, 
	// 			调用者必须在使用 Iterator 之前调用一个 Seek 方法。
	virtual Iterator* NewIterator(const ReadOptions& options) = 0;

    // 返回一个当前 DB 状态的 handle
    // 在这个 handle 上创建的 Iterator 会看到一个 stable snapshot
    // 当这个 snapshot 不再需要时应调用 ReleaseSnapshot(result)
    virtual const Snapshot* GetSnapshot() = 0;

  // Release a previously acquired snapshot.  The caller must not
  // use "snapshot" after this call.
  virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;

  // DB implementations can export properties about their state
  // via this method.  If "property" is a valid property understood by this
  // DB implementation, fills "*value" with its current value and returns
  // true.  Otherwise returns false.
  //
  //
  // Valid property names include:
  //
  //  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
  //     where <N> is an ASCII representation of a level number (e.g. "0").
  //  "leveldb.stats" - returns a multi-line string that describes statistics
  //     about the internal operation of the DB.
  //  "leveldb.sstables" - returns a multi-line string that describes all
  //     of the sstables that make up the db contents.
  //  "leveldb.approximate-memory-usage" - returns the approximate number of
  //     bytes of memory in use by the DB.
  virtual bool GetProperty(const Slice& property, std::string* value) = 0;

  // For each i in [0,n-1], store in "sizes[i]", the approximate
  // file system space used by keys in "[range[i].start .. range[i].limit)".
  //
  // Note that the returned sizes measure file system space usage, so
  // if the user data compresses by a factor of ten, the returned
  // sizes will be one-tenth the size of the corresponding user data size.
  //
  // The results may not include the sizes of recently written data.
  virtual void GetApproximateSizes(const Range* range, int n,
                                   uint64_t* sizes) = 0;

  // Compact the underlying storage for the key range [*begin,*end].
  // In particular, deleted and overwritten versions are discarded,
  // and the data is rearranged to reduce the cost of operations
  // needed to access the data.  This operation should typically only
  // be invoked by users who understand the underlying implementation.
  //
  // begin==nullptr is treated as a key before all keys in the database.
  // end==nullptr is treated as a key after all keys in the database.
  // Therefore the following call will compact the entire database:
  //    db->CompactRange(nullptr, nullptr);
  virtual void CompactRange(const Slice* begin, const Slice* end) = 0;
};

// 销毁指定 DB 的数据（应小心使用）
//
// Note: 为了向后兼容，如果 DestroyDB 无法 list 数据库文件，
// 仍会返回 Status::OK() 以 masking 该 failure。
LEVELDB_EXPORT Status DestroyDB(const std::string& name,
                                const Options& options);

// 如果一个 DB 无法打开，可以尝试调用此方法以尽可能多地恢复数据库的内容。
// 不过仍然会有一些数据可能丢失，因此在包含重要信息的数据库上调用此函数时要小心。
LEVELDB_EXPORT Status RepairDB(const std::string& dbname,
                               const Options& options);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_DB_H_
