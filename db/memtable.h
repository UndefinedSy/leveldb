// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include <string>

#include "db/dbformat.h"
#include "db/skiplist.h"
#include "leveldb/db.h"
#include "util/arena.h"

namespace leveldb {

class InternalKeyComparator;
class MemTableIterator;

// MemTables are reference counted.
// The initial reference count is 0
// the caller must call Ref() at least once.
class MemTable {
public:
	explicit MemTable(const InternalKeyComparator& comparator);

	MemTable(const MemTable&) = delete;
	MemTable& operator=(const MemTable&) = delete;

	// Increase reference count.
	void Ref() { ++refs_; }

	// Drop reference count.  Delete if no more references exist.
	void Unref() {
		--refs_;
		assert(refs_ >= 0);
		if (refs_ <= 0) {
			delete this;
		}
  	}

	// Returns an estimate of the number of bytes of data in use by this
	// data structure.
	// It is safe to call when MemTable is being modified.
	size_t ApproximateMemoryUsage();

	// 调用者必须确保在这个返回的 Iterator 仍存活时，底层 MemTable 也应保持存活。
	// 此 Iterator 返回的 key 是由 db/format.{h,cc} 模块中的 AppendInternalKey 编码的 internal keys。
	// 
	// @return, 一个获取 memtable 内容的 iterator。
	Iterator* NewIterator();

	// Add an entry into memtable that maps key to value at the
	// specified sequence number and with the specified type.
	// Typically value will be empty if type==kTypeDeletion.
	void Add(SequenceNumber seq, ValueType type,
			 const Slice& key, const Slice& value);

	// If memtable contains a value for key, store it in *value and return true.
	// If memtable contains a deletion for key, store a NotFound() error in *status and return true.
	// Else, return false.
	bool Get(const LookupKey& key, std::string* value, Status* s);

private:
	friend class MemTableIterator;
	friend class MemTableBackwardIterator;

	struct KeyComparator {
		const InternalKeyComparator comparator;
		explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
		int operator()(const char* a, const char* b) const;
	};

	typedef SkipList<const char*, KeyComparator> Table;

	~MemTable();  // Private since only Unref() should be used to delete it

	KeyComparator comparator_;
	int refs_;
	Arena arena_;
	Table table_;	// skipList
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
