// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// VersionEdit 记录了 Version 之间的变化 δ, 表示增加了什么文件，或者删除了什么文件
// 
// 每次文件有变动时，leveldb 会把变动记录到一个 VersionEdit 变量中
// 然后通过 VersionEdit 把变动应用到 current version，
// 并把 current version 的快照，也就是 db 元信息保存到 MANIFEST 文件中
// 
// MANIFEST 文件组织形式也是以 VersionEdit 的形式写入的
// 它本身是一个 log 文件格式，一个 VersionEdit 就是一条log record

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

// 看起来像是一个 table file 的元信息
struct FileMetaData
{
    FileMetaData()
      : refs(0)
      , allowed_seeks(1 << 30)
      , file_size(0) {}

    int refs;
    int allowed_seeks;  	// Seeks allowed until compaction
    uint64_t number;
    uint64_t file_size;     // File size in bytes
    InternalKey smallest;   // Smallest internal key served by table
    InternalKey largest;    // Largest internal key served by table
};

class VersionEdit
{
public:
	VersionEdit() { Clear(); }
	~VersionEdit() = default;

	void Clear();

	void SetComparatorName(const Slice& name) {
		has_comparator_ = true;
		comparator_ = name.ToString();
	}

	void SetLogNumber(uint64_t num) {
		has_log_number_ = true;
		log_number_ = num;
	}

	void SetPrevLogNumber(uint64_t num) {
		has_prev_log_number_ = true;
		prev_log_number_ = num;
	}

	void SetNextFile(uint64_t num) {
		has_next_file_number_ = true;
		next_file_number_ = num;
	}

	void SetLastSequence(SequenceNumber seq) {
		has_last_sequence_ = true;
		last_sequence_ = seq;
	}

	// TODO: Compact point 是啥
	void SetCompactPointer(int level, const InternalKey& key) {
		compact_pointers_.push_back(std::make_pair(level, key));
	}

	// Add the specified file at the specified number.
	// 向 level 添加 file 所标识的 sstable 的文件信息
	// REQUIRES: This version has not been saved (see VersionSet::SaveTo)
	// REQUIRES: "smallest" and "largest" are smallest and largest keys in file
	void AddFile(int level, uint64_t file, uint64_t file_size,
				 const InternalKey& smallest, const InternalKey& largest)
	{
		FileMetaData f;
		f.number = file;
		f.file_size = file_size;
		f.smallest = smallest;
		f.largest = largest;
		new_files_.push_back(std::make_pair(level, f));
	}

	// Delete the specified "file" from the specified "level".
	// 从 level 中删除 file 所标识的 sstable file:
	void RemoveFile(int level, uint64_t file) {
		deleted_files_.insert(std::make_pair(level, file));
	}

	void EncodeTo(std::string* dst) const;
	Status DecodeFrom(const Slice& src);

	std::string DebugString() const;

private:
	friend class VersionSet;

	typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

	// TODO: log 和 file 的区别
	std::string comparator_;
	uint64_t log_number_;
	uint64_t prev_log_number_;
	uint64_t next_file_number_;
	SequenceNumber last_sequence_;
	bool has_comparator_;
	bool has_log_number_;
	bool has_prev_log_number_;
	bool has_next_file_number_;
	bool has_last_sequence_;

	// 各 level 的 compact 点
	std::vector<std::pair<int, InternalKey>> compact_pointers_;
	// deleted files <level, file_num>, 相对 base Version 删除的 files
	DeletedFileSet deleted_files_;
	// added files <level, FileMetaData>, 相对 base Version 增加的 files
	std::vector<std::pair<int, FileMetaData>> new_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
