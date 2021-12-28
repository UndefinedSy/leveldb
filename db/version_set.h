// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// VersionSet 是一个由一组 Versions 组成的一个 DBImpl
// 最新的 Version 称为 CURRENT
// 对于较旧的 Versions 可能仍然会被保留，以提供对 live iterators 的一致性视图
// 
// 每个 Version 都会 track 每个 level 的一组 Table files
// VersionSet 是所有 Versions 的集合
// 
// Version, VersionSet 都是 thread-compatible 的，但所有的访问都需要 external sync

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

namespace log {
class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// 返回最小的 index `i`, i 应满足 files[i]->largest >= key
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

// Version 是一个 sstable files 的集合，以及它管理的 compaction 状态
class Version {
public:
	struct GetStats {
		FileMetaData* seek_file;
		int seek_file_level;
	};

    // Append to *iters a sequence of iterators that will
    // yield the contents of this Version when merged together.
    // 向 *iters 中 append 一系列的 iterators
    // 这些 iterators 会在 merge 的时候生成该 Version 的内容
    // REQUIRES: This version has been saved (see VersionSet::SaveTo)
    void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

    // 根据 key 查找对应的 value
    // 如果找到则存入到 *val, 并返回 OK
    // 如果没有找到则返回 non-OK. Fills *stats.
    // REQUIRES: lock is not held
    Status Get(const ReadOptions&, const LookupKey& key,
               std::string* val, GetStats* stats);

    // 将 "stat" 添加到 current state
    // 如果可能需要触发 compaction 则返回 true，否则返回 false。
    // REQUIRES: lock is held
    bool UpdateStats(const GetStats& stats);

    // Record a sample of bytes read at the specified internal key.
    // Samples are taken approximately once every config::kReadBytesPeriod
    // bytes.  Returns true if a new compaction may need to be triggered.
    // 记录在传入的 internal key 上读取到的 sample 的 bytes
    // 大约每 config::kReadBytesPeriod 个 bytes 会取一次 samples
    // 如果需要出发一次 compaction 则返回 true
    // REQUIRES: lock is held
    bool RecordReadSample(Slice key);

    // Reference count management (so Versions do not disappear out from
    // under live iterators)
    void Ref();
    void Unref();

    void GetOverlappingInputs(
        int level,
        const InternalKey* begin,  // nullptr means before all keys
        const InternalKey* end,    // nullptr means after all keys
        std::vector<FileMetaData*>* inputs);

    // 如果在 `level` 中有文件与 [*smallest_user_key, *largest_user_key] 存在 overlap 则返回 true  
    // smallest_user_key == nullptr 表示 DB 中最小的 key
    // largest_user_key == nullptr 表示 DB 中最大的 key
    bool OverlapInLevel(int level,
                        const Slice* smallest_user_key,
                        const Slice* largest_user_key);

    // Return the level at which we should place a new memtable compaction
    // result that covers the range [smallest_user_key,largest_user_key].
    // 返回我们需要在哪个 level 上执行一次新的 memtable compaction，  
    // 该 compaction 覆盖了范围 [smallest_user_key, largest_user_key].  
    int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                   const Slice& largest_user_key);

    int NumFiles(int level) const { return files_[level].size(); }

    // Return a human readable string that describes this version's contents.
    std::string DebugString() const;

private:
	friend class Compaction;
	friend class VersionSet;

	class LevelFileNumIterator;

	explicit Version(VersionSet* vset)
		: vset_(vset)
		, next_(this)
		, prev_(this)
		, refs_(0)
		, file_to_compact_(nullptr)
		, file_to_compact_level_(-1)
		, compaction_score_(-1)
		, compaction_level_(-1) {}

	Version(const Version&) = delete;
	Version& operator=(const Version&) = delete;

	~Version();

	Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

    // 对每个与 user_key 存在 overlap 的文件, 按从新到旧的顺序调用 func(arg, level, f)
    // 如果某次调用 func() 返回 false，则不再继续调用
	//
	// REQUIRES: user portion of internal_key == user_key.
	void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
							bool (*func)(void*, int, FileMetaData*));

	VersionSet* vset_;  // VersionSet to which this Version belongs
	Version* next_;     // Next version in linked list
	Version* prev_;     // Previous version in linked list
	int refs_;          // Number of live refs to this version

	// 每层 level 的 sstable files
	std::vector<FileMetaData*> files_[config::kNumLevels];

	// Next file to compact based on seek stats.
	// 基于 seek stats 的下一个要 compact 的文件
	FileMetaData* file_to_compact_;
	int file_to_compact_level_;

	// Level that should be compacted next and its compaction score.
	// Score < 1 means compaction is not strictly needed.  These fields
	// are initialized by Finalize().
	// 接下来要被 compacted 的 level 和它的 compaction score
	// Score < 1 意味着并不是紧迫地需要进行 compaction
	// 这些字段是在 Finalize() 时初始化
	double compaction_score_;
	int compaction_level_;
};

class VersionSet {
public:
  VersionSet(const std::string& dbname, const Options* options,
             TableCache* table_cache, const InternalKeyComparator*);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  Status Recover(bool* save_manifest);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRange(int level, const InternalKey* begin,
                           const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const {
    Version* v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

private:
	class Builder;

	friend class Compaction;
	friend class Version;

	bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

	void Finalize(Version* v);

	void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
					InternalKey* largest);

	void GetRange2(const std::vector<FileMetaData*>& inputs1,
					const std::vector<FileMetaData*>& inputs2,
					InternalKey* smallest, InternalKey* largest);

	void SetupOtherInputs(Compaction* c);

	// Save current contents to *log
	Status WriteSnapshot(log::Writer* log);

	void AppendVersion(Version* v);

	Env* const env_;
	const std::string dbname_;
	const Options* const options_;
	TableCache* const table_cache_;
	const InternalKeyComparator icmp_;

	// DB 相关的元信息
	uint64_t next_file_number_;
	uint64_t manifest_file_number_;
	uint64_t last_sequence_;
	uint64_t log_number_;
	uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

	// Opened lazily, MANIFEST 相关
	WritableFile* descriptor_file_;
	log::Writer* descriptor_log_;

	// Opened lazily, Version 相关 
	Version dummy_versions_;  // Head of circular doubly-linked list of versions.
	Version* current_;        // == dummy_versions_.prev_

	// Per-level 的 key, 标识了该 level 下一次 compaction 的 start
	// 其或者是一个 empty string, 或者是一个有效的 InternalKey
	std::string compact_pointer_[config::kNumLevels];
};

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);

  int level_;
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit edit_;

  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<FileMetaData*> inputs_[2];  // The two sets of inputs

  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<FileMetaData*> grandparents_;
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
