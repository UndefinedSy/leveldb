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


// 如果 files 中存在某个文件, 其 user key range 与 [*smallest,*largest] 有重叠，则返回true。
// - smallest == nullptr 表示无穷小
// - largest == nullptr 表示无穷大
// 
// REQUIRES: disjoint_sorted_files 为 true 表示 files[] 中是有序的相互不重叠的 key range
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

    // 所有的 Versions 构成一个 Version Set, 其是一个 Version 为元素的双向链表
	VersionSet* vset_;
	Version* next_;     // Next version in linked list
	Version* prev_;     // Previous version in linked list
	int refs_;          // Number of live refs to this version

	// 该 Version 下每层 level 中的 sstable files
	std::vector<FileMetaData*> files_[config::kNumLevels];

	// 基于 seek stats 所决定的下一次 compaction 的元信息
    // 在 UpdateStats() 中更新
	FileMetaData* file_to_compact_;
	int file_to_compact_level_;

	// 接下来要被 compacted 的 level 和它的 compaction score, 用于判断是否需要进行 major compaction
	// Score < 1 意味着不需要立刻进行 compaction
	// 在 Finalize() 中初始化
	double compaction_score_;
	int compaction_level_;
};

class VersionSet
{
public:
    VersionSet(const std::string& dbname, const Options* options,
               TableCache* table_cache, const InternalKeyComparator*);
    VersionSet(const VersionSet&) = delete;
    VersionSet& operator=(const VersionSet&) = delete;

    ~VersionSet();

    // 将 *edit 应用于 current version，生成一个新的描述符
    // 该描述符既被保存到持久化状态，又被 install 为新的 current version
    // 在实际写入文件时将释放 *mu
    // REQUIRES: *mu is held on entry.
    // REQUIRES: no other thread concurrently calls LogAndApply()
    Status LogAndApply(VersionEdit* edit, port::Mutex* mu) EXCLUSIVE_LOCKS_REQUIRED(mu);

    // Recover the last saved descriptor from persistent storage.
    Status Recover(bool* save_manifest);

    // Return the current version.
    Version* current() const { return current_; }

    // Return the current manifest file number
    uint64_t ManifestFileNumber() const { return manifest_file_number_; }

    // Allocate and return a new file number
    uint64_t NewFileNumber() { return next_file_number_++; }

    // 安排重复使用 file_number，除非已经又分配了一个更新的 file num
    // REQUIRES: "file_number" was returned by a call to NewFileNumber().
    void ReuseFileNumber(uint64_t file_number)
    {
        if (next_file_number_ == file_number + 1)
            next_file_number_ = file_number;
    }

    // Return the number of Table files at the specified level.
    int NumLevelFiles(int level) const;

    // Return the combined file size of all files at the specified level.
    int64_t NumLevelBytes(int level) const;

    // Return the last sequence number.
    uint64_t LastSequence() const { return last_sequence_; }

    // Set the last sequence number to s.
    void SetLastSequence(uint64_t s)
    {
        assert(s >= last_sequence_);
        last_sequence_ = s;
    }

    // Mark the specified file number as used. 推高 next_file_number_
    void MarkFileNumberUsed(uint64_t number);

    // Return the current log file number.
    uint64_t LogNumber() const { return log_number_; }

    // 返回当前正在被 compacted 的 log file 的 file num.
    // 如果没有这样的 file 则返回 0
    uint64_t PrevLogNumber() const { return prev_log_number_; }

    Compaction* PickCompaction();

    // Return a compaction object for compacting the range [begin,end] in
    // the specified level.  Returns nullptr if there is nothing in that
    // level that overlaps the specified range.  Caller should delete
    // the result.
    // 返回一个 compaction 对象，用于压实指定级别中的[begin,end]范围。 如果该层中没有任何东西与指定范围重叠，则返回nullptr。 调用者应删除该结果。
    Compaction* CompactRange(int level,
                             const InternalKey* begin, const InternalKey* end);

    // 对于任一个 level >= 1 的 file, 返回 next level 中与其存在 overlapping 的最大字节数
    int64_t MaxNextLevelOverlappingBytes();

    // Create an iterator that reads over the compaction inputs for "*c".
    // The caller should delete the iterator when no longer needed.
    // 创建一个迭代器，读取 "*c "的压实输入。当不再需要时，调用者应该删除这个迭代器。
    Iterator* MakeInputIterator(Compaction* c);

    /**
     * 判断 current_ Version 是否需要 compaction, 以下情况下会返回 True
     * - 存在有一个 level 的 compaction_score_ > 1 (由于文件数或者文件大小)
     * - 存在有文件因为 seek 次数超过阈值
     */
    bool NeedsCompaction() const
    {
        Version* v = current_;
        return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
    }

    // 将任何 live version 中列出的所有 files 添加到 *live 中
    // 可能会改变一些 internal state
    void AddLiveFiles(std::set<uint64_t>* live);

    // 返回对于 Version `v` 的数据 `key` 在数据库中的近似的 offset。
    uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

    // 返回一个 human-readable 的简短 (single-line) 摘要，即每个 level 中的文件数量
    // 使用 *scratch 作为存储
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

	// TODO: 都什么含义
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

// `Compaction` 封装了关于一次 compaction 的信息
class Compaction
{
public:
    ~Compaction();

    // 返回将被 compacted 的 level-n.
    // Compaction 将根据 level-n 和 level-(n+1) 的 Inputs 做 merge 得到一组 level-(n+1) 文件
    int level() const { return level_; }

    // Return the object that holds the edits to the descriptor done
    // by this compaction.
    // 返回持有本次 compaction 所处理的 descriptor 的 VersionEdit
    VersionEdit* edit() { return &edit_; }

    // "which" must be either 0 or 1
    int num_input_files(int which) const { return inputs_[which].size(); }

    // Return the ith input file at "level()+which" ("which" must be 0 or 1).
    FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

    // Maximum size of files to build during this compaction.
    uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

    // 判断本次 compaction 是否为 trivial, 即只需要将单个的 input 文件移动到 next level
    // 而不需要做 merging 或者 splitting
    bool IsTrivialMove() const;

    // 将本次 Compaction 的所有 inputs 都作为 delete operations 添加到 *edit
    void AddInputDeletions(VersionEdit* edit);

    // 返回 Ture 表示, 我们现有的信息保证本次 compaction 在 level-(n+1) 中产生的数据
    // 不存在大于 level(n+1) 的层中
    bool IsBaseLevelForKey(const Slice& user_key);

    // 返回 True 表示我们应在处理 internal_key 之前就停止构建当前的 output
    bool ShouldStopBefore(const Slice& internal_key);

    // 一旦 Compacation 成功，释放本次 compaction 的 input version
    void ReleaseInputs();

private:
    friend class Version;
    friend class VersionSet;

    Compaction(const Options* options, int level);

    int level_;
    uint64_t max_output_file_size_;
    Version* input_version_;
    VersionEdit edit_;

    // 指代 level-n 和 level-(n+1)
    std::vector<FileMetaData*> inputs_[2];  // The two sets of inputs

    // 用于检查 overlapping grandparent 文件数量的相关元数据
    // (parent == level_(n+1), grandparent == level_(n+2)
    std::vector<FileMetaData*> grandparents_;
    size_t grandparent_index_;  // Index in grandparent_starts_
    bool seen_key_;             // Some output key has been seen
    int64_t overlapped_bytes_;  // current output 和 grandparent files 之间 overlap 的字节数

    // 用于实现 IsBaseLevelForKey 的 State

    // level_ptrs_ holds indices into input_version_->levels_: our state
    // is that we are positioned at one of the file ranges for each
    // higher level than the ones involved in this compaction (i.e. for
    // all L >= level_ + 2).
    // level_ptrs_ 持有 input_version_->levels_ 的索引:
    // 我们的状态是，我们被定位在比这次压实所涉及的级别更高的文件范围中的一个（即对所有L >= level_ + 2）。
    size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
