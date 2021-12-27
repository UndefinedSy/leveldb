// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic>
#include <deque>
#include <set>
#include <string>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB
{
public:
    DBImpl(const Options& options, const std::string& dbname);

    DBImpl(const DBImpl&) = delete;
    DBImpl& operator=(const DBImpl&) = delete;

    ~DBImpl() override;

    // Implementations of the DB interface
    Status Put(const WriteOptions&, const Slice& key,
               const Slice& value) override;
    Status Delete(const WriteOptions&, const Slice& key) override;
    Status Write(const WriteOptions& options, WriteBatch* updates) override;
    Status Get(const ReadOptions& options, const Slice& key,
               std::string* value) override;
    Iterator* NewIterator(const ReadOptions&) override;
    const Snapshot* GetSnapshot() override;
    void ReleaseSnapshot(const Snapshot* snapshot) override;
    bool GetProperty(const Slice& property, std::string* value) override;
    void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
    void CompactRange(const Slice* begin, const Slice* end) override;

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

private:
    friend class DB;
    struct CompactionState;
    struct Writer;

    // Information for a manual compaction
    struct ManualCompaction {
        int level;
        bool done;
        const InternalKey* begin;  // null means beginning of key range
        const InternalKey* end;    // null means end of key range
        InternalKey tmp_storage;   // Used to keep track of compaction progress
    };

    // Per level compaction stats.  stats_[level] stores the stats for
    // compactions that produced data for the specified "level".
    struct CompactionStats {
        CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

        void Add(const CompactionStats& c) {
            this->micros += c.micros;
            this->bytes_read += c.bytes_read;
            this->bytes_written += c.bytes_written;
        }

        int64_t micros;
        int64_t bytes_read;
        int64_t bytes_written;
    };

    Iterator* NewInternalIterator(const ReadOptions&,
                                  SequenceNumber* latest_snapshot,
                                  uint32_t* seed);

    Status NewDB();

    // 从持久存储中恢复该 fd 的数据。这可能需要进行大量工作来恢复最近记录的更新。
    // @param edit, 对这个 fd 所做的任何修改都会添加到 *edit。
    Status Recover(VersionEdit* edit, bool* save_manifest) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    void MaybeIgnoreError(Status* s) const;

    // Delete any unneeded files and stale in-memory entries.
    void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    // 将 in-memory write buffer 给 compact 到磁盘
    // 切换到一个新的 log-file/memtable 并开始写新的 fd (如果没有出错)
    // 如果 compaction 出错会通过 bg_error_ 上报
    void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                          VersionEdit* edit, SequenceNumber* max_sequence)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    Status MakeRoomForWrite(bool force /* compact even if there is room? */)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    WriteBatch* BuildBatchGroup(Writer** last_writer)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    void RecordBackgroundError(const Status& s);

    void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    static void BGWork(void* db);
    void BackgroundCall();
    void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    void CleanupCompaction(CompactionState* compact)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    Status DoCompactionWork(CompactionState* compact)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    Status OpenCompactionOutputFile(CompactionState* compact);
    Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
    Status InstallCompactionResults(CompactionState* compact)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    const Comparator* user_comparator() const {
        return internal_comparator_.user_comparator();
    }

    // Constant after construction
    Env* const env_;
    const InternalKeyComparator internal_comparator_;
    const InternalFilterPolicy internal_filter_policy_;
    const Options options_;  // options_.comparator == &internal_comparator_
    const bool owns_info_log_;
    const bool owns_cache_;
    const std::string dbname_;

    // table_cache_ provides its own synchronization
    TableCache* const table_cache_; // table cache 自身保证了线程安全

    // Lock over the persistent DB state.  Non-null iff successfully acquired.
    FileLock* db_lock_;

    // State below is protected by mutex_
    port::Mutex mutex_;
    std::atomic<bool> shutting_down_;
    port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
    MemTable* mem_;
    MemTable* imm_ GUARDED_BY(mutex_);  // Memtable being compacted
    std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_
    WritableFile* logfile_; // log 文件
    uint64_t logfile_number_ GUARDED_BY(mutex_);    // log file 的 file num
    log::Writer* log_;  // log writer
    uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.

    // Queue of writers.
    std::deque<Writer*> writers_ GUARDED_BY(mutex_);
    WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

    SnapshotList snapshots_ GUARDED_BY(mutex_);

    // 需要保护的 table files 的集合，因为这些 table files 属于 ongoing compactions 的一部分。
    std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

    // 是否有 background compaction 已被调度或正在运行
    bool background_compaction_scheduled_ GUARDED_BY(mutex_);

    ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);

    VersionSet* const versions_ GUARDED_BY(mutex_);

    // Have we encountered a background error in paranoid mode?
    Status bg_error_ GUARDED_BY(mutex_);

    // stats_[level] 存储为指定“级别”生成数据的压缩的统计信息。
    CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
