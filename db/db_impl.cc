// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer
{
    explicit Writer(port::Mutex* mu)
        : batch(nullptr)
        , sync(false)
        , done(false)
        , cv(mu) {}

    Status status;
    WriteBatch* batch;
    bool sync;
    bool done;
    port::CondVar cv;
};

struct DBImpl::CompactionState {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env)
    , internal_comparator_(raw_options.comparator)
    , internal_filter_policy_(raw_options.filter_policy)
    , options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options))
    , owns_info_log_(options_.info_log != raw_options.info_log)
    , owns_cache_(options_.block_cache != raw_options.block_cache)
    , dbname_(dbname)
    , table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_)))
    , db_lock_(nullptr)
    , shutting_down_(false)
    , background_work_finished_signal_(&mutex_)
    , mem_(nullptr)
    , imm_(nullptr)
    , has_imm_(false)
    , logfile_(nullptr)
    , logfile_number_(0)
    , log_(nullptr)
    , seed_(0)
    , tmp_batch_(new WriteBatch)
    , background_compaction_scheduled_(false)
    , manual_compaction_(nullptr)
    , versions_(new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_))
    {}

DBImpl::~DBImpl()
{
    mutex_.Lock();
    shutting_down_.store(true, std::memory_order_release);
    // Wait for background work to finish.
    while (background_compaction_scheduled_) {
        background_work_finished_signal_.Wait();
    }
    mutex_.Unlock();

    // 释放这个 db 对应的文件锁
    if (db_lock_ != nullptr) {
        env_->UnlockFile(db_lock_);
    }

    delete versions_;
    if (mem_ != nullptr) mem_->Unref();
    if (imm_ != nullptr) imm_->Unref();
    delete tmp_batch_;
    delete log_;
    delete logfile_;
    delete table_cache_;

    if (owns_info_log_) delete options_.info_log;
    if (owns_cache_) delete options_.block_cache;
}

Status
DBImpl::NewDB() {
    VersionEdit new_db;
    new_db.SetComparatorName(user_comparator()->Name());
    new_db.SetLogNumber(0);
    new_db.SetNextFile(2);
    new_db.SetLastSequence(0);

    const std::string manifest = DescriptorFileName(dbname_, 1); /* dbname_/MANIFEST-000001 */
    WritableFile* file;
    Status s = env_->NewWritableFile(manifest, &file);
    if (!s.ok()) return s;

    {
        log::Writer log(file);
        std::string record;
        new_db.EncodeTo(&record);
        s = log.AddRecord(record);
        if (s.ok()) {
            s = file->Sync();
        }
        if (s.ok()) {
            s = file->Close();
        }
    }
    delete file;

    if (s.ok()) {
        // Make "CURRENT" file that points to the new manifest file.
        s = SetCurrentFile(env_, dbname_, 1);
    } else {
        env_->RemoveFile(manifest);
    }
    return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

/**
 * 删除不再需要的 files 以及 stale in-memory entries
 * 
 */
void DBImpl::RemoveObsoleteFiles()
{
    mutex_.AssertHeld();

    if (!bg_error_.ok())
    {
        // After a background error, we don't know whether a new version may
        // or may not have been committed, so we cannot safely garbage collect.
        return;
    }

    // Make a set of all of the live files
    // 获取当前所有仍在 VersionSer 的 files
    std::set<uint64_t> live = pending_outputs_;
    versions_->AddLiveFiles(&live);

    // 获取当前 dbname_ 目录下的所有文件
    std::vector<std::string> filenames;
    env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose

    uint64_t number;
    FileType type;
    std::vector<std::string> files_to_delete;
    for (std::string& filename : filenames)
    {
        if (ParseFileName(filename, &number, &type))
        {
            bool keep = true;
            switch (type)
            {
                case kLogFile:
                    keep = ((number >= versions_->LogNumber())
                            || (number == versions_->PrevLogNumber()));
                    break;
                case kDescriptorFile:
                    // 只需要保留自己的 MANIFES file 以及任何更新的 incarnations'(???)
                    // 以防存在 race condition, 这里也允许 other incarnations(???)
                    keep = (number >= versions_->ManifestFileNumber());
                    break;
                case kTableFile:
                    keep = (live.find(number) != live.end());
                    break;
                case kTempFile:
                    // Any temp files that are currently being written to must
                    // be recorded in pending_outputs_, which is inserted into "live"
                    keep = (live.find(number) != live.end());
                    break;
                case kCurrentFile:
                case kDBLockFile:
                case kInfoLogFile:
                    keep = true;
                    break;
            }

            if (!keep) {
                files_to_delete.push_back(std::move(filename));
                if (type == kTableFile) table_cache_->Evict(number);

                Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
                    static_cast<unsigned long long>(number));
            }
        }
    }

    // 删除文件时解锁, 避免阻塞其他线程
    // 所有被删除的文件都有唯一的名称, 不会与新创建的文件冲突, 因此可以安全地删除, 同时允许其他线程继续进行
    mutex_.Unlock();
    for (const std::string& filename : files_to_delete)
        env_->RemoveFile(dbname_ + "/" + filename);

    mutex_.Lock();
}

/**
 * 从持久存储中恢复 dbname_ 对应 db 的数据.
 * 这可能需要进行大量工作来恢复最近记录的更新.
 * @param edit[OUT], 对这个 fd 所做的任何修改都会添加到 *edit
 * @param save_manifest[OUT],
 */
Status
DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
    mutex_.AssertHeld();

    // 建目录 dbname_ && 上文件锁
    // 这里忽略 CreateDir 中的 error, 因为只有当 fd 已被创建了, 才会 commit 这个 DB 的创建
    // 而这个目录可能已经存在于之前失败的创建尝试中
    env_->CreateDir(dbname_);
    assert(db_lock_ == nullptr);
    Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
    if (!s.ok()) return s;

    if (!env_->FileExists(CurrentFileName(dbname_))) /* dbname_/CURRENT */
    {
        if (options_.create_if_missing)
        {
            Log(options_.info_log, "Creating DB %s since it was missing.",
                dbname_.c_str());

            s = NewDB();
            if (!s.ok()) return s;
        }
        else
        {
            return Status::InvalidArgument(
                dbname_, "does not exist (create_if_missing is false)");
        }
    }
    else
    {
        if (options_.error_if_exists) {
            return Status::InvalidArgument(dbname_, "exists (error_if_exists is true)");
        }
    }

    // 尝试从 CURRENT 的 manifest 中恢复 VersionSet
    s = versions_->Recover(save_manifest);
    if (!s.ok()) return s;

    SequenceNumber max_sequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of leveldb.
    // 尝试从所有比 MANIFEST 文件中记录的 log files 更新的文件中恢复
    // (这些更新的文件可能是由前一个版本所添加, 但还没有记录到 MANIFEST 中
    //  这种情况可能出现在 Memtable 或者 Immemtable 还没来得及写入
    //  SSTable file, db 就挂掉了)
    // 
    // 需要注意的是, PrevLogNumber() 是早版本 leveldb 的机制, 目前不再使用
    // 这里主要是为了保持向前兼容, 以防恢复一个由旧版本的 leveldb 产生的数据库
    const uint64_t min_log = versions_->LogNumber();
    const uint64_t prev_log = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    // 获取 dbname_ 目录下的所有文件
    s = env_->GetChildren(dbname_, &filenames);
    if (!s.ok()) return s;
    
    std::set<uint64_t> expected;
    versions_->AddLiveFiles(&expected);
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++)
    {
        // 这里会先从 expected 中移除目录下的文件名
        // 然后会将 > versions_->LogNumber() 的文件名加入到 logs
        // logs 中即是未被记录在 VersionSet 的 log files
        if (ParseFileName(filenames[i], &number, &type))
        {
            expected.erase(number);
            if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
                logs.push_back(number);
        }
    }

    // 如果当前目录中的文件与 AddLiveFiles 文件不匹配, 则说明丢了文件
    if (!expected.empty())
    {
        char buf[50];
        std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                    static_cast<int>(expected.size()));
        return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
    }

    // 按照 log file 的生成顺序, 由旧到新地回放 Recover
    // 恢复的方式为读 log file 并写到 Memtable
    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size(); i++)
    {
        s = RecoverLogFile(logs[i],
                           (i == logs.size() - 1),
                           save_manifest,
                           edit,
                           &max_sequence);
        if (!s.ok()) return s;

        // 之前的版本在分配这个 log number 后可能没有写任何 MANIFEST 记录
        // 所以我们手动地更新 VersionSet 中的 file number allocation counter
        versions_->MarkFileNumberUsed(logs[i]);
    }

    // 更新醉的全局 sequence, 因为 log file 对应的 Memtable 还没有生成 SSTable
    // 因此不会写到 MANIFEST
    if (versions_->LastSequence() < max_sequence)
        versions_->SetLastSequence(max_sequence);

    return Status::OK();
}

/**
 * 恢复 log number 对应的 log file, 这里的恢复方式体现为:
 * 将该 log 中的数据不断写入 Memtable File.
 * @param log_number[IN], 待恢复的 log file 的编号
 * @param last_log[IN], 标识当前文件是否为 last log
 *                      若是, 可能会 reuse 该文件作为 db 的 log file / memtable
 * @param save_manifest[OUT], 表示是否写了 Level-0 并更新了 MANIFEST
 * @param edit[IN/OUT], 应该是对 MANIFEST 更新的 VersionEdit
 * @param max_sequence[OUT], 写 memtable 过程中会更新
 */
Status DBImpl::RecoverLogFile(uint64_t log_number,
                              bool last_log,
                              bool* save_manifest,
                              VersionEdit* edit,
                              SequenceNumber* max_sequence)
{
    struct LogReporter : public log::Reader::Reporter
    {
        Env* env;
        Logger* info_log;
        const char* fname;
        Status* status;  // null if options_.paranoid_checks==false
        void Corruption(size_t bytes, const Status& s) override
        {
            Log(info_log, "%s%s: dropping %d bytes; %s",
                (this->status == nullptr ? "(ignoring error) " : ""), fname,
                static_cast<int>(bytes), s.ToString().c_str());
            if (this->status != nullptr && this->status->ok()) *this->status = s;
        }
    };

    mutex_.AssertHeld();

    // Open the log file
    std::string fname = LogFileName(dbname_, log_number);   /* dbname_/log_number.log */
    SequentialFile* file;
    Status status = env_->NewSequentialFile(fname, &file);
    if (!status.ok())
    {
        MaybeIgnoreError(&status);
        return status;
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = options_.info_log;
    reporter.fname = fname.c_str();
    reporter.status = (options_.paranoid_checks ? &status : nullptr);
    // We intentionally make log::Reader do checksumming even if
    // paranoid_checks==false so that corruptions cause entire commits
    // to be skipped instead of propagating bad information (like overly
    // large sequence numbers).
    // 这里有意让 log::Reader 进行校验, 即使 paranoid_checks==false
    // 如果不进行校验, 则 corruptions 会导致整个 commits 被跳过,
    // 而不是传播错误信息(如过大的 sequence numbers)
    log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
    Log(options_.info_log, "Recovering log #%llu",
        (unsigned long long)log_number);

    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    int compactions = 0;
    MemTable* mem = nullptr;
    while (reader.ReadRecord(&record, &scratch) && status.ok())
    {
        if (record.size() < 12)
        {
            reporter.Corruption(record.size(), Status::Corruption("log record too small"));
            continue;
        }

        WriteBatchInternal::SetContents(&batch, record);

        if (mem == nullptr)
        {
            mem = new MemTable(internal_comparator_);
            mem->Ref();
        }

        status = WriteBatchInternal::InsertInto(&batch, mem);
        MaybeIgnoreError(&status);
        if (!status.ok())
        {
            break;
        }

        const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                        WriteBatchInternal::Count(&batch) - 1;
        if (last_seq > *max_sequence)
        {
            *max_sequence = last_seq;
        }

        if (mem->ApproximateMemoryUsage() > options_.write_buffer_size)
        {
            compactions++;
            *save_manifest = true;
            status = WriteLevel0Table(mem, edit, nullptr);
            mem->Unref();
            mem = nullptr;
            if (!status.ok())
            {
                // Reflect errors immediately so that conditions like full
                // file-systems cause the DB::Open() to fail.
                break;
            }
        }
    }

    delete file;

    // See if we should keep reusing the last log file.
    if (status.ok()
        && options_.reuse_logs
        && last_log
        && compactions == 0)
    {
        assert(logfile_ == nullptr);
        assert(log_ == nullptr);
        assert(mem_ == nullptr);

        uint64_t lfile_size;
        if (env_->GetFileSize(fname, &lfile_size).ok()
            && env_->NewAppendableFile(fname, &logfile_).ok())
        {
            Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
            log_ = new log::Writer(logfile_, lfile_size);
            logfile_number_ = log_number;
            if (mem != nullptr)
            {
                mem_ = mem;
                mem = nullptr;
            }
            else
            {
                // mem can be nullptr if lognum exists but was empty.
                mem_ = new MemTable(internal_comparator_);
                mem_->Ref();
            }
        }
    }

    if (mem != nullptr)
    {
        // mem did not get reused; compact it.
        if (status.ok())
        {
            *save_manifest = true;
            status = WriteLevel0Table(mem, edit, nullptr);
        }
        mem->Unref();
    }

    return status;
}

/**
 * 将 memtable 写到一个 table file(<dbname>/<number>.ldb)
 * 若生成了一个有效的 table file, 会将该文件的 FileMetaData 添加到 VersionEdit
 * @param mem[IN], 
 * @param edit[OUT], 若生成了 table file, 则会向 *edit 中 AddFile
 * @param base[IN], current version
 */
Status
DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
{
  	mutex_.AssertHeld();

	const uint64_t start_micros = env_->NowMicros();
	FileMetaData meta;
	meta.number = versions_->NewFileNumber();	// prepare filenum for new table file
  	pending_outputs_.insert(meta.number);
	Iterator* iter = mem->NewIterator();
	Log(options_.info_log, "Level-0 table #%llu: started",
						   (unsigned long long)meta.number);

	// 尝试生成 table file, 该操作被 mutex_ 保护
	Status s;
	{
		mutex_.Unlock();
		s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
		mutex_.Lock();
	}
	Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
							(unsigned long long)meta.number,
							(unsigned long long)meta.file_size,
							s.ToString().c_str());
  	delete iter;
  	pending_outputs_.erase(meta.number);

	// 如果 file_size 为 0, 则将不会生成 table file(file 已经被删除？)
	// 因此不应该将该 file 添加到 MANIFEST
  	int level = 0;
  	if (s.ok() && meta.file_size > 0)	// 生成了 table file
	{
		const Slice min_user_key = meta.smallest.user_key();
		const Slice max_user_key = meta.largest.user_key();
		if (base != nullptr)
		{
            // level 表示生成的 table file 应该落到哪一层
            // 若不存在 overlap 则默认为在 level-0
			level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
		}
		edit->AddFile(level, meta.number, meta.file_size, meta.smallest, meta.largest);
	}

	// 关于本次 compaction 的统计信息
	CompactionStats stats;
	stats.micros = env_->NowMicros() - start_micros;
	stats.bytes_written = meta.file_size;
	stats_[level].Add(stats);
	return s;
}

// Minor Compaction, 将 Memtable dump 为 SST
void DBImpl::CompactMemTable()
{
	mutex_.AssertHeld();
	assert(imm_ != nullptr);

	// Save the contents of the memtable as a new Table
	VersionEdit edit;
	Version* base = versions_->current();
	base->Ref();
	Status s = WriteLevel0Table(imm_, &edit, base);
	base->Unref();

    if (s.ok() && shutting_down_.load(std::memory_order_acquire))
    {
        s = Status::IOError("Deleting DB during memtable compaction");
    }

    // Replace immutable memtable with the generated Table
    // 写到 MANIFEST, 表示 minor compaction 完成?
    if (s.ok())
    {
        edit.SetPrevLogNumber(0);
        edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
        s = versions_->LogAndApply(&edit, &mutex_);
    }

    // immutable memtable 的清理
    if (s.ok())
    {
        // Commit to the new state
        imm_->Unref();
        imm_ = nullptr;
        has_imm_.store(false, std::memory_order_release);
        RemoveObsoleteFiles();
    }
    else
    {
        RecordBackgroundError(s);
    }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end)
{
    int max_level_with_files = 1;
    {
        // 在 current version 中寻找与传入 range 有重叠的最大 level
        MutexLock l(&mutex_);
        Version* base = versions_->current();
        for (int level = 1; level < config::kNumLevels; level++)
        {
            if (base->OverlapInLevel(level, begin, end))
            {
                max_level_with_files = level;
            }
        }
    }

    // 先强制将 memtable 进行 minor compaction, 然后逐层尝试 major compaction
    TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
    for (int level = 0; level < max_level_with_files; level++)
    {
        TEST_CompactRange(level, begin, end);
    }
}

/**
 * @brief Compact any files in the named level that overlap [*begin,*end]
 * 
 * @param level 
 * @param begin 
 * @param end 
 */
void DBImpl::TEST_CompactRange(int level, const Slice* begin, const Slice* end)
{
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);

    InternalKey begin_storage, end_storage;

    ManualCompaction manual;
    manual.level = level;
    manual.done = false;
    
    if (begin == nullptr)
    {
        manual.begin = nullptr;
    }
    else
    {
        begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
        manual.begin = &begin_storage;
    }
    
    if (end == nullptr)
    {
        manual.end = nullptr;
    }
    else
    {
        end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
        manual.end = &end_storage;
    }

    MutexLock l(&mutex_);
    while (!manual.done
           && !shutting_down_.load(std::memory_order_acquire)
           && bg_error_.ok())
    {
        if (manual_compaction_ == nullptr)
        {
            // 当前 manual compaction 处于 Idle 状态
            manual_compaction_ = &manual;
            MaybeScheduleCompaction();
        }
        else
        {
            // 当前 manual compaction 正在进行
            background_work_finished_signal_.Wait();
        }
    }

    if (manual_compaction_ == &manual) {
        // Cancel my manual compaction since we aborted early for some reason.
        manual_compaction_ = nullptr;
    }
}

/**
 * @brief 强制触发当前的 memtable contents 进行 minor compaction
 */
Status DBImpl::TEST_CompactMemTable()
{
    // WriteBatch 置 nullptr 用于等待之前的 writes 完成
    Status s = Write(WriteOptions(), nullptr);
    if (s.ok())
    {
        // Wait until the compaction completes
        MutexLock l(&mutex_);
        while (imm_ != nullptr && bg_error_.ok())
        {
            background_work_finished_signal_.Wait();
        }

        if (imm_ != nullptr) s = bg_error_;
    }
    return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

/**
 * 以下三种场景下满足其一则会 schedule 一个后台的 compaction:
 * - imm != nullptr, 即需要将 Memtable dump 成 SSTable
 * - manual_compaction_ != nullptr, 即通过 DBImpl::CompactRange 人工触发了一次 compaction
 * - versions_->NeedsCompaction() 返回 true, 即 current Version 的文件数量/大小/seek 次数的状态
 */
void DBImpl::MaybeScheduleCompaction()
{
	mutex_.AssertHeld();
  
  	if (background_compaction_scheduled_)
	{
    	// Already scheduled
  	}
	else if (shutting_down_.load(std::memory_order_acquire))
	{
    	// DB is being deleted; no more background compactions
  	}
	else if (!bg_error_.ok())
	{
    	// Already got an error; no more changes
	}
	else if (imm_ == nullptr	// 没有 Immutable MemTable 需要 dump 成 SST
			 && manual_compaction_ == nullptr	// 非通过 DBImpl::CompactRange 人工触发
             && !versions_->NeedsCompaction())	// 判断不需要进一步发起 Compaction
	{
    	// No work to be done
  	}
	else
	{
		background_compaction_scheduled_ = true;
		env_->Schedule(&DBImpl::BGWork, this);
	}
}

void DBImpl::BGWork(void* db) {
    reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall()
{
    MutexLock l(&mutex_);
    assert(background_compaction_scheduled_);

    if (shutting_down_.load(std::memory_order_acqsuire))
    {
        // No more background work when shutting down.
    }
    else if (!bg_error_.ok())
    {
        // No more background work after a background error.
    }
    else
    {
        BackgroundCompaction();
    }

    background_compaction_scheduled_ = false;

    // Previous compaction may have produced too many files in a level,
    // so reschedule another compaction if needed.
    MaybeScheduleCompaction();
    background_work_finished_signal_.SignalAll();
}

/**
 * @brief 用户实际发起一次 Compaction
 * Compaction 存在有以下的优先级:
 * 1. Minor Compaction -- imm_ != nullptr
 * 2. Manual Compaction -- manual_compaction_ != nullptr
 * 3. 基于 Size 的 Major Compaction --┐
 * 4. 基于 Seek 的 Major Compaction --+-- PickCompaction != nullptr
 * 
 * 
 */
void DBImpl::BackgroundCompaction()
{
    mutex_.AssertHeld();

    // 存在有 immutable memtable 时会进行 minor compaction
    if (imm_ != nullptr)
    {
        CompactMemTable();
        return;
    }

    Compaction* c;
    bool is_manual = (manual_compaction_ != nullptr);
    InternalKey manual_end;
    if (is_manual)
    {
        ManualCompaction* m = manual_compaction_;
        c = versions_->CompactRange(m->level, m->begin, m->end);
        m->done = (c == nullptr);
        if (c != nullptr)
        {
            manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
        }
        Log(options_.info_log,
            "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
            m->level,
            (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
            (m->end ? m->end->DebugString().c_str() : "(end)"),
            (m->done ? "(end)" : manual_end.DebugString().c_str()));
    }
    else
    {
        c = versions_->PickCompaction();
    }

    Status status;
    if (c == nullptr)
    {
        // Nothing to do
    }
    else if (!is_manual && c->IsTrivialMove())
    {
        assert(c->num_input_files(0) == 1);

        // Move file to next level
        FileMetaData* f = c->input(0, 0);
        c->edit()->RemoveFile(c->level(), f->number);
        c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                           f->smallest, f->largest);
        status = versions_->LogAndApply(c->edit(), &mutex_);
        if (!status.ok())
        {
            RecordBackgroundError(status);
        }
        VersionSet::LevelSummaryStorage tmp;
        Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
            static_cast<unsigned long long>(f->number), c->level() + 1,
            static_cast<unsigned long long>(f->file_size),
            status.ToString().c_str(), versions_->LevelSummary(&tmp));
    }
    else
    {
        // do major compaction
        CompactionState* compact = new CompactionState(c);
        status = DoCompactionWork(compact);
        if (!status.ok())
        {
            RecordBackgroundError(status);
        }
        CleanupCompaction(compact);
        c->ReleaseInputs();
        RemoveObsoleteFiles();
    }
    delete c;

    if (status.ok())
    {
        // Done
    }
    else if (shutting_down_.load(std::memory_order_acquire))
    {
        // Ignore compaction errors found during shutting down
    }
    else
    {
        Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
    }

    if (is_manual)
    {
        ManualCompaction* m = manual_compaction_;
        if (!status.ok())
        {
            m->done = true; // done 为 true 表示 manual compaction 出现 error? (TODO)
        }

        if (!m->done)
        {
            // 只 compact 了请求 range 的一个部分
            // 更新 *m 为之后要做 compact 的 range
            m->tmp_storage = manual_end;
            m->begin = &m->tmp_storage;
        }
        manual_compaction_ = nullptr;
    }
}

void DBImpl::CleanupCompaction(CompactionState* compact)
{
    mutex_.AssertHeld();

    if (compact->builder != nullptr)
    {
        // May happen if we get a shutdown call in the middle of compaction
        compact->builder->Abandon();
        delete compact->builder;
    }
    else
    {
        assert(compact->outfile == nullptr);
    }
    delete compact->outfile;
    for (size_t i = 0; i < compact->outputs.size(); i++)
    {
        const CompactionState::Output& out = compact->outputs[i];
        pending_outputs_.erase(out.number);
    }
    delete compact;
}

/**
 * @brief 创建新的 output SST file, 并设置 compact->builder
 */
Status DBImpl::OpenCompactionOutputFile(CompactionState* compact)
{
    assert(compact != nullptr);
    assert(compact->builder == nullptr);

    uint64_t file_number;
    {
        mutex_.Lock();
        file_number = versions_->NewFileNumber();
        pending_outputs_.insert(file_number);
        CompactionState::Output out;
        out.number = file_number;
        out.smallest.Clear();
        out.largest.Clear();
        compact->outputs.push_back(out);
        mutex_.Unlock();
    }

    // Make the output file
    std::string fname = TableFileName(dbname_, file_number);
    Status s = env_->NewWritableFile(fname, &compact->outfile);
    if (s.ok())
    {
        compact->builder = new TableBuilder(options_, compact->outfile);
    }
    return s;
}

/**
 * @brief TODO
 * 
 * (因为LevelDB限制了每个SSTable的大小，因此在Major Compaction期间，
 *  如果当前写入的SSTable过大，会将其拆分成多个SSTable写入，
 * 所以这里关闭的是最后一个SSTable。
 * 该方法主要通过SSTable的Builder的Finish方法完成对SSTable的写入，
 * 这里不再赘述，感兴趣的读者可以自行阅读。)
 * 
 * @param compact 
 * @param input 
 * @return Status 
 */
Status
DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                   Iterator* input)
{
    assert(compact != nullptr);
    assert(compact->outfile != nullptr);
    assert(compact->builder != nullptr);

    const uint64_t output_number = compact->current_output()->number;
    assert(output_number != 0);

    // Check for iterator errors
    Status s = input->status();
    const uint64_t current_entries = compact->builder->NumEntries();
    if (s.ok())
    {
        s = compact->builder->Finish();
    }
    else
    {
        compact->builder->Abandon();
    }
    const uint64_t current_bytes = compact->builder->FileSize();
    compact->current_output()->file_size = current_bytes;
    compact->total_bytes += current_bytes;
    delete compact->builder;
    compact->builder = nullptr;

    // Finish and check for file errors
    if (s.ok())
    {
        s = compact->outfile->Sync();
    }
    if (s.ok())
    {
        s = compact->outfile->Close();
    }
    delete compact->outfile;
    compact->outfile = nullptr;

    if (s.ok() && current_entries > 0)
    {
        // Verify that the table is usable
        Iterator* iter =
            table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
        s = iter->status();
        delete iter;
        if (s.ok())
        {
            Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
                (unsigned long long)output_number, compact->compaction->level(),
                (unsigned long long)current_entries,
                (unsigned long long)current_bytes);
        }
    }
    return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

/**
 * @brief Major Compaction 的实现
 * 
 * @param compact 
 * @return Status 
 */
Status
DBImpl::DoCompactionWork(CompactionState* compact)
{
    const uint64_t start_micros = env_->NowMicros();
    int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

    Log(options_.info_log, "Compacting %d@%d + %d@%d files",
        compact->compaction->num_input_files(0),
        compact->compaction->level(),
        compact->compaction->num_input_files(1),
        compact->compaction->level() + 1);

    assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
    assert(compact->builder == nullptr);
    assert(compact->outfile == nullptr);
    // 保留最旧的 snapshot number (TODO: for what?)
    if (snapshots_.empty())
    {
        compact->smallest_snapshot = versions_->LastSequence();
    }
    else
    {
        compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
    }

    Iterator* input = versions_->MakeInputIterator(compact->compaction);

    // 实际进行 Compaction 过程中释放锁
    mutex_.Unlock();

    input->SeekToFirst();
    Status status;
    ParsedInternalKey ikey;
    std::string current_user_key;
    // 当前 key 的 userkey 之前是否出现过
    bool has_current_user_key = false;
    // 上次出现时的 sequence number
    // 若之前没出现过则为 kMaxSequenceNumber
    SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
    while (input->Valid()
           && !shutting_down_.load(std::memory_order_acquire))
    {
        // 先看看是否有 immutable memtable 等待 minor compaction
        // compaction 线程优先 minor compaction
        if (has_imm_.load(std::memory_order_relaxed))
        {
            const uint64_t imm_start = env_->NowMicros();
            mutex_.Lock();
            if (imm_ != nullptr)
            {
                CompactMemTable();
                // Wake up MakeRoomForWrite() if necessary.
                background_work_finished_signal_.SignalAll();
            }
            mutex_.Unlock();
            imm_micros += (env_->NowMicros() - imm_start);
        }

        Slice key = input->key();

        // 如果 output 加上这个 key 之后与 granparent_ 的 overlap 太大,
        // 主动关闭当前的 SST file
        if (compact->compaction->ShouldStopBefore(key)
            && compact->builder != nullptr)
        {
            status = FinishCompactionOutputFile(compact, input);
            if (!status.ok())
            {
                break;
            }
        }

        // Handle key/value, add to state, etc.
        bool drop = false;
        if (!ParseInternalKey(key, &ikey))
        {
            // 解析该 key 出错, 这里不希望隐藏该 error key
            current_user_key.clear();
            has_current_user_key = false;
            last_sequence_for_key = kMaxSequenceNumber;
        }
        else
        {
            if (!has_current_user_key
                || user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) != 0)
            {
                // 一个 user key 第一次出现
                current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                has_current_user_key = true;
                last_sequence_for_key = kMaxSequenceNumber;
                // Note. 即便 SequenceNumber 可能在 smallest_snapshot 前
                // 但如果一个 UserKey 是第一次出现, 也不能将其丢弃
            }

            if (last_sequence_for_key <= compact->smallest_snapshot)
            {
                // 如果一个 key 不是第一次出现, 且其 sequence 在最小的快照之前
                // 表明该 entry 已经被一个更新的 userkey 覆盖
                drop = true;  // (A)
            }
            else if (ikey.type == kTypeDeletion
                     && ikey.sequence <= compact->smallest_snapshot
                     && compact->compaction->IsBaseLevelForKey(ikey.user_key))
            {
                // 对于一个可能被 compact 到 level_+1 的 user key:
                // (1) 其类型是 Deletion
                // (2) 在 level_+2 即更高层中没有同样的 user key
                // (3) 在本次 compact 的层中的数据, 以及有着更小的 sequence number
                //     的数据会在之后的迭代中被丢弃 (上述的 rule (A) 保证)
                // 综上, 这个 deletion marker (即该 key/value entry) 可以直接丢弃
                drop = true;
            }

            last_sequence_for_key = ikey.sequence;
        }
#if 0
        Log(options_.info_log,
            "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
            "%d smallest_snapshot: %d",
            ikey.user_key.ToString().c_str(),
            (int)ikey.sequence, ikey.type, kTypeValue, drop,
            compact->compaction->IsBaseLevelForKey(ikey.user_key),
            (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif
        
        // 将需要保留的 key/value 加入到当前的 output SST file 中
        if (!drop)
        {
            // 如果 output 与 level_+2 的 overlap 过大会在上面被关闭
            // 所以如果需要这里重新开一个 output file
            if (compact->builder == nullptr)
            {
                status = OpenCompactionOutputFile(compact);
                if (!status.ok()) break;
            }

            if (compact->builder->NumEntries() == 0)
            {
                // 新的 output SST file builder, 置当前 key 为 smallest
                compact->current_output()->smallest.DecodeFrom(key);
            }

            compact->current_output()->largest.DecodeFrom(key);
            compact->builder->Add(key, input->value());

            // 当 output file 的大小超过该层 SST file 阈值时主动关闭
            if (compact->builder->FileSize() >= compact->compaction->MaxOutputFileSize())
            {
                status = FinishCompactionOutputFile(compact, input);
                if (!status.ok()) break;
            }
        }

        input->Next();  // 同 UserKey 是按照 Seq 降序遍历的
    }

    if (status.ok() && shutting_down_.load(std::memory_order_acquire))
    {
        status = Status::IOError("Deleting DB during compaction");
    }
    if (status.ok() && compact->builder != nullptr)
    {
        status = FinishCompactionOutputFile(compact, input);
    }
    if (status.ok())
    {
        status = input->status();
    }
    delete input;
    input = nullptr;

    CompactionStats stats;
    stats.micros = env_->NowMicros() - start_micros - imm_micros;
    for (int which = 0; which < 2; which++)
    {
        for (int i = 0; i < compact->compaction->num_input_files(which); i++)
        {
            stats.bytes_read += compact->compaction->input(which, i)->file_size;
        }
    }
    for (size_t i = 0; i < compact->outputs.size(); i++)
    {
        stats.bytes_written += compact->outputs[i].file_size;
    }

    mutex_.Lock();
    stats_[compact->compaction->level() + 1].Add(stats);

    if (status.ok())
    {
        status = InstallCompactionResults(compact);
    }
    if (!status.ok())
    {
        RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
    return status;
}

namespace {

struct IterState {
    port::Mutex* const mu;
    Version* const version GUARDED_BY(mu);
    MemTable* const mem GUARDED_BY(mu);
    MemTable* const imm GUARDED_BY(mu);

    IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
        : mu(mutex)
        , version(version)
        , mem(mem)
        , imm(imm)
    {}
};

static void
CleanupIteratorState(void* arg1, void* arg2)
{
    IterState* state = reinterpret_cast<IterState*>(arg1);
    state->mu->Lock();

    state->mem->Unref();
    if (state->imm != nullptr) state->imm->Unref();
    state->version->Unref();

    state->mu->Unlock();
    delete state;
}

}  // anonymous namespace

/**
 * 返回一个 merged iterator, 该 iter 由以下的 iter 构成:
 * - mem
 * - imm
 * - current version 中的所有 sst files
 * @param options[IN]
 * @param latest_snapshot[OUT] 最新的 snapshot 的 number?
 * @param seed[OUT] TODO
 */
Iterator*
DBImpl::NewInternalIterator(const ReadOptions& options,
                            SequenceNumber* latest_snapshot,
                            uint32_t* seed)
{
    mutex_.Lock();

    *latest_snapshot = versions_->LastSequence();

    // Collect together all needed child iterators
    std::vector<Iterator*> list;
    // memtable 的 iterator
    list.push_back(mem_->NewIterator());
    mem_->Ref();
    // immutable memtable 的 iterator
    if (imm_ != nullptr) {
        list.push_back(imm_->NewIterator());
        imm_->Ref();
    }
    // current version 中各 level 的 file 上的 iterator
    versions_->current()->AddIterators(options, &list);
    // 将上面拿到的 mem, imm, sst iterators 聚合得到一个 merged iter
    Iterator* internal_iter =
        NewMergingIterator(&internal_comparator_, &list[0], list.size());
    versions_->current()->Ref();

    IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
    internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

    *seed = ++seed_;
    mutex_.Unlock();
    return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

/**
 * 尝试在 DB 中点查 Key
 * @param options[IN]
 * @param key[IN]
 * @param value[OUT], 如果 key 存在，则会在 value 中存入对应的 value
 *  				  如果 key 不存在，则 *value **不会被修改**
 * @return 当未发发生错误，且 key 存在时返回 OK
 * 		   当未发生错误，且 key 不存在时返回状态 Status::IsNotFound() 为 true
 * 		   其他错误会有别的 Status
 */
Status
DBImpl::Get(const ReadOptions& options, const Slice& key,
            std::string* value)
{
    Status s;
    MutexLock l(&mutex_);
    SequenceNumber snapshot;

    if (options.snapshot != nullptr)
    {
        snapshot = static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
    }
    else
    {
        snapshot = versions_->LastSequence();
    }

    MemTable* mem = mem_; mem->Ref();
    MemTable* imm = imm_; if (imm != nullptr) imm->Ref();
    Version* current = versions_->current(); current->Ref();

    bool have_stat_update = false;
    Version::GetStats stats;

    // Unlock while reading from files and memtables
    {
        mutex_.Unlock();

        LookupKey lkey(key, snapshot);
        if (mem->Get(lkey, value, &s)) // 首先查 Memtable
        {
            // Hit in memtable, Done
        }
        else if (imm != nullptr && imm->Get(lkey, value, &s)) // 其次查 Immutable Memtable (如果有的话)
        {
            // Hit in immutable memtable, Done
        }
        else // 然后查 current Version 中的 log file
        {
            s = current->Get(options, lkey, value, &stats);
            have_stat_update = true;
        }

        mutex_.Lock();
    }

    if (have_stat_update && current->UpdateStats(stats))
    {
        // 若本次查找到了 SSTable file, 且有文件因为 seek 次数被标记为待 compaction
        MaybeScheduleCompaction();
    }

    mem->Unref();
    if (imm != nullptr) imm->Unref();
    current->Unref();

    return s;
}

Iterator*
DBImpl::NewIterator(const ReadOptions& options)
{
    SequenceNumber latest_snapshot;
    uint32_t seed;
    Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
    return NewDBIterator(this,
                         user_comparator(),
                         iter,
                         (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                         seed);
}

void DBImpl::RecordReadSample(Slice key)
{
    MutexLock l(&mutex_);
  
    if (versions_->current()->RecordReadSample(key))
        MaybeScheduleCompaction();
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status
DBImpl::Write(const WriteOptions& options, WriteBatch* updates)
{
    Writer w(&mutex_);
    w.batch = updates;
    w.sync = options.sync;
    w.done = false;

    MutexLock l(&mutex_);
    writers_.push_back(&w);
    while (!w.done && &w != writers_.front())
    {
        w.cv.Wait();
    }

    if (w.done) return w.status;

    // May temporarily unlock and wait.
    Status status = MakeRoomForWrite(updates == nullptr);
    uint64_t last_sequence = versions_->LastSequence();
    Writer* last_writer = &w;
    if (status.ok()
        && updates != nullptr)  // nullptr batch is for compactions
    {
        WriteBatch* write_batch = BuildBatchGroup(&last_writer);
        WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
        last_sequence += WriteBatchInternal::Count(write_batch);

        // append 到 WAL, 并 apply 到 memtable
        // 在这阶段可以释放锁, 因为 &w 目前负责 logging 以及防止有并发的 logger 和并发的写 mem_
        {
            mutex_.Unlock();

            // write WAL with sync
            status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
            bool sync_error = false;
            if (status.ok() && options.sync)
            {
                status = logfile_->Sync();  // TODO, log_ & logfile_? 看起来是一个东西, 但两者都有 sync?
                if (!status.ok()) {
                    sync_error = true;
                }
            }

            // write to memtable
            if (status.ok()) {
                status = WriteBatchInternal::InsertInto(write_batch, mem_);
            }

            mutex_.Lock();
            if (sync_error) {
                // The state of the log file is indeterminate: the log record we
                // just added may or may not show up when the DB is re-opened.
                // So we force the DB into a mode where all future writes fail.
                RecordBackgroundError(status);
            }
        }

        if (write_batch == tmp_batch_) tmp_batch_->Clear();

        versions_->SetLastSequence(last_sequence);
    }

    // 通知之前队列里排队, 且被 piggyback 的 batch
    while (true)
    {
        Writer* ready = writers_.front();
        writers_.pop_front();
        if (ready != &w)
        {
            ready->status = status;
            ready->done = true;
            ready->cv.Signal();
        }
        if (ready == last_writer) break;
    }

    // 如果队列还有在排队的, notify 开始下一次 Write
    if (!writers_.empty()) {
        writers_.front()->cv.Signal();
    }

    return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
/**
 * @brief 从 writers 的对头开始向后尝试聚合一个 write batch
 * 
 * @note 若队首 non-sync, 不会将 sync 的 write 加入 batch
 *       会根据 writer_front 的 batch size 调整 batch 的大小
 * 
 * @param last_writer [IN], 调用时 Writer 的指针
 *                    [OUT], 指向最后一个加入到 write batch 的 Writer
 * @return WriteBatch*, 本次 writers 队列中可以 batch 的 writes
 *                      所聚合成的一个 write batch
 */
WriteBatch*
DBImpl::BuildBatchGroup(Writer** last_writer)
{
    mutex_.AssertHeld();
    assert(!writers_.empty());

    // writer_ 的队首决定了本次 write 的 batch 的一些属性
    Writer* first = writers_.front();
    WriteBatch* result = first->batch;
    assert(result != nullptr);

    size_t size = WriteBatchInternal::ByteSize(first->batch);

    // 允许 BatchGroup 增长到 max size
    size_t max_size = 1 << 20;  // (1M)
    // 但如果是 small write，将限制可增长到的阈值，从而避免过多地减慢 small write
    if (size <= (128 << 10)) // (128K)
        max_size = size + (128 << 10);

    *last_writer = first;
    std::deque<Writer*>::iterator iter = writers_.begin();
    ++iter;  // Advance past "first"
    for (; iter != writers_.end(); ++iter)
    {
        Writer* w = *iter;
        if (w->sync && !first->sync)
        {
            // 若队列中某一个 writebatch 需要 sync, 但队首可以异步
            // 这里不讲这个 sync write 加入到一个由 non-sync write 的 batch 中
            break;
        }

        if (w->batch != nullptr)
        {
            size += WriteBatchInternal::ByteSize(w->batch);
            if (size > max_size)
            {
                // Do not make batch too big
                break;
            }

            if (result == first->batch)
            {
                // 循环第一次的初始化动作
                // 使用 tmp_batch 来存储 writes, 并将 first 放入到 tmp_batch
                result = tmp_batch_;
                assert(WriteBatchInternal::Count(result) == 0);
                WriteBatchInternal::Append(result, first->batch);
            }

            // Append to *result
            WriteBatchInternal::Append(result, w->batch);
        }
        *last_writer = w;
    }
    return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWr]ite(bool force)
{
    mutex_.AssertHeld();
    assert(!writers_.empty());

    bool allow_delay = !force;
    Status s;
    while (true)
    {
        if (!bg_error_.ok())
        {
            // Yield previous error
            s = bg_error_;
            break;
        }
        else if (allow_delay
                 && versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger)
        {
            // 表示当前的 L0 files 已经接近阈值
            // 当实际到达阈值时写请求可能会被 delay 数秒, 所以我们选择在每个写中
            // delay 1ms, 以减少延迟的抖动.
            // 这里的 delay 会将 CPU 让给 compaction thread, 以防他们与 writer
            // 共享同一个 CPU core
            mutex_.Unlock();
            env_->SleepForMicroseconds(1000);
            allow_delay = false;  // 一个写只允许 Delay 一次
            mutex_.Lock();
        }
        else if (!force
                 && (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size))
        {
            // There is room in current memtable
            break;
        }
        else if (imm_ != nullptr)
        {
            // 当前的 memtable 已满, 但前一个 minor compaction 还在进行
            // so we wait.
            Log(options_.info_log, "Current memtable full; waiting...\n");
            background_work_finished_signal_.Wait();
        }
        else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger)
        {
            // 当 L0 文件数达到阈值了(12), 需要等待 compaction 完成
            Log(options_.info_log, "Too many L0 files; waiting...\n");
            background_work_finished_signal_.Wait();
        }
        else
        {
            // 这里表示 memtable 已满, 准备切换为 imm 并进行 minor compaction
            assert(versions_->PrevLogNumber() == 0);
            uint64_t new_log_number = versions_->NewFileNumber();
            WritableFile* lfile = nullptr;
            s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
            if (!s.ok())
            {
                // Avoid chewing through file number space in a tight loop.
                versions_->ReuseFileNumber(new_log_number);
                break;
            }
            delete log_;
            delete logfile_;
            logfile_ = lfile;
            logfile_number_ = new_log_number;
            log_ = new log::Writer(lfile);
            imm_ = mem_;
            has_imm_.store(true, std::memory_order_release);
            mem_ = new MemTable(internal_comparator_);
            mem_->Ref();
            force = false;  // Do not force another compaction if have room
            MaybeScheduleCompaction();
        }
    }
    return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

/**
 * Put 方法的默认实现, 如果 DB 的子类期望可以直接使用该方法
 * 设置数据库中的条目 “key” 对应值为 “value”
 * 若成功返回 OK, 否则返回一个 non-OK
 * 注意: 考虑设置 options.sync = true
 */
Status
DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value)
{
    WriteBatch batch;
    batch.Put(key, value);
    return Write(opt, &batch);
}

// 和 Put 类似的行为
Status DB::Delete(const WriteOptions& opt, const Slice& key) {
    WriteBatch batch;
    batch.Delete(key);
    return Write(opt, &batch);
}

DB::~DB() = default;

/**
 * 用于打开一个以 name 标识的 DB，调用者应该在不需要 DB 时 delete *dbptr
 * @param options[IN], 
 * @param name[IN], 要打开的 DB 的 name
 * @param dbptr[OUT], 存储一个指向 heap-allocated DB 的指针
 * @return 发生错误是会返回一个 non-OK status，并且 dbptr 会是一个 nullptr
 */
Status
DB::Open(const Options& options, const std::string& dbname, DB** dbptr)
{
    *dbptr = nullptr;

    DBImpl* impl = new DBImpl(options, dbname);

    impl->mutex_.Lock();

    // 尝试根据 MANIFEST 和 local 的 log file 恢复 db
    VersionEdit edit;
    bool save_manifest = false; // 1. VersionSet::Recover 中若不能 Reuse MANIFEST
                                // 2. 存在需要恢复的 log file 且发生了 Memtable Dump 到 Level-0
    Status s = impl->Recover(&edit, &save_manifest); // Recover handles create_if_missing, error_if_exists

    // 若上述 Recover 中没有可以 reuse 的 memtable, 搞个新的
    if (s.ok() && impl->mem_ == nullptr)
    {
        // Create new log and a corresponding memtable.
        uint64_t new_log_number = impl->versions_->NewFileNumber();
        WritableFile* lfile;
        s = options.env->NewWritableFile(LogFileName(dbname, new_log_number), &lfile);
        if (s.ok())
        {
            edit.SetLogNumber(new_log_number);
            impl->logfile_ = lfile;
            impl->logfile_number_ = new_log_number;
            impl->log_ = new log::Writer(lfile);
            impl->mem_ = new MemTable(impl->internal_comparator_);
            impl->mem_->Ref();
        }
    }

    // 若之前的 Recover 中产生了新的 MANIFEST edir, 应用以生成新的 CURRENT
    if (s.ok() && save_manifest)
    {
        edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
        edit.SetLogNumber(impl->logfile_number_);
        s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
    }

    if (s.ok())
    {
        impl->RemoveObsoleteFiles();
        impl->MaybeScheduleCompaction();
    }

    impl->mutex_.Unlock();

    if (s.ok())
    {
        assert(impl->mem_ != nullptr);
        *dbptr = impl;
    }
    else
    {
        delete impl;
    }
    return s;
}

Snapshot::~Snapshot() = default;

/**
 * 真·删库跑路
 */
Status
DestroyDB(const std::string& dbname, const Options& options)
{
    Env* env = options.env;
    // 获取当前 db 下的文件
    std::vector<std::string> filenames;
    Status result = env->GetChildren(dbname, &filenames);
    if (!result.ok())
    {
        // Ignore error in case directory does not exist
        return Status::OK();
    }

    // 获取该 db 对应的文件锁
    FileLock* lock;
    const std::string lockname = LockFileName(dbname);
    result = env->LockFile(lockname, &lock);
  
    if (result.ok())
    {
        uint64_t number;
        FileType type;
        for (size_t i = 0; i < filenames.size(); i++)
        {
            if (ParseFileName(filenames[i], &number, &type)
                && type != kDBLockFile) // Lock file will be deleted at end
            {
                Status del = env->RemoveFile(dbname + "/" + filenames[i]);
                if (result.ok() && !del.ok())
                {
                    // 仅记录第一个错误
                    result = del;
                }
            }
        }

        env->UnlockFile(lock);  // Ignore error since state is already gone
        env->RemoveFile(lockname);
        env->RemoveDir(dbname);  // Ignore error in case dir contains other files
    }

    return result;
}

}  // namespace leveldb
