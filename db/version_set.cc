// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <cstdio>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

// level-0 基于文件数
// 其他 level 会根据 total file size
// level-1 - 10M 之后每层 *10
static double
MaxBytesForLevel(const Options* options, int level) {
    // Note: the result for level zero is not really used since we set
    // the level-0 compaction threshold based on number of files.

    // Result for both level-0 and level-1
    double result = 10. * 1048576.0;
    while (level > 1)
    {
        result *= 10;
        level--;
    }
    return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version()
{
    assert(refs_ == 0);

    // Remove from linked list
    prev_->next_ = next_;
    next_->prev_ = prev_;

    // Drop references to files
    for (int level = 0; level < config::kNumLevels; level++)
    {
        for (size_t i = 0; i < files_[level].size(); i++)
        {
            FileMetaData* f = files_[level][i];
            assert(f->refs > 0);

            f->refs--;
            if (f->refs <= 0) delete f;
        }
    }
}

/**
 * 返回最小的 index `i`, i 应满足 files[i]->largest >= key
 * @REQUIRES: files 应该是一个相互不 overlapping 的 files 的有序列表
 * @return 如果没有这样的 file 则返回 files.size(); 否则返回对应的 index
 */
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key)
{
	uint32_t left = 0;
	uint32_t right = files.size();
	while (left < right)
	{
		uint32_t mid = (left + right) / 2;
		const FileMetaData* f = files[mid];
		if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0)
		{
			// mid.largest < key, 即在 mid 之前的 files 都是 uninteresting
			left = mid + 1;
		}
		else
		{
			// mid.largest >= key, 即在 mid 之后的 files 是 uninteresting
			right = mid;
		}
	}
	return right;
}

// 若 user_key 存在, 且在 f 的 largest key 之后则返回 true; 否则返回 false
static bool
AfterFile(const Comparator* ucmp, const Slice* user_key, const FileMetaData* f)
{
    // null user_key occurs before all keys and is therefore never after *f
    return (user_key != nullptr &&
            ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

// 若 user_key 存在, 且在 f 的 smallest key 之前则返回 true; 否则返回 false
static bool
BeforeFile(const Comparator* ucmp, const Slice* user_key, const FileMetaData* f)
{
    // null user_key occurs after all keys and is therefore never before *f
    return (user_key != nullptr &&
            ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

/**
 * 判断 files[] 中是否存在某个文件, 其 user key range 与 [*smallest,*largest] 有重叠
 * @param icmp[IN]
 * @param disjoint_sorted_files[IN], 标识 files 中文件是否是有序且相互不重叠的
 * @param files[IN]
 * @param smallest_user_key[IN], user key range 的下界, nullptr 表示无穷小
 * @param largest_user_key[IN], user key range 的上界, nullptr 表示无穷大
 * @return 若找到存在 overlap 文件则返回 true
 */
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key)
{
    const Comparator* ucmp = icmp.user_comparator();

    // files 非有序且互不重叠, 因此逐个遍历
    if (!disjoint_sorted_files)
    {
        // Need to check against all files
        for (size_t i = 0; i < files.size(); i++)
        {
            const FileMetaData* f = files[i];
            if (AfterFile(ucmp, smallest_user_key, f)
                || BeforeFile(ucmp, largest_user_key, f))
            {
                // No overlap
            }
            else
            {
                return true;  // Overlap
            }
        }
        return false;
    }

    /*-------------------------------------------------------------------------*/
    // files 有序且互不重叠, 二分搜索 file list
    uint32_t index = 0;
    if (smallest_user_key != nullptr)
    {
        // Find the earliest possible internal key for smallest_user_key
        // TODO: kMaxSequenceNumber 是否有什么深意
        InternalKey small_key(*smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
        // 搜索第一个 largest key >= smallest user key 的 file 的 index
        index = FindFile(icmp, files, small_key.Encode());
    }

    if (index >= files.size()) return false;

    // 找到第一个 largest >= smallest user key 的 file
    // 再判断该 file 的 smallest 是否在 largest user key 之后, 即是否 user key range 完全在 file 之前
    return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.
// 对于一个给定的 version/level pair, 生成关于该 level 中的 files 的信息
// 对于一个给定的 entry
// - key() 是该文件中出现的 largest key
// - value() 是一个 16-byte 的值，包含 2 个 EncodeFixed64: | file number(8) | file size(8) |
class Version::LevelFileNumIterator : public Iterator
{
public:
	LevelFileNumIterator(const InternalKeyComparator& icmp,
						 const std::vector<FileMetaData*>* flist)
		: icmp_(icmp)
		, flist_(flist)	// 该 Version 一个 level 的所有 sstable files
		, index_(flist->size()) /* Marks as invalid */
		{}

  	bool Valid() const override { return index_ < flist_->size(); }

	void Seek(const Slice& target) override
	{
		index_ = FindFile(icmp_, *flist_, target);
	}

	void SeekToFirst() override { index_ = 0; }
	void SeekToLast() override { index_ = flist_->empty() ? 0 : flist_->size() - 1; }
	
	void Next() override
	{
		assert(Valid());
		index_++;
	}

	void Prev() override
	{
		assert(Valid());
		if (index_ == 0)
			index_ = flist_->size();	/* Marks as invalid */
		else
			index_--;
	}

	// index 当前所指的 sstable 的 largest key
  	Slice key() const override
	  {
		assert(Valid());
		return (*flist_)[index_]->largest.Encode();
	}

	// 返回对应 sstable 的 number 和 file size 编码成 Slice
	Slice value() const override
	{
		assert(Valid());
		EncodeFixed64(value_buf_, (*flist_)[index_]->number);
		EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
		return Slice(value_buf_, sizeof(value_buf_));
	}
  
  	Status status() const override { return Status::OK(); }

private:
	const InternalKeyComparator icmp_;	// InternalKey Comparator
	const std::vector<FileMetaData*>* const flist_;	// 该 Version 一个 level 的所有 sstable files
	uint32_t index_;

	// Backing store for value().  Holds the file number and size.
	mutable char value_buf_[16];
};

static Iterator*
GetFileIterator(void* arg, const ReadOptions& options, const Slice& file_value)
{
    TableCache* cache = reinterpret_cast<TableCache*>(arg);
    if (file_value.size() != 16)
        return NewErrorIterator(Status::Corruption("FileReader invoked with unexpected value"));
    else
    {
        return cache->NewIterator(options,
                   /*file_number*/DecodeFixed64(file_value.data()),
                     /*file_size*/DecodeFixed64(file_value.data() + 8));
    }
}

// 返回一个 TwoLevelIterator
// 先找到该 level 中第一个包含某个 key 的 sstable file 的 filenum 和 filesize
// 再获取 filenum 对应 table 的 table cache 的 iterator
Iterator*
Version::NewConcatenatingIterator(const ReadOptions& options, int level) const
{
    return NewTwoLevelIterator(
        /*index_iter*/new LevelFileNumIterator(vset_->icmp_, &files_[level]),
        /*block_function*/&GetFileIterator, /*arg*/vset_->table_cache_,
        /*ReadOptions*/options);
}

void
Version::AddIterators(const ReadOptions& options,
                      std::vector<Iterator*>* iters)
{
    // Merge all level zero files together since they may overlap
    // 对于 level-0 的文件, 建立 Table Cache 的 iterator
    // (TODO: 据说这会直接载入 sstable 文件到内存 cache)
    for (size_t i = 0; i < files_[0].size(); i++)
    {
        iters->push_back(
            vset_->table_cache_->NewIterator(
                options, files_[0][i]->number, files_[0][i]->file_size));
    }

    // For levels > 0, we can use a concatenating iterator that sequentially
    // walks through the non-overlapping files in the level, opening them
    // lazily.
    // 对于 level > 0 的部分, 建立每个 level 的 concatenating iterator(TwoLevelIterator)
    // 这样的 iterator 可以 sequentially 的遍历该 level 中的 non-overlapping files
    for (int level = 1; level < config::kNumLevels; level++) {
        if (!files_[level].empty()) // 这采用的是一种 lazily opening 的方式
            iters->push_back(NewConcatenatingIterator(options, level));
    }
}

// Callback from TableCache::Get()
namespace {
enum SaverState
{
	kNotFound,
	kFound,
	kDeleted,
	kCorrupt,
};

struct Saver
{
	SaverState state;
	const Comparator* ucmp;	// UserKey Comparator
	Slice user_key;
	std::string* value;
};
}  // namespace

/**
 * 解析 InternalKey, 比较 ikey 的 UserKey 部分与传入 Saver 的 UserKey 是否一致
 * 若一致则会在 Saver state 中记录该 InternalKey 的 State(是否有 delete tombstone)
 * 若该 InternalKey 还未被 delete 会将 Slice 的 data 存到 Save->value
 */
static void
SaveValue(void* arg, const Slice& ikey, const Slice& v)
{
	Saver* s = reinterpret_cast<Saver*>(arg);

	ParsedInternalKey parsed_key;
	if (!ParseInternalKey(ikey, &parsed_key))
		s->state = kCorrupt;
	else
	{
		if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0)
		{
			s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
			if (s->state == kFound)
				s->value->assign(v.data(), v.size());
		}
	}
}

// level-0 文件的进行排序的比较函数, 根据 number 降序排列
// TODO: FileMetaData 的 number 是啥意义
static bool
NewestFirst(FileMetaData* a, FileMetaData* b) {
  	return a->number > b->number;
}

// 对每个与 user_key 存在 overlap 的文件, 按从新到旧的顺序调用 func(arg, level, f)
// 如果某次调用 func() 返回 false，则不再继续调用
//
// REQUIRES: user portion of internal_key == user_key.
void Version::ForEachOverlapping(Slice user_key, Slice internal_key,
                                 void* arg, bool (*func)(void*, int, FileMetaData*))
{
  	const Comparator* ucmp = vset_->icmp_.user_comparator();

	// 先尝试搜索 level-0 的文件
	std::vector<FileMetaData*> tmp;
	tmp.reserve(files_[0].size());
	// 按照从新到旧的顺序搜索, 将所有与 user_key 存在 overlap 的文件计入 tmp
	for (uint32_t i = 0; i < files_[0].size(); i++)
	{
		FileMetaData* f = files_[0][i];
		if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0
			&& ucmp->Compare(user_key, f->largest.user_key()) <= 0)
		{
			tmp.push_back(f);
		}
	}

	// 对 level-0 中找到的 overlapping file, 按照 number 降序调用 *func
	if (!tmp.empty())
	{
		std::sort(tmp.begin(), tmp.end(), NewestFirst);
		for (uint32_t i = 0; i < tmp.size(); i++)
		{
			if (!(*func)(arg, 0, tmp[i]))
			{
				return;
			}
		}
	}

	// 然后按序逐层搜索 >0 的 levels
	for (int level = 1; level < config::kNumLevels; level++)
	{
		size_t num_files = files_[level].size();
		if (num_files == 0) continue;


		// 因为 >0 的 level 各文件之间不存在 overlap
		// 因此只需要二分查找第一个 largest key >= internal_key 的 file 的 index
		uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
		if (index < num_files)	// 在该 level 中找到了第一个存在 >= internal_key 的 file
		{
			FileMetaData* f = files_[level][index];
			if (ucmp->Compare(user_key, f->smallest.user_key()) < 0)
			{
				// user_key < f 的 smallest user_key, 即该层中不存在 overlapping file
			}
			else
			{
				if (!(*func)(arg, level, f))
				{
					return;
				}
			}
		}
	}
}

/**
 * 根据 key 查找当前 Version 下对应的 value, 查找自 level-0 向下, 从新到旧的顺序遍历
 * REQUIRES: lock is not held
 * @param options[IN]
 * @param k[IN], key to lookup
 * @param value[OUT], k 对应的 value 会存入到 *val
 * @param stats[OUT], 当本次 Get 中 Seek 了多于 1 个 file 时, 记录 Seek 的第一个 file 的信息
 * @return 找到则返回 OK, 否则返回 non-OK
 */
Status Version::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* stats)
{
	stats->seek_file = nullptr;
	stats->seek_file_level = -1;

	struct State
	{
		Saver saver;
		GetStats* stats;
		const ReadOptions* options;
		Slice ikey;
        // 上一次 Match() 的 file 的信息
		FileMetaData* last_file_read;
		int last_file_read_level;

		VersionSet* vset;
		Status s;
		bool found;

		// ForEachOverlapping 的 Callback
		static bool Match(void* arg, int level, FileMetaData* f)
		{
			State* state = reinterpret_cast<State*>(arg);

			if (state->stats->seek_file == nullptr
				&& state->last_file_read != nullptr)
			{
                // 这里表示本次 Get 中 Seek 了不止一个 file
				// We have had more than one seek for this read.  Charge the 1st file.
				state->stats->seek_file = state->last_file_read;
				state->stats->seek_file_level = state->last_file_read_level;
			}

			state->last_file_read = f;
			state->last_file_read_level = level;

			state->s = state->vset->table_cache_->Get(*state->options,
                                                      f->number, f->file_size,
                                                      state->ikey,
													  &state->saver, SaveValue);
			if (!state->s.ok())
			{
				state->found = true;    // ?
				return false;
			}
			
			switch (state->saver.state)
			{
				case kNotFound:
					return true;  // Keep searching in other files
				case kFound:
					state->found = true;
					return false;
				case kDeleted:
					return false;
				case kCorrupt:
					state->s =
						Status::Corruption("corrupted key for ", state->saver.user_key);
					state->found = true;    // ?
					return false;
			}

			// Not reached. Added to avoid false compilation warnings of
			// "control reaches end of non-void function".
			return false;
		}
  	};

	State state;
	state.found = false;
	state.stats = stats;
	state.last_file_read = nullptr;
	state.last_file_read_level = -1;

	state.options = &options;
	state.ikey = k.internal_key();
	state.vset = vset_;

	state.saver.state = kNotFound;
	state.saver.ucmp = vset_->icmp_.user_comparator();
	state.saver.user_key = k.user_key();
	state.saver.value = value;

	ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

	return state.found ? state.s : Status::NotFound(Slice());
}

/**
 * 如果对一个 key range 的 Version::Get() 需要 Seek 多个 file
 * 对所 Seek 的第一个 file(newest?) 记数, 记数超过阈值时将该 file 标记为 file to compact
 * @return 当更新了本次 stat 中标记文件的 allowed seeks 记数后认为该 file 可以 compact 则返回 true
 *         否则返回 false
 */
bool Version::UpdateStats(const GetStats& stats) {
    FileMetaData* f = stats.seek_file;
    if (f != nullptr)
    {
        f->allowed_seeks--;
        
        if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr)
        {
            file_to_compact_ = f;
            file_to_compact_level_ = stats.seek_file_level;
            return true;
        }
    }
    return false;
}

bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() { ++refs_; }

void Version::Unref()
{
    assert(this != &vset_->dummy_versions_);
    assert(refs_ >= 1);
    --refs_;
    if (refs_ == 0) delete this;
}

bool Version::OverlapInLevel(int level,
                             const Slice* smallest_user_key,
                             const Slice* largest_user_key)
{
    return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level], smallest_user_key, largest_user_key);
}

int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key)
{
  	int level = 0;
  	if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key))
	{
		InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
		InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
		std::vector<FileMetaData*> overlaps;

		// 如果 level+1 中不存在 overlap 且 level+2 中 overlapping 的 #bytes 有限
		// 则将检查的 level 向下推进一层
		while (level < config::kMaxMemCompactLevel)	// MAX 2
		{
			if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key))
			{
				// 若发现在某一个 level 的 next level 中存在 overlap 则退出并返回
				break;
			}

			if (level + 2 < config::kNumLevels)
			{
				// 对于一个 level, 若 level+1 中不存在 overlap
				// 则尝试检查 level+2 中是否存在 overlap
				GetOverlappingInputs(level + 2, &start, &limit, &overlaps);

				// Check that file does not overlap too many grandparent bytes.
				const int64_t sum = TotalFileSize(overlaps);
				if (sum > MaxGrandParentOverlapBytes(vset_->options_))
				{
					break;
				}
			}
			level++;
		}
	}
  	return level;
}


/**
 * 遍历一个 level, 将所有与传入 key range 存在 overlap 的文件存入 inputs
 * 
 * 需要注意的是, 如果是在 level-0, 因为不同文件之间可能相互重叠
 * 因此发现一个文件 overlap 时会将 key range 更新为 [min(begin, file_smallest), max(end, file_largest)]
 * 并重新搜索该 level
 * @param level[IN]
 * @param begin[IN]
 * @param end[IN]
 * @param inputs[OUT], 该 level 中对于一个 key range [begin, end] 认为存在 overlap 的 files
 */
void Version::GetOverlappingInputs(int level,
                                   const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs)
{
    assert(level >= 0);
    assert(level < config::kNumLevels);

    inputs->clear();
    Slice user_begin, user_end;
    if (begin != nullptr) user_begin = begin->user_key();
    if (end != nullptr) user_end = end->user_key();
    const Comparator* user_cmp = vset_->icmp_.user_comparator();

    // 按序遍历该 level 中的所有 files
    for (size_t i = 0; i < files_[level].size();)
    {
        FileMetaData* f = files_[level][i++];
        const Slice file_start = f->smallest.user_key();
        const Slice file_limit = f->largest.user_key();

        // 有 begin, 且当前 file 的 largest < begin
        if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0)
        {
            // "f" is completely before specified range; skip it
        }
        // 有 end, 且当前 file 的 smallest > end
        else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0)
        {
            // "f" is completely after specified range; skip it
        }
        // 当前 file 与期望的 key range 存在 overlap
        else
        {
            inputs->push_back(f);

            // Level-0 files 可能相互重叠, 因此要检查新增加的文件是否扩大了 range
            // 即指定了上/下界的情况, 且当前的 key range 落在一个 level 0 的 key range
            // 此时会将 key range 的上/下界置为该 level-0 的 上/下界, 然后重新搜索
            if (level == 0)
            {
                // Level-0 files may overlap each other.  So check if the newly
                // added file has expanded the range.  If so, restart search.
                // 有 begin, 且该 level-0 file 的 smallest < begin
                if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0)
                {
                    user_begin = file_start;
                    inputs->clear();
                    i = 0;
                }
                // 有 end, 且当前 file 的 largest > end
                else if (end != nullptr && user_cmp->Compare(file_limit, user_end) > 0)
                {
                    user_end = file_limit;
                    inputs->clear();
                    i = 0;
                }
            }
        }
    }
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// helper class
// 用于有效地将一整个 VersionEdit 序列应用于一个特定的状态
// 而不需要创建包含中间状态的完整副本的 intermediate Versions
class VersionSet::Builder {
private:
    // Helper to sort by v->files_[file_number].smallest
    // 首先根据 file 的 smallest key 升序排序
    // smallest key 相同时则根据 file number 升序排序
    struct BySmallestKey
    {
        const InternalKeyComparator* internal_comparator;

        bool operator()(FileMetaData* f1, FileMetaData* f2) const
        {
            int r = internal_comparator->Compare(f1->smallest, f2->smallest);
            if (r != 0)
                return (r < 0);
            else
                // Break ties by file number
                return (f1->number < f2->number);
        }
    };

    typedef std::set<FileMetaData*, BySmallestKey> FileSet;
    struct LevelState
    {
        std::set<uint64_t> deleted_files;
        FileSet* added_files;
    };

    VersionSet* vset_;
    Version* base_;
    LevelState levels_[config::kNumLevels];

public:
    // Initialize a builder with the files from *base and other info from *vset
    Builder(VersionSet* vset, Version* base)
        : vset_(vset)
        , base_(base)
    {
        base_->Ref();
        BySmallestKey cmp;
        cmp.internal_comparator = &vset_->icmp_;
        for (int level = 0; level < config::kNumLevels; level++)
        {
            levels_[level].added_files = new FileSet(cmp);
        }
    }

    ~Builder()
    {
        for (int level = 0; level < config::kNumLevels; level++)
        {
            const FileSet* added = levels_[level].added_files;
            std::vector<FileMetaData*> to_unref;
            to_unref.reserve(added->size());
            for (FileSet::const_iterator it = added->begin();
                it != added->end();
                ++it)
            {
                to_unref.push_back(*it);
            }
            delete added;

            for (uint32_t i = 0; i < to_unref.size(); i++)
            {
                FileMetaData* f = to_unref[i];
                f->refs--;
                if (f->refs <= 0)
                {
                    delete f;
                }
            }
        }
        base_->Unref();
    }

    // Apply all of the edits in *edit to the current state.
    void Apply(VersionEdit* edit)
    {
        // Update compaction pointers (各 level 下次 compaction 的 start key)
        for (size_t i = 0; i < edit->compact_pointers_.size(); i++)
        {
            const int level = edit->compact_pointers_[i].first;
            vset_->compact_pointer_[level] =
                edit->compact_pointers_[i].second.Encode().ToString();
        }

        // Delete files
        for (const auto& deleted_file_set_kvp : edit->deleted_files_)
        {
            const int level = deleted_file_set_kvp.first;
            const uint64_t number = deleted_file_set_kvp.second;
            levels_[level].deleted_files.insert(number);
        }

        // Add new files
        for (size_t i = 0; i < edit->new_files_.size(); i++)
        {
            const int level = edit->new_files_[i].first;
            FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
            f->refs = 1;

			// 我们会在一定次数的 seeks 后自动 compact 一个文件. 假设:
			//   (1) 一次 Seek 花费了 10ms
            //   (2) 读/写一个 1MB 的数据花费了 10ms (100MB/s)
			//   (3) 一次 1MB 的 Compaction 会产生 25MB 的 IO:
			// 		- 该 level 的读: 1MB 
			// 		- next level 的读: 10-12MB (boundaries 可能没有对齐)
			// 		- 向 next leve 的写: 10-12MB
			// 这意味着对于 1MB 数据的 Compaction 会有 25 次 Seeks 的开销
			// 也就是说，每 Compact 40KB 的数据就等价于一次 Seek 的成本
			// 保守估计来说, 我们允许在触发一次 Compaction 前, 每 16KB 的数据可以有一次 Seek

            f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
            if (f->allowed_seeks < 100) f->allowed_seeks = 100;

            levels_[level].deleted_files.erase(f->number);
            levels_[level].added_files->insert(f);
        }
    }

    /**
     * 根据 current state 以及 Apply() 的 edit 生成新 Version 的 files_(即每层的 FileMetaData)
     * 这个过程中会通过 MaybeAddFile 应用 VersionEdit 中 deleted files 和 added files
     * @param v[OUT], 最初 v 是空的, 表示生成的新 Version
     */
    void SaveTo(Version* v)
    {
        BySmallestKey cmp;
        cmp.internal_comparator = &vset_->icmp_;

        // 逐层处理
        for (int level = 0; level < config::kNumLevels; level++)
        {
            // Merge the set of added files with the set of pre-existing files.
            // Drop any deleted files.
            // Store the result in *v.

            // current base
            const std::vector<FileMetaData*>& base_files = base_->files_[level];
            std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
            std::vector<FileMetaData*>::const_iterator base_end = base_files.end();

            // versionEdit
            const FileSet* added_files = levels_[level].added_files;
            v->files_[level].reserve(base_files.size() + added_files->size());

            for (const auto& added_file : *added_files)
            {
                // 对该 level 中 base 的所有 < added_file 的 FileMetaData 调用 MaybeAddFile
                for (std::vector<FileMetaData*>::const_iterator bpos =
                        std::upper_bound(base_iter, base_end, added_file, cmp);
                    base_iter != bpos;
                    ++base_iter)
                {
                    MaybeAddFile(v, level, *base_iter);
                }

                // 然后对 added file 调用
                MaybeAddFile(v, level, added_file);
            }

            // Add remaining base files
            for (; base_iter != base_end; ++base_iter)
            {
                MaybeAddFile(v, level, *base_iter);
            }

#ifndef NDEBUG
            // Make sure there is no overlap in levels > 0
            if (level > 0)
            {
                for (uint32_t i = 1; i < v->files_[level].size(); i++)
                {
                    const InternalKey& prev_end = v->files_[level][i - 1]->largest;
                    const InternalKey& this_begin = v->files_[level][i]->smallest;
                    if (vset_->icmp_.Compare(prev_end, this_begin) >= 0)
                    {
                        std::fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                                    prev_end.DebugString().c_str(),
                                    this_begin.DebugString().c_str());
                        std::abort();
                    }
                }
            }
#endif
        }
    }

    // 对 MaybeAddFile 的调用是有序的
    void MaybeAddFile(Version* v, int level, FileMetaData* f)
    {
        if (levels_[level].deleted_files.count(f->number) > 0)
        {
            // File is deleted: do nothing
        }
        else
        {
            std::vector<FileMetaData*>* files = &v->files_[level];
            // 对于非 level-0, 要求该 level 中不同文件之间不能存在 overlap
            if (level > 0 && !files->empty())
            {
                // 对 MaybeAddFile 的调用是有序的, 因此判断前一个的最大值是否 < 新的最小值
                assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                            f->smallest) < 0);
            }
            f->refs++;
            files->push_back(f);
        }
    }
};

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env)
    , dbname_(dbname)
    , options_(options)
    , table_cache_(table_cache)
    , icmp_(*cmp)
    , next_file_number_(2)
    , manifest_file_number_(0)  // Filled by Recover()
    , last_sequence_(0)
    , log_number_(0)
    , prev_log_number_(0)
    , descriptor_file_(nullptr)
    , descriptor_log_(nullptr)
    , dummy_versions_(this)
    , current_(nullptr) {
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

/**
 * 将 *edit 应用于 current version，生成一个新的描述符
 * 生成的新描述符既被保存到持久化状态，又被 install 为新的 current version
 * 在实际写入文件时将释放 *mu
 * REQUIRES: *mu is held on entry.
 * REQUIRES: no other thread concurrently calls LogAndApply()
 * 
 * @param edit, current version 需要 apply 的变化
 * @param mu
 */
Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu)
{
    // TODO: log number 是啥
    if (edit->has_log_number_)
    {
        assert(edit->log_number_ >= log_number_);
        assert(edit->log_number_ < next_file_number_);
    }
    else
    {
        edit->SetLogNumber(log_number_);
    }

    if (!edit->has_prev_log_number_)
    {
        edit->SetPrevLogNumber(prev_log_number_);
    }

    edit->SetNextFile(next_file_number_);
    edit->SetLastSequence(last_sequence_);

    Version* v = new Version(this);

    {
        Builder builder(this, current_);
        builder.Apply(edit);
        builder.SaveTo(v);
    }

    Finalize(v);

    // Initialize new descriptor log file if necessary by creating
    // a temporary file that contains a snapshot of the current version.
    std::string new_manifest_file;
    Status s;
    if (descriptor_log_ == nullptr)
    {
        // No reason to unlock *mu here since we only hit this path in the
        // first call to LogAndApply (when opening the database).
        assert(descriptor_file_ == nullptr);
        new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
        edit->SetNextFile(next_file_number_);
        s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
        if (s.ok())
        {
            descriptor_log_ = new log::Writer(descriptor_file_);
            s = WriteSnapshot(descriptor_log_);
        }
    }

    // Unlock during expensive MANIFEST log write
    {
        mu->Unlock();

        // Write new record to MANIFEST log
        if (s.ok())
        {
            std::string record;
            edit->EncodeTo(&record);
            s = descriptor_log_->AddRecord(record);
            if (s.ok()) {
                s = descriptor_file_->Sync();
            }
            if (!s.ok()) {
                Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
            }
        }

        // If we just created a new descriptor file, install it by writing a
        // new CURRENT file that points to it.
        if (s.ok() && !new_manifest_file.empty())
        {
            s = SetCurrentFile(env_, dbname_, manifest_file_number_);
        }

        mu->Lock();
    }

    // Install the new version
    if (s.ok())
    {
        AppendVersion(v);
        log_number_ = edit->log_number_;
        prev_log_number_ = edit->prev_log_number_;
    }
    else
    {
        delete v;
        if (!new_manifest_file.empty())
        {
            delete descriptor_log_;
            delete descriptor_file_;
            descriptor_log_ = nullptr;
            descriptor_file_ = nullptr;
            env_->RemoveFile(new_manifest_file);
        }
    }

    return s;
}

Status VersionSet::Recover(bool* save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption("CURRENT points to a non-existent file",
                                s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);
  int read_records = 0;

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true /*checksum*/,
                       0 /*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      ++read_records;
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  } else {
    std::string error = s.ToString();
    Log(options_->info_log, "Error recovering version set with %d records: %s",
        read_records, error.c_str());
  }

  return s;
}

bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

/**
 * 逐层判断该 level 是否最需要进行 compaction
 * - 对于 level-0 的判断标准是 file num
 * - 对于其他 level 的判断标准是 total file size
 * @param v[OUT], 最需要 compaction 的 level & score 会存入 v 的成员
 */
void VersionSet::Finalize(Version* v)
{
    // Precomputed best level for next compaction
    int best_level = -1;
    double best_score = -1;

    for (int level = 0; level < config::kNumLevels - 1; level++)
    {
        double score;
        if (level == 0)
        {
            // 对于 level-0 作特殊处理, 限定文件数而不是字节数. 有两个原因:
            // 
            // (1) 对于有较大的 write-buffer 情况下，最好不要做太多的 level-0 compactions
            // 
            // (2) 每次读操作都需要对 level-0 的文件做 merge
            //     因此我们希望在单个文件大小较小的情况下避免存在过多的文件
            //    （这也许是因为 write-buffer 设置较小，或者压缩率很高，或者有很多 overwrites/deletions）
            score = v->files_[level].size() /
                    static_cast<double>(config::kL0_CompactionTrigger);
        }
        else
        {
            // Compute the ratio of current size to size limit.
            const uint64_t level_bytes = TotalFileSize(v->files_[level]);
            score =
                static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
        }

        if (score > best_score)
        {
            best_level = level;
            best_score = score;
        }
    }

    v->compaction_level_ = best_level;
    v->compaction_score_ = best_score;
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  static_assert(config::kNumLevels == 7, "");
  std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
      int(current_->files_[0].size()), int(current_->files_[1].size()),
      int(current_->files_[2].size()), int(current_->files_[3].size()),
      int(current_->files_[4].size()), int(current_->files_[5].size()),
      int(current_->files_[6].size()));
  return scratch->buffer;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(options, files[i]->number,
                                                  files[i]->file_size);
        }
      } else {
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != nullptr);
  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);
    c = new Compaction(options_, level);

    // Pick the first file that comes after compact_pointer_[level]
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      if (compact_pointer_[level].empty() ||
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        c->inputs_[0].push_back(f);
        break;
      }
    }
    if (c->inputs_[0].empty()) {
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return nullptr;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  SetupOtherInputs(c);

  return c;
}

// Finds the largest key in a vector of files. Returns true if files it not
// empty.
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;
  for (size_t i = 1; i < files.size(); ++i) {
    FileMetaData* f = files[i];
    if (icmp.Compare(f->largest, *largest_key) > 0) {
      *largest_key = f->largest;
    }
  }
  return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
FileMetaData* FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& level_files,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  FileMetaData* smallest_boundary_file = nullptr;
  for (size_t i = 0; i < level_files.size(); ++i) {
    FileMetaData* f = level_files[i];
    if (icmp.Compare(f->smallest, largest_key) > 0 &&
        user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) ==
            0) {
      if (smallest_boundary_file == nullptr ||
          icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files) {
  InternalKey largest_key;

  // Quick return if compaction_files is empty.
  if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
    return;
  }

  bool continue_searching = true;
  while (continue_searching) {
    FileMetaData* smallest_boundary_file =
        FindSmallestBoundaryFile(icmp, level_files, largest_key);

    // If a boundary file was found advance largest_key, otherwise we're done.
    if (smallest_boundary_file != NULL) {
      compaction_files->push_back(smallest_boundary_file);
      largest_key = smallest_boundary_file->largest;
    } else {
      continue_searching = false;
    }
  }
}

void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;

  AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]);
  GetRange(c->inputs_[0], &smallest, &largest);

  current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                 &c->inputs_[1]);
  AddBoundaryInputs(icmp_, current_->files_[level + 1], &c->inputs_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                     &expanded1);
      AddBoundaryInputs(icmp_, current_->files_[level + 1], &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size), int(expanded0.size()),
            int(expanded1.size()), long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->RemoveFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    while (level_ptrs_[lvl] < files.size()) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
         icmp->Compare(internal_key,
                       grandparents_[grandparent_index_]->largest.Encode()) >
             0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

}  // namespace leveldb
