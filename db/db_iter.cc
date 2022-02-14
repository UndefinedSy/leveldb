// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      std::fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      std::fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {


// Memtables 和 SSTables 构成了包含有 (userkey,seq,type) => uservalue 的 DB representation
// DBIter 将 DB representation 中找到的, 对于同一个 userkey 的多个 entries 合并为一个 entry
// DBIter 需要负责考虑 sequence numbers, deletion markers, overwrtes 等情况.
class DBIter : public Iterator
{
public:
    // 描述了当前 iterator 的移动方向
    // (1) 当方向为 kForward 时, internal iterator 指向生成
    //     this->key(), this->value() 的 entry
    // (2) 当方向为 kReverse 时, internal iterator 指向所有的
    //     user key == this->key() 的 entry 之前的位置
    enum Direction { kForward, kReverse };

    DBIter(const DBIter&) = delete;
    DBIter& operator=(const DBIter&) = delete;

    DBIter(DBImpl* db, const Comparator* cmp,
           Iterator* iter, SequenceNumber s, uint32_t seed)
      : db_(db)
      , user_comparator_(cmp)
      , iter_(iter)
      , sequence_(s)
      , direction_(kForward)
      , valid_(false)
      , rnd_(seed)
      , bytes_until_read_sampling_(RandomCompactionPeriod())
      {}

    ~DBIter() override { delete iter_; }

    bool Valid() const override { return valid_; }
    Slice key() const override {
        assert(valid_);
        return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
    }
    Slice value() const override {
        assert(valid_);
        return (direction_ == kForward) ? iter_->value() : saved_value_;
    }
    Status status() const override {
        if (status_.ok()) {
        return iter_->status();
        } else {
        return status_;
        }
    }

    void Next() override;
    void Prev() override;
    void Seek(const Slice& target) override;
    void SeekToFirst() override;
    void SeekToLast() override;

private:
    void FindNextUserEntry(bool skipping, std::string* skip);
    void FindPrevUserEntry();
    bool ParseKey(ParsedInternalKey* key);

    inline void SaveKey(const Slice& k, std::string* dst) {
        dst->assign(k.data(), k.size());
    }

    inline void ClearSavedValue()
    {
        if (saved_value_.capacity() > 1048576)
        {
            std::string empty;
            swap(empty, saved_value_);
        }
        else
        {
            saved_value_.clear();
        }
    }

    // Picks the number of bytes that can be read until a compaction is scheduled.
    // 选择在 schedule 一次 compaction 之前可以读的字节数
    size_t RandomCompactionPeriod()
    {
        return rnd_.Uniform(2 * config::kReadBytesPeriod);
    }

    DBImpl* db_;
    const Comparator* const user_comparator_;
    Iterator* const iter_;  // 底层的 merging iterator
    SequenceNumber const sequence_; // 所使用的 snapshot 对应的 sequence number
    Status status_;
    std::string saved_key_;    // == current key when direction_==kReverse
    std::string saved_value_;  // == current raw value when direction_==kReverse
    Direction direction_;
    bool valid_;
    Random rnd_;
    size_t bytes_until_read_sampling_;
};

// TODO
inline bool
DBIter::ParseKey(ParsedInternalKey* ikey)
{
    Slice k = iter_->key();

    size_t bytes_read = k.size() + iter_->value().size();
    while (bytes_until_read_sampling_ < bytes_read)
    {
        bytes_until_read_sampling_ += RandomCompactionPeriod();
        db_->RecordReadSample(k);
    }
    assert(bytes_until_read_sampling_ >= bytes_read);
    bytes_until_read_sampling_ -= bytes_read;

    if (!ParseInternalKey(k, ikey))
    {
        status_ = Status::Corruption("corrupted internal key in DBIter");
        return false;
    }
    else
    {
        return true;
    }
}

// Merging Iter 是以一条 record 为单位
// DBIter 是以一个 key 为单位, 即其 Next / Prev 会迭代到 key 发生变化

void DBIter::Next()
{
    assert(valid_);

    if (direction_ == kReverse) // Switch directions?
    {
        direction_ = kForward;

        if (!iter_->Valid())
        {
            // 这里之前是 Reverse 方向, 因此 InValid 是在 first 之前.
            iter_->SeekToFirst();
        }
        else
        {
            // iter_ 指向 this->key() 对应 entry 之前的位置
            // 因此这里进入 this->key() 对应 entry 的 range
            // 然后使用下面的 normal skipping code
            iter_->Next();
        }

        if (!iter_->Valid())
        {
            valid_ = false;
            saved_key_.clear();
            return;
        }
        // saved_key_ already contains the key to skip past.
    }
    else
    {
        // Store in saved_key_ the current key so we skip it below.
        SaveKey(ExtractUserKey(iter_->key()), &saved_key_);

        // 此时 iter_ 指向的是 current key, 因此可以直接调用 Next() 以避免检查 current key
        iter_->Next();
        if (!iter_->Valid())
        {
            valid_ = false;
            saved_key_.clear();
            return;
        }
    }

    FindNextUserEntry(true, &saved_key_);
}

/**
 * @brief 向 Forward 方向遍历寻找下一个 sequence 可见的 entry
 *        会跳过被 deletion tombstone 覆盖的 entries
 *        若找到了下一个 valid entry 则 valid_ 置 true, 否则为 false 
 * @note 最初 iter 可能是落在 current key, 也可能落在 next key(如 seek)
 * 
 * @param skipping[IN], 是否要跳过比 skip 更小的 entry,
 *                      过程中会因为 deletion tombstone 而置为 true
 * @param skip[IN/OUT], 可传递的临时缓存, 要 skip 的 user key
 */
void DBIter::FindNextUserEntry(bool skipping, std::string* skip)
{
    assert(iter_->Valid());
    assert(direction_ == kForward);

    // Loop until we hit an acceptable entry to yield
    // 循环直到找到一个 acceptable entry
    do
    {
        ParsedInternalKey ikey;
        if (ParseKey(&ikey) 
            && ikey.sequence <= sequence_)  // 该 key 在当前 snapshot 的可见范围
        {
            switch (ikey.type)
            {
                case kTypeDeletion:
                    // Arrange to skip all upcoming entries for this key since
                    // they are hidden by this deletion.
                    // 表示跳过此 key 的所有即将出现的 entries, 这些 entries 被这里的 deletion 隐藏
                    SaveKey(ikey.user_key, skip);
                    skipping = true;
                    break;
                case kTypeValue:
                    if (skipping
                        && user_comparator_->Compare(ikey.user_key, *skip) <= 0)    // TODO: Why <= skipped key
                    {
                        // Entry hidden
                    }
                    else
                    {
                        valid_ = true;
                        saved_key_.clear();
                        return;
                    }
                    break;
            }
        }

        iter_->Next();
    } while (iter_->Valid());

    saved_key_.clear();
    valid_ = false;
}

/**
 * 以下面的数据为例展示 Next/Prev 的过程
 * @note internal_key 的排序方式为:
 *       1. user_key 升序
 *       2. sequence 降序
 *       3. type 降序
 * 
 * key 1:1:1
 * key 2:3:1
 * key 2:2:1
 * key 2:1:1
 * key 3:3:1
 * key 3:2:0 (type=0 表示删除)
 * key 3:1:1
 * key 4:3:1
 * key 4:2:1
 * 
 * 假设最初 Iter 位于 4:2:1, direction 为 Forward, snapshot 为 2
 * 现在调用 Prev(), workflow 如下:
 * 0. 先置 saved key 为当前的 user key, 即 4
 * 1. 先向前找到第一个不同的 user key -- 3:1:1, dir 改为 kReverse
 *    开始 FindPrevUserEntry()
 * 2. 向前到 3:2:0, 发现 type=0(deletion), 清空 saved_key / value
 * 3. 向前到 2:1:1, 因为之前为 kTypeDeletion, 更新 saved key / value
 * 4. 向前到 2:2:1, 因此 user key 相同 2 = 2, 更新 saved key / value
 * 6. 向前到 1:1:1, 因为之前不是 Deletion 且 1:1:1 < 2:2:1, 结束遍历.
 *    此时 iter 位于 1:1:1,
 *    但 saved key / value 为 2:2:1 的 entry
 * 
 * 接着上面的场景(iter 位于 1:1:1, direction 为 kReverse, snapshot 为 2)
 * 考虑 Next() 的 workflow:
 * 1. 向后进入 this->key() 的 userkey 最大可见的 entry, 即 2:2:1
 *    开始 FindNextUserEntry()
 * 2. 最初 skipping 为 true, skip(即 saved_key) 为 2
 * 3. 向后到 2:1:1, 此时 Entry hidden
 * 4. 向后到 3:2:0, 因为 type=0(deletion), 更新 skip 为 3
 * 5. 向后到 3:1:1, 此时 Entry hidden
 * 6. 向后到 4:2:1, 因为 4 > 3, valide 置为 true 后 return
 *    此时 Iter 指向 4:2:1
 */

void DBIter::Prev()
{
    assert(valid_);

    if (direction_ == kForward)  // Switch directions?
    {
        // 此时 iter_ 指向 current entry. 此时反向 Scan 直到 key 发生变化
        // 以便使用正常的反向 Scanning code
        assert(iter_->Valid());  // Otherwise valid_ would have been false
        SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
        while (true)
        {
            iter_->Prev();
            if (!iter_->Valid())
            {
                valid_ = false;
                saved_key_.clear();
                ClearSavedValue();
                return;
            }
            // 找到前一个不同的 key 就可以出循环了
            if (user_comparator_->Compare(ExtractUserKey(iter_->key()), saved_key_) < 0)
            {
                break;
            }
        }
        direction_ = kReverse;
    }

    FindPrevUserEntry();
}

/**
 * @brief 反向移动 Iter 寻找第一个 **user_key** < saved_key 且 non-deleted entry
 * 
 * @note internal_key 的排序方式为:
 *       1. user_key 升序
 *       2. sequence 降序
 *       3. type 降序
 * @note 进入 FindPrevUserEntry 时, iter_ 位于 savedkey 对应的 entry 之前
 *       的第一个 userkey 不相同的位置
 */
void DBIter::FindPrevUserEntry()
{
    assert(direction_ == kReverse);
    // 循环中至少进行一个 Prev 操作
    ValueType value_type = kTypeDeletion;

    if (iter_->Valid())
    {
        do
        {
            ParsedInternalKey ikey;
            if (ParseKey(&ikey)
                && ikey.sequence <= sequence_) // 该 key 对于当前 snapshot 可见
            {
                // 这里对于一个 Deletion 的 key, 会循环向前遍历到前一个可见的 User key
                if ((value_type != kTypeDeletion)
                    && user_comparator_->Compare(ikey.user_key, saved_key_) < 0)
                {
                    // 找到了 previous keys 的一个 non-deleted value, 因此可以出循环了
                    break;
                }
                
                value_type = ikey.type;
                if (value_type == kTypeDeletion)
                {
                    saved_key_.clear();
                    ClearSavedValue();
                }
                else
                {
                    Slice raw_value = iter_->value();
                    if (saved_value_.capacity() > raw_value.size() + 1048576)
                    {
                        std::string empty;
                        swap(empty, saved_value_);
                    }
                    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
                    saved_value_.assign(raw_value.data(), raw_value.size());
                }
            }
            iter_->Prev();
        } while (iter_->Valid());
    }

    // 表示整个 Iter 遍历完了, 但是还没有找到一个可用的 entry
    if (value_type == kTypeDeletion)
    {
        // End
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        direction_ = kForward;
    }
    else
    {
        // 表示找到了一个可用的 entry
        // 需要注意的是, 上面的循环中 break 掉了,
        // 但是这里的 value_type 和 saved_key_ 可能还没有被更新
        valid_ = true;
    }
}

void DBIter::Seek(const Slice& target)
{
    direction_ = kForward;
    ClearSavedValue();
    saved_key_.clear();

    AppendInternalKey(&saved_key_,
                        ParsedInternalKey(target, sequence_, kValueTypeForSeek));

    iter_->Seek(saved_key_);
    if (iter_->Valid())
    {
        FindNextUserEntry(false, &saved_key_ /* temporary storage */);
    }
    else
    {
        valid_ = false;
    }
}

void DBIter::SeekToFirst()
{
    direction_ = kForward;
    ClearSavedValue();

    iter_->SeekToFirst();
    if (iter_->Valid())
    {
        FindNextUserEntry(false, &saved_key_ /* temporary storage */);
    }
    else
    {
        valid_ = false;
    }
}

void DBIter::SeekToLast()
{
    direction_ = kReverse;
    ClearSavedValue();
    iter_->SeekToLast();
    // FindPrevUserEntry 总是会进行一次 Prev, so 不用主动检查 valid
    FindPrevUserEntry();
}

}  // anonymous namespace

/**
 * 返回一个新的 iterator, 该 iterator 将在指定的 sequence 上的
 * internal keys (由 internal_iter 产生) 转换为适当的 user keys
 * @param db[IN]
 * @param user_key_comparator[IN]
 * @param iternal_iter[IN]
 * @param sequence[IN]
 * @param ssed[IN]
 */
Iterator*
NewDBIterator(DBImpl* db,
              const Comparator* user_key_comparator,
              Iterator* internal_iter,
              SequenceNumber sequence,
              uint32_t seed)
{
    return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
}

}  // namespace leveldb
