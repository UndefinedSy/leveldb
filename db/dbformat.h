// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBFORMAT_H_
#define STORAGE_LEVELDB_DB_DBFORMAT_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

// Grouping of constants.  We may want to make some of these
// parameters set via options.
namespace config {
static const int kNumLevels = 7;

// Level-0 compaction is started when we hit this many files.
static const int kL0_CompactionTrigger = 4;

// Soft limit on number of level-0 files.  We slow down writes at this point.
static const int kL0_SlowdownWritesTrigger = 8;

// Maximum number of level-0 files.  We stop writes at this point.
static const int kL0_StopWritesTrigger = 12;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
static const int kMaxMemCompactLevel = 2;

// Approximate gap in bytes between samples of data read during iteration.
static const int kReadBytesPeriod = 1048576;

}  // namespace config

class InternalKey;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
enum ValueType { kTypeDeletion = 0x0, kTypeValue = 0x1 };
// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
static const ValueType kValueTypeForSeek = kTypeValue;

typedef uint64_t SequenceNumber;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1);

// Exactly Parsed InternalKey 
struct ParsedInternalKey {
    Slice user_key;
    SequenceNumber sequence;
    ValueType type;

    ParsedInternalKey() {}  // Intentionally left uninitialized (for speed)
    ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
        : user_key(u), sequence(seq), type(t) {}
    std::string DebugString() const;
};

// Return the length of the encoding of "key".
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
  return key.user_key.size() + 8;
}

// Append the serialization of "key" to *result.
void AppendInternalKey(std::string* result, const ParsedInternalKey& key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
//
// On error, returns false, leaves "*result" in an undefined state.
bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result);

// Returns the user key portion of an internal key.
inline Slice ExtractUserKey(const Slice& internal_key) {
	assert(internal_key.size() >= 8);
	return Slice(internal_key.data(), internal_key.size() - 8);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
// 对 user key 进行比较，并通过 sequence number 进行降序排序
class InternalKeyComparator : public Comparator {
private:
    const Comparator* user_comparator_;

public:
    explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) {}
    const char* Name() const override;
    int Compare(const Slice& a, const Slice& b) const override;
    void FindShortestSeparator(std::string* start,
                                const Slice& limit) const override;
    void FindShortSuccessor(std::string* key) const override;

    const Comparator* user_comparator() const { return user_comparator_; }

    int Compare(const InternalKey& a, const InternalKey& b) const;
};


// 对 FilterPolicy 的 wrapper， 以方便地对 InternalKey 做过滤
// 该 wrapper 类会将 InternalKey 的 UserKey 部分用于 Filter Policy
class InternalFilterPolicy : public FilterPolicy
{
private:
    const FilterPolicy* const user_policy_;

public:
    explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) {}
    const char* Name() const override;
    void CreateFilter(const Slice* keys, int n, std::string* dst) const override;
    bool KeyMayMatch(const Slice& key, const Slice& filter) const override;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
class InternalKey {
// rep_:
// +--------------------+---------------+------------+
// |  user_key(string)  |  seq_num(7B)  |  type(1B)  |
// +--------------------+---------------+------------+
private:

    std::string rep_;   // rep_ 为空表示 invalid

public:
    InternalKey() {}  // Leave rep_ as empty to indicate it is invalid
    InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
        AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
    }

    // 用 Slice s 给 InternalKey 赋值并返回是否为空?
    bool DecodeFrom(const Slice& s) {
        rep_.assign(s.data(), s.size());
        return !rep_.empty();
    }

    Slice Encode() const {
        assert(!rep_.empty());
        return rep_;
    }

    Slice user_key() const { return ExtractUserKey(rep_); }

    void SetFrom(const ParsedInternalKey& p) {
        rep_.clear();
        AppendInternalKey(&rep_, p);
    }

    void Clear() { rep_.clear(); }

    std::string DebugString() const;
};

inline int InternalKeyComparator::Compare(const InternalKey& a,
                                          const InternalKey& b) const {
  return Compare(a.Encode(), b.Encode());
}

/**
 * 尝试解析 internal_key.
 * @param internal_key[IN], 待解析 InternalKey
 * @param result[OUT], 解析成功时会将解析后数据存到 result, 失败是该字段为未定义状态
 * @return 成功时返回 true, 否则返回 false
 */
inline bool
ParseInternalKey(const Slice& internal_key,
                 ParsedInternalKey* result)
{
    const size_t n = internal_key.size();
    if (n < 8) return false;
    uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
    uint8_t c = num & 0xff;
    result->sequence = num >> 8;
    result->type = static_cast<ValueType>(c);
    result->user_key = Slice(internal_key.data(), n - 8);
    return (c <= static_cast<uint8_t>(kTypeValue));
}

// A helper class useful for DBImpl::Get(), 用于 Get() 的 Key
class LookupKey {
public:
	// Initialize *this for looking up user_key at a snapshot with
	// the specified sequence number.
	LookupKey(const Slice& user_key, SequenceNumber sequence);

	LookupKey(const LookupKey&) = delete;
	LookupKey& operator=(const LookupKey&) = delete;

	~LookupKey();

	// Return a key suitable for lookup in a MemTable.
	// memtable_key 即将整个 LookupKey 转为 Slice
	Slice memtable_key() const { return Slice(start_, end_ - start_); }

	// Return an internal key (suitable for passing to an internal iterator)
	// internal_key 是去掉 klength 的部分
	Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

	// Return the user key
	Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

private:
	// LookupKey 的组成如下，即 userkey length + InternalKey.
	// +--------------------------+	<-- start_
	// |  klength  varint32       |
	// +--------------------------+ <-- kstart_
	// |  userkey  char[klength]  |
	// +--------------------------+ <-- end_
	// |  tag      uint64		  |
	// +--------------------------+
	//                              
	// The array is a suitable MemTable key.
	// The suffix starting with "userkey" can be used as an InternalKey.
	const char* start_;
	const char* kstart_;
	const char* end_;
	// LookupKey 会在栈上分配 200 个字节，以避免为 short keys 分配内存
	char space_[200];  // Avoid allocation for short keys
};

inline LookupKey::~LookupKey() {
  if (start_ != space_) delete[] start_;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DBFORMAT_H_
