// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"

#include <cstdio>
#include <sstream>

#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
    assert(seq <= kMaxSequenceNumber);
    assert(t <= kValueTypeForSeek);
    return (seq << 8) | t;
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
    result->append(key.user_key.data(), key.user_key.size());
    PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

std::string ParsedInternalKey::DebugString() const {
  std::ostringstream ss;
  ss << '\'' << EscapeString(user_key.ToString()) << "' @ " << sequence << " : "
     << static_cast<int>(type);
  return ss.str();
}

std::string InternalKey::DebugString() const {
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    return parsed.DebugString();
  }
  std::ostringstream ss;
  ss << "(bad)" << EscapeString(rep_);
  return ss.str();
}

const char* InternalKeyComparator::Name() const {
  return "leveldb.InternalKeyComparator";
}

// InternalKey 的比较顺位:
// - 首先是按照 user key(根据用户提供的 comparator) 进行升序排序
// - 其次是 sequence number 降序排序
// - 最后根据 type 降序排序
int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
    int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
    if (r == 0) {
        const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
        const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
        if (anum > bnum) {
            r = -1;
        } else if (anum < bnum) {
            r = +1;
        }
    }
    return r;
}

// 取出 Internal Key 中的 UserKey 字段, 根据 user 指定的 Comparator 和 limit 尝试替换 start
// - 如果 start 被截短了，就用新的 start 并使用最大的sequence number
// - 否则保持不变
void InternalKeyComparator::FindShortestSeparator(std::string* start,
                                                  const Slice& limit) const {
    // 先尝试用 user_comparator_ 更新 userkey 部分
	Slice user_start = ExtractUserKey(*start);
	Slice user_limit = ExtractUserKey(limit);
	std::string tmp(user_start.data(), user_start.size());  // tmp 即待处理 userkey
	user_comparator_->FindShortestSeparator(&tmp, user_limit);

    // 如果 tmp userkey 在物理上长度被截短, 但其逻辑值变大了
	if (tmp.size() < user_start.size()
		&& user_comparator_->Compare(user_start, tmp) < 0)
	{
		// User key has become shorter physically, but larger logically.
		// Tack on the earliest possible number to the shortened user key.
        // 生成新的 *start: tmp + 0xFFFF FFFF FFFF FF00 + 0x1(kValueTypeForSeek)
        // kMaxSequenceNumber 保证其排在相同 user key 记录序列的第一个
		PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
		assert(this->Compare(*start, tmp) < 0);
		assert(this->Compare(tmp, limit) < 0);
		start->swap(tmp);
	}
}

// 取出 Internal Key 中的 UserKey 字段，根据 user 指定的 comparator 尝试替换 key
// - 如果 key 被替换了，就用新的 key 并使用最大的 sequence number
// - 否则保持不变
void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
	Slice user_key = ExtractUserKey(*key);
	std::string tmp(user_key.data(), user_key.size());
    // 先尝试用 user_comparator_ 更新 tmp
	user_comparator_->FindShortSuccessor(&tmp);
	if (tmp.size() < user_key.size()
		&& user_comparator_->Compare(user_key, tmp) < 0)
	{
		// User key has become shorter physically, but larger logically.
		// Tack on the earliest possible number to the shortened user key.
        // tmp 在物理上被截短了，但其逻辑值变大了.
        // 生成新的 *key: tmp + kMaxSequenceNumber + kValueTypeForSeek
		PutFixed64(&tmp,
				PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
		assert(this->Compare(*key, tmp) < 0);
		key->swap(tmp);
	}
}

const char* InternalFilterPolicy::Name() const { return user_policy_->Name(); }

void
InternalFilterPolicy::CreateFilter(const Slice* keys, int n, std::string* dst) const
{
	// 这里认为 table.cc 中的代码不介意调整 keys[]
	Slice* mkey = const_cast<Slice*>(keys);
	for (int i = 0; i < n; i++) {
		mkey[i] = ExtractUserKey(keys[i]);
		// TODO(sanjay): Suppress dups?
	}
	user_policy_->CreateFilter(keys, n, dst);
}

bool
InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
    return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size();
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  dst = EncodeVarint32(dst, usize + 8);
  kstart_ = dst;
  std::memcpy(dst, user_key.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

}  // namespace leveldb
