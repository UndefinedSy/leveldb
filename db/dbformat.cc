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

// TODO
void InternalKeyComparator::FindShortestSeparator(std::string* start,
                                                  const Slice& limit) const {
    // 先尝试用 user_comparator_ 更新 userkey 部分
	Slice user_start = ExtractUserKey(*start);
	Slice user_limit = ExtractUserKey(limit);
	std::string tmp(user_start.data(), user_start.size());
	user_comparator_->FindShortestSeparator(&tmp, user_limit);

    // 如果 userkey 长度被截短, 但其在逻辑空间上变大了
	if (tmp.size() < user_start.size()
		&& user_comparator_->Compare(user_start, tmp) < 0)
	{
		// User key has become shorter physically, but larger logically.
		// Tack on the earliest possible number to the shortened user key.
		PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
		assert(this->Compare(*start, tmp) < 0);
		assert(this->Compare(tmp, limit) < 0);
		start->swap(tmp);
	}
}

// TODO
void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
	Slice user_key = ExtractUserKey(*key);
	std::string tmp(user_key.data(), user_key.size());
	user_comparator_->FindShortSuccessor(&tmp);
	if (tmp.size() < user_key.size()
		&& user_comparator_->Compare(user_key, tmp) < 0)
	{
		// User key has become shorter physically, but larger logically.
		// Tack on the earliest possible number to the shortened user key.
		PutFixed64(&tmp,
				PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
		assert(this->Compare(*key, tmp) < 0);
		key->swap(tmp);
	}
}

const char* InternalFilterPolicy::Name() const { return user_policy_->Name(); }

void InternalFilterPolicy::CreateFilter(const Slice* keys, int n,
                                        std::string* dst) const {
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  Slice* mkey = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]);
    // TODO(sanjay): Suppress dups?
  }
  user_policy_->CreateFilter(keys, n, dst);
}

bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
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
