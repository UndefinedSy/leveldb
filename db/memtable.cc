// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& comparator)
    : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {}

MemTable::~MemTable() { assert(refs_ == 0); }

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

// 一个 Memtable 中的 Entry 的构成:
// ```
// +--------------+-------------------------------+
// |  key_size    | varint32, internal_key.size() |
// +--------------+-------------------------------+
// |  key_bytes   | char[internal_key.size()]     |
// |   - userkey  |   - (key_size - 8)            |
// |   - tag      |   - 8B (7B Sequence + 1B Type)|
// +--------------+-------------------------------+
// |  value_size  | varint32, value.size()        |
// +--------------+-------------------------------+
// |  value_bytes | char[value.size()]            |
// +--------------+-------------------------------+
// ```

void
MemTable::Add(SequenceNumber s, ValueType type,
              const Slice& key, const Slice& value)
{
    size_t key_size = key.size();
    size_t val_size = value.size();
    size_t internal_key_size = key_size + 8;
    const size_t encoded_len = VarintLength(internal_key_size) + internal_key_size +
                               VarintLength(val_size) + val_size;
    char* buf = arena_.Allocate(encoded_len);

    // Encode Key Part
    char* p = EncodeVarint32(buf, internal_key_size);
    std::memcpy(p, key.data(), key_size);
    p += key_size;
    EncodeFixed64(p, (s << 8) | type);
    p += 8;

    // Encode Value Part
    p = EncodeVarint32(p, val_size);
    std::memcpy(p, value.data(), val_size);
    assert(p + val_size == buf + encoded_len);

    table_.Insert(buf);
}

/**
 * 
 * @param key[IN]
 * @param value[OUT]
 * @param s[OUT]
 */
bool
MemTable::Get(const LookupKey& key, std::string* value, Status* s)
{
    Slice memkey = key.memtable_key();
    Table::Iterator iter(&table_);
    iter.Seek(memkey.data());
    // Seek 到 >= memkey 的第一个记录
    if (iter.Valid())
    {
        // entry format is:
        //    klength  varint32
        //    userkey  char[klength]
        //    tag      uint64
        //    vlength  varint32
        //    value    char[vlength]
        // Check that it belongs to same user key.  We do not check the
        // sequence number since the Seek() call above should have skipped
        // all entries with overly large sequence numbers.
        // 检查是否是同一个 Userkey.
        // 这里不需要检查 Sequence Number, Seek() 会跳过所有 SequenceNum 过大的 entries
        // ??? 不是小于?
        const char* entry = iter.key();
        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
        // 检查 input key 中的 userkey 是否与 skiplist 的 user_key 相同
        if (comparator_.comparator.user_comparator()
                ->Compare(Slice(key_ptr, key_length - 8), key.user_key()) == 0)
        {
            // Correct user key
            const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
            switch (static_cast<ValueType>(tag & 0xff)) {
                // ValueType 为 kTypeValue 则解析 Value 部分
                case kTypeValue: {
                    Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
                    value->assign(v.data(), v.size());
                    return true;
                }
                // ValueType 为 Tombstone
                case kTypeDeletion:
                    *s = Status::NotFound(Slice());
                    return true;
            }
        }
    }
    return false;
}

}  // namespace leveldb
