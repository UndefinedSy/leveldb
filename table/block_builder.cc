// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder 用于生成 blocks，其中 key 是进行前缀压缩的
// 
// 当存储一个 key 时，会丢弃其与前一个 string 共享的前缀，以减少空间消耗
// 此外，每隔 K 个 keys，会进行一次不采取前缀压缩的方式，存储整个键，称为 "restart point"
// 一个 block 的尾部会存储 restart points 的 offset，在寻找某个 key 时也可用来做二分搜索
// Value 是以无压缩的形式存储在相应的 Key 之后的
// 
// 对于一个 KV Pair 的 entry 有着如下的构成:
// ```
// +----------------+----------------------+
// | shared_bytes   | varint32             |  // shared_byte == 0 表示 restart point
// +----------------+----------------------+
// | unshared_bytes | varint32             |
// +----------------+----------------------+
// | value_length   | varint32             |
// +----------------+----------------------+
// | key_delta      | char[unshared_bytes] |
// +----------------+----------------------+
// | value          | char[value_length]   |
// +----------------+----------------------+
// ```
// 
// 一个 Block 的 trailer 有着如下的构成:
// ```
// +--------------+----------------------+
// | restarts     | uint32[num_restarts] |
// +--------------+----------------------+
// | num_restarts | uint32               |
// +--------------+----------------------+
// ```
// 其中 restarts[i] 中记录的是该 block 中第 i 个 restart point 的 offset

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false)
{
	assert(options->block_restart_interval >= 1);
	restarts_.push_back(0);  // First restart point is at offset 0
}

void BlockBuilder::Reset() {
	buffer_.clear();
	restarts_.clear();
	restarts_.push_back(0);  // First restart point is at offset 0
	counter_ = 0;
	finished_ = false;
	last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
    return (buffer_.size() +                       // Raw data buffer
            restarts_.size() * sizeof(uint32_t) +  // Restart array
            sizeof(uint32_t));                     // Restart array length
}

// 完成该 Block 的 build, 向尾部添加 trailer meta
// +--------------+----------------------+
// | restarts     | uint32[num_restarts] |
// +--------------+----------------------+
// | num_restarts | uint32               |
// +--------------+----------------------+
Slice BlockBuilder::Finish() {
    // Append restart array
    for (size_t i = 0; i < restarts_.size(); i++) {
        PutFixed32(&buffer_, restarts_[i]);
    }
    PutFixed32(&buffer_, restarts_.size());
    finished_ = true;
    return Slice(buffer_);
}

// 添加 KV Pair 的逻辑, 其中每个 restart point 之间的 key 会根据其 previous key 做前缀压缩
// +----------------+----------------------+
// | shared_bytes   | varint32             |  // shared_byte == 0 表示 restart point
// +----------------+----------------------+
// | unshared_bytes | varint32             |
// +----------------+----------------------+
// | value_length   | varint32             |
// +----------------+----------------------+
// | key_delta      | char[unshared_bytes] |
// +----------------+----------------------+
// | value          | char[value_length]   |
// +----------------+----------------------+
void BlockBuilder::Add(const Slice& key, const Slice& value) {
    Slice last_key_piece(last_key_);
    assert(!finished_);
    assert(counter_ <= options_->block_restart_interval);
    assert(buffer_.empty()  // No values yet?
           || options_->comparator->Compare(key, last_key_piece) > 0);
    
    size_t shared = 0;  // 与 last_key_ 的公共前缀长度
    // try prefix compression
    if (counter_ < options_->block_restart_interval)
    {
        // See how much sharing to do with previous string
        const size_t min_length = std::min(last_key_piece.size(), key.size());
        while ((shared < min_length) && (last_key_piece[shared] == key[shared]))
        {
            shared++;
        }
    }
    // restart point 
    else
    {
        // Restart compression
        restarts_.push_back(buffer_.size());    // 记录该 offset 为 restart point
        counter_ = 0;
    }
    const size_t non_shared = key.size() - shared;

    // Add meta "<shared><non_shared><value_size>" to buffer_
    PutVarint32(&buffer_, shared);
    PutVarint32(&buffer_, non_shared);
    PutVarint32(&buffer_, value.size());

    // Add string delta to buffer_ followed by value
    buffer_.append(key.data() + shared, non_shared);
    buffer_.append(value.data(), value.size());

    // Update state
    last_key_.resize(shared);   // last_key_ 收缩到公共前缀部分
    last_key_.append(key.data() + shared, non_shared);  // append 上非公共部分, 即更新为新的 key
    assert(Slice(last_key_) == key);
    counter_++;
}

}  // namespace leveldb
