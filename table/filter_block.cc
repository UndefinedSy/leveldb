// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// 每 2KB 的 data 会生成一个 new filter
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

// 开始构建新的 filter block
// TableBuilder 在:
// - 构造时 StartBlock(0)
// - Flush 时 StartBlock(the start offset of the next data block)
void
FilterBlockBuilder::StartBlock(uint64_t block_offset)
{
    // 计算出 data block 到 block_offset 位置需要多少个 2KB filter
    uint64_t filter_index = (block_offset / kFilterBase);
    assert(filter_index >= filter_offsets_.size());

    // 循环调用 GenerateFilter(), 为中间部分生成 filters
    while (filter_index > filter_offsets_.size())
    {
		// 看起来是每次 GenerateFilter 会推动 filter_offsets_ 的 size +1
		// 但是 GenerateFilter 会一次把自上一次 GenerateFilter 期间的 keys_ 消耗完
		// 所以 filter_index 需要 n 次 GenerateFilter(), 只有第 1 次有 keys
		// 2~n 次中都是 num_keys == 0, 直接向 filter_offsets_ 压入当前 filter data size
		// TODO: 为啥这么设计？？？
        GenerateFilter();
    }
}

// 将 `key` Append 到一个 flattened string `keys_`
// 并在 start_ 中记录了其起始位置
void
FilterBlockBuilder::AddKey(const Slice& key)
{
    Slice k = key;
    start_.push_back(keys_.size());
    keys_.append(k.data(), k.size());
}

// 完成 filter 部分的构建, 向 result_ 尾部 append 上各 filter 的 offset
// 其 layout 可以参考 table_format.md 中的 filter Meta Block 部分
Slice
FilterBlockBuilder::Finish()
{
	// 将剩余的部分未消耗 keys_ 生成一个 filter
    if (!start_.empty()) {
        GenerateFilter();
    }

	// Append 每个 filter 在 result_ 中的 offsets
	const uint32_t array_offset = result_.size();
	for (size_t i = 0; i < filter_offsets_.size(); i++) {
		PutFixed32(&result_, filter_offsets_[i]);
	}

	// Append 上 filter offset arrays 的起始地址
  	PutFixed32(&result_, array_offset);
	// Save encoding parameter in result
  	result_.push_back(kFilterBaseLg);
  	return Slice(result_);
}

void
FilterBlockBuilder::GenerateFilter()
{
    // 距离上次 Generate, batch 了多少 keys
    const size_t num_keys = start_.size();
    if (num_keys == 0)
    {
        // Fast path if there are no keys for this filter
        filter_offsets_.push_back(result_.size());
        return;
    }

    // 根据 flattened string `keys_` 切出来 keys list 存入 tmp_keys_
    start_.push_back(keys_.size());  // Simplify length computation
    tmp_keys_.resize(num_keys);
    for (size_t i = 0; i < num_keys; i++)
    {
        const char* base = keys_.data() + start_[i];
        size_t length = start_[i + 1] - start_[i];
        tmp_keys_[i] = Slice(base, length);
    }

	// 记录下该 filter 在 result_ 中的 start offset
	filter_offsets_.push_back(result_.size());
    // 根据 tmp_keys_ 生成一个 filter, 并将其 append 到 result_
    policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

    // reset for next GenerateFilter
    tmp_keys_.clear();
    keys_.clear();
    start_.clear();
}

/* -------------------- Filter Block Reader -------------------- */
FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy)
    , data_(nullptr)
    , offset_(nullptr)
    , num_(0)
    , base_lg_(0)
{
    size_t n = contents.size();

    if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array

    base_lg_ = contents[n - 1];	// Encoding parameter kFilterBaseLg(2KB)
	// offset array 在 filter block 中的偏移
    uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
    if (last_word > n - 5) return;

    data_ = contents.data();
    offset_ = data_ + last_word;	// offset array 的起始地址
    num_ = (n - 5 - last_word) / 4;	// offset array 中的 entries 数量
}

/**
 * @param block_offset, 要查找的 data block 在 sstable 中的 offset
 * @param key, 要查找的 key
 */
bool
FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key)
{
	// 根据 data block 在 sstable 中的 offset 定位其 filter index
	uint64_t index = block_offset >> base_lg_;

	if (index < num_)
	{
		// filter range
		uint32_t start = DecodeFixed32(offset_ + index * 4);
		uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);

		if (start <= limit && limit <= static_cast<size_t>(offset_ - data_))
		{
			// 恢复出相应的 filter 并尝试判断 Match
			Slice filter = Slice(data_ + start, limit - start);
			return policy_->KeyMayMatch(key, filter);
		}
		else if (start == limit)
		{
			// Empty filters do not match any keys
			return false;
		}
	}
	// 需要注意的是, Error 场景也被当作 potential matches 处理的
	return true;
}

}  // namespace leveldb
