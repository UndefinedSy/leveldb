// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <cstdint>
#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct Options;

// BlockBuilder 相当于是 Block Writer
class BlockBuilder {
public:
	explicit BlockBuilder(const Options* options);

	BlockBuilder(const BlockBuilder&) = delete;
	BlockBuilder& operator=(const BlockBuilder&) = delete;

	// Reset the contents as if the BlockBuilder was just constructed.
	void Reset();

	// REQUIRES: 上一次调用 Reset() 之后没有调用过 Finish()
	// REQUIRES: 传入参数 key 要比任何已经加入的 key 大
	void Add(const Slice& key, const Slice& value);
	
	// 用于完成该 block 的构建, 即将 block trailer 给 append 到 kv pairs 之后
    // 返回一个指向 block contents 的 slice
	// 该 slice 在这个 builder 的生命周期内 或 在调用 Reset() 之前保持 valid
	Slice Finish();

	// Returns an estimate of the current (uncompressed) size of the block
	// we are building.
	// 返回正在构建 block 的未压缩的 size 估计值
	size_t CurrentSizeEstimate() const;

	// Return true iff no entries have been added since the last Reset()
	bool empty() const { return buffer_.empty(); }

private:
	const Options* options_;
	std::string buffer_;              // Destination buffer
	std::vector<uint32_t> restarts_;  // Restart points
	int counter_;                     // Number of entries emitted since restart
	bool finished_;                   // Has Finish() been called?
	std::string last_key_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
