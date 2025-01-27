// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/filter_policy.h"

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

namespace {

static uint32_t
BloomHash(const Slice& key)
{
  	return Hash(key.data(), key.size(), 0xbc9f1d34);
}

class BloomFilterPolicy : public FilterPolicy
{
public:
	explicit BloomFilterPolicy(int bits_per_key)
		: bits_per_key_(bits_per_key)
	{
		// 向下舍入，以减少 probing 的成本
		// 模拟 Hash 函数个数为 ln(2) * bits_per_key
		k_ = static_cast<size_t>(bits_per_key * 0.69);  // 0.69 =~ ln(2)
		if (k_ < 1) k_ = 1;
		if (k_ > 30) k_ = 30;
	}

	// Default BloomFilter 的 Name: leveldb.BuiltinBloomFilter2
  	const char* Name() const override { return "leveldb.BuiltinBloomFilter2"; }

	void CreateFilter(const Slice* keys, int n, std::string* dst) const override
	{
		// Compute bloom filter size (in both bits and bytes)
		size_t bits = n * bits_per_key_;

		// 如果 keys 的数量很小, 以至于需要的 bits < 64, 则可能导致很高的 false positive rate.
		// 对于这种场景会强制将其约束在 minimum bloom filter length
		if (bits < 64) bits = 64;

		// 转换为向上取整的 bytes 数和对应的 bits 数
		size_t bytes = (bits + 7) / 8;
		bits = bytes * 8;

		// 预分配内存空间
		const size_t init_size = dst->size();
		dst->resize(init_size + bytes, 0);

		// 向 filter 的尾部压入模拟 hash 函数的个数
		dst->push_back(static_cast<char>(k_));  // Remember # of probes in filter

		char* array = &(*dst)[init_size];
		for (int i = 0; i < n; i++) {
			// 使用 double-hashing 以生成一个 hash values 序列
            // TODO: 具体 hash 怎么算的
			// See analysis in [Kirsch,Mitzenmacher 2006].
			uint32_t h = BloomHash(keys[i]);
			const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
			for (size_t j = 0; j < k_; j++)
            {
				const uint32_t bitpos = h % bits;
				array[bitpos / 8] |= (1 << (bitpos % 8));
				h += delta;
			}
		}
	}

    bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const override {
        const size_t len = bloom_filter.size();
        if (len < 2) return false;

        const char* array = bloom_filter.data();
        const size_t bits = (len - 1) * 8;

        // 这里不适用成员 k_, 而是向 array 中编码了 k
        // 这样我们可以读取使用了不同参数所生成的 bloom filters
        const size_t k = array[len - 1];
        if (k > 30)
        {
            // k > 30 是为那些 short bloom filters 的新的编码所预留的空间
            // Consider it a match.
            return true;
        }

        // 按照 Create 的逻辑算 hash value
        // 有一个不匹配则返回 false, 否则返回返回 true
        uint32_t h = BloomHash(key);
        const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
        for (size_t j = 0; j < k; j++) {
            const uint32_t bitpos = h % bits;
            if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) return false;
            h += delta;
        }
        return true;
    }

private:
	size_t bits_per_key_;
	size_t k_;	// hash 函数的个数
};
}  // namespace

const FilterPolicy*
NewBloomFilterPolicy(int bits_per_key)
{
  	return new BloomFilterPolicy(bits_per_key);
}

}  // namespace leveldb
