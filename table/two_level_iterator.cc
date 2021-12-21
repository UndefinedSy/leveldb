// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator : public Iterator
{
public:
	TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
					void* arg, const ReadOptions& options);

	~TwoLevelIterator() override;

	void Seek(const Slice& target) override;
	void SeekToFirst() override;
	void SeekToLast() override;
	void Next() override;
	void Prev() override;

	bool Valid() const override { return data_iter_.Valid(); }

	// key() 返回的是 block data 的 iter 所指向的 key
	Slice key() const override {
		assert(Valid());
		return data_iter_.key();
	}

	// value() 返回的是 block data 的 iter 所指向的 value
	Slice value() const override {
		assert(Valid());
		return data_iter_.value();
	}

	Status status() const override
	{
		// It'd be nice if status() returned a const Status& instead of a Status
		if (!index_iter_.status().ok())
		{
			return index_iter_.status();
		}
		else if (data_iter_.iter() != nullptr && !data_iter_.status().ok())
		{
			return data_iter_.status();
		}
		else
		{
			return status_;
		}
	}

private:
	void SaveError(const Status& s) {
		if (status_.ok() && !s.ok()) status_ = s;
	}

	void SkipEmptyDataBlocksForward();
	void SkipEmptyDataBlocksBackward();
	void SetDataIterator(Iterator* data_iter);
	void InitDataBlock();

	BlockFunction block_function_;	// 对 block 的操作函数
	void* arg_;						// BlockFunction 的自定义参数
	const ReadOptions options_;		// BlockFunction 的 read options
	Status status_;
	IteratorWrapper index_iter_;	// 遍历 block 的迭代器
	IteratorWrapper data_iter_;  	// May be nullptr, 遍历 block data 的迭代器  
	// 如果 data_iter_ 为 non-null, 则 data_block_handle_ 是
	// 传递给 block_function_ 用以创建 data_iter_ 的 index_value(index_iter_ 指向的 value)
	std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function)
    , arg_(arg)
    , options_(options)
    , index_iter_(index_iter)
    , data_iter_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() = default;

// 先通过 index iter 的 Seek 找 target 所在的 block
// 再通过 block 的 data iter 找对应的位置
void
TwoLevelIterator::Seek(const Slice& target)
{
	index_iter_.Seek(target);
	InitDataBlock();
	if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
	SkipEmptyDataBlocksForward();	// 跳过 Empty Block
}

void
TwoLevelIterator::SeekToFirst() {
	index_iter_.SeekToFirst();
	InitDataBlock();
	if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
	SkipEmptyDataBlocksForward();
}

void
TwoLevelIterator::SeekToLast() {
	index_iter_.SeekToLast();
	InitDataBlock();
	if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
	SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
	assert(Valid());
	data_iter_.Next();
	SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
	assert(Valid());
	data_iter_.Prev();
	SkipEmptyDataBlocksBackward();
}

void
TwoLevelIterator::SkipEmptyDataBlocksForward()
{
	// 当前 data_iter_ 为 nullptr 或者 invalid 了
	while (data_iter_.iter() == nullptr
		   || !data_iter_.Valid())
	{
		// check index iter
		if (!index_iter_.Valid())
		{
			SetDataIterator(nullptr);
			return;
		}

		// 尝试移到下一个 block, 并将 data_iter 指向下一个 block 的 first key
		index_iter_.Next();
		InitDataBlock();
		if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
	}
}

void
TwoLevelIterator::SkipEmptyDataBlocksBackward()
{
	// 当前 data_iter_ 为 nullptr 或者 invalid 了
	while (data_iter_.iter() == nullptr
		   || !data_iter_.Valid())
	{
		// check index iter
		if (!index_iter_.Valid()) {
			SetDataIterator(nullptr);
			return;
		}

		
		// Move to next block
		// 尝试移到前一个 block, 并将 data_iter 指向前一个 block 的 last key
		index_iter_.Prev();
		InitDataBlock();
		if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
	}
}

// 记录当前 data_iter_ 状态, 然后更新 data_iter_
void
TwoLevelIterator::SetDataIterator(Iterator* data_iter)
{
	if (data_iter_.iter() != nullptr)
		SaveError(data_iter_.status());

	data_iter_.Set(data_iter);
}

// 尝试用当前 index iter 所指向的 value 构建 data iter
void
TwoLevelIterator::InitDataBlock()
{
	if (!index_iter_.Valid())
	{
		SetDataIterator(nullptr);
	}
	else
	{
		Slice handle = index_iter_.value();
		if (data_iter_.iter() != nullptr
			&& handle.compare(data_block_handle_) == 0)
		{
			// 这里表示 data_iter_ 之前已经构造过
			// 且构造 data_iter_ 的就是这个 index value
		}
		else
		{
			Iterator* iter = (*block_function_)(arg_, options_, handle);
			data_block_handle_.assign(handle.data(), handle.size());
			SetDataIterator(iter);
		}
	}
}

}  // namespace

Iterator*
NewTwoLevelIterator(Iterator* index_iter,
                    BlockFunction block_function, void* arg,
                    const ReadOptions& options)
{
    return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
