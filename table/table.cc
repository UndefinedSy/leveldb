// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
    ~Rep() {
        delete filter;
        delete[] filter_data;
        delete index_block;
    }

    Options options;
    Status status;
    RandomAccessFile* file;
    uint64_t cache_id;	// BlockCache for this table
    FilterBlockReader* filter;	// FilterBlock 的 Reader
    const char* filter_data;    // 非 nullptr 则表示需要调用者 handle delete[]

    BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
    Block* index_block;
};

Status
Table::Open(const Options& options, RandomAccessFile* file, uint64_t size,
            Table** table)
{
    *table = nullptr;
    if (size < Footer::kEncodedLength)
        return Status::Corruption("file is too short to be an sstable");

    // Read footer part
    char footer_space[Footer::kEncodedLength];
    Slice footer_input;
    Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                          &footer_input, footer_space);
    if (!s.ok()) return s;

    // Parse footer part
    Footer footer;
    s = footer.DecodeFrom(&footer_input);
    if (!s.ok()) return s;

    // Read the index block
    BlockContents index_block_contents;
    ReadOptions opt;
    if (options.paranoid_checks)
        opt.verify_checksums = true;
    s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

    if (s.ok())
    {
        // We've successfully read the footer and the index block:
        // we're ready to serve requests.
        Block* index_block = new Block(index_block_contents);
        Rep* rep = new Table::Rep;
        rep->options = options;
        rep->file = file;
        rep->metaindex_handle = footer.metaindex_handle();
        rep->index_block = index_block;
		// 如果 option 中设置了 block cache, 则还需要为该 table 创建 cache
        rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
        rep->filter_data = nullptr;
        rep->filter = nullptr;
        *table = new Table(rep);
        (*table)->ReadMeta(footer);
    }

    return s;
}

// 尝试读出 filter_policy 所指向的 filter 的 value 部分
void
Table::ReadMeta(const Footer& footer)
{
    if (rep_->options.filter_policy == nullptr)
        return;  // Do not need any metadata

    // TODO(sanjay):
    // Skip this if footer.metaindex_handle() size indicates it is an empty block.
    ReadOptions opt;
    if (rep_->options.paranoid_checks)
        opt.verify_checksums = true;

    // read meta contents (filter & stats)
    BlockContents contents;
    if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
        // Do not propagate errors since meta info is not needed for operation
        return;
    }
    Block* meta = new Block(contents);

    // 读取对应 filter.xxx 的 value 部分
    Iterator* iter = meta->NewIterator(BytewiseComparator());
    std::string key = "filter.";
    key.append(rep_->options.filter_policy->Name());
    iter->Seek(key);
    if (iter->Valid() && iter->key() == Slice(key)) {
        ReadFilter(iter->value());
    }
    delete iter;
    delete meta;
}

/**
 * 
 * @param filter_handle_value, value part of the specified filter.xxx name
 */
void
Table::ReadFilter(const Slice& filter_handle_value)
{
    // 通过 filter_handle_value 解析 filter block 的  BlockHandle
    Slice v = filter_handle_value;
    BlockHandle filter_handle;
    if (!filter_handle.DecodeFrom(&v).ok()) {
        return;
    }

    // We might want to unify with ReadBlock() if we start
    // requiring checksum verification in Table::Open.
    // 如果要求在 Table::Open 中进行 checksum 校验，可能会想和 ReadBlock() 统一起来
    ReadOptions opt;
    if (rep_->options.paranoid_checks) {
        opt.verify_checksums = true;
    }

    BlockContents block;
    if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
        return;
    }
    if (block.heap_allocated) {
        rep_->filter_data = block.data.data();  // Will need to delete later
    }
    rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

// Iterator 的 Cleanup Func, 用于清理没有 block cache 情况下的 block
static void
DeleteBlock(void* arg, void* ignored)
{
  	delete reinterpret_cast<Block*>(arg);
}

// block cache 中的 deleter
static void
DeleteCachedBlock(const Slice& key, void* value)
{
	Block* block = reinterpret_cast<Block*>(value);
	delete block;
}

/**
 * Iterator 的Cleanup Func, 用于清理有 block cache 情况下的 cache handle
 * @param arg, block_cache
 * @param h, cache handle
 */
static void
ReleaseBlock(void* arg, void* h)
{
	Cache* cache = reinterpret_cast<Cache*>(arg);
	Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
	cache->Release(handle);
}

/**
 * 将一个  index iter value（即一个 encoded BlockHandle）转换成一个对应 block 的 iterator
 * @param arg[IN], const_cast<Table*>(this)
 * @param options[IN], Table::NewIterator 传入的 ReadOptions
 * @param index_value[IN], index iter 所指向的 kv pair 的 value
 */
Iterator*
Table::BlockReader(void* arg, const ReadOptions& options,
				   const Slice& index_value)
{
	Table* table = reinterpret_cast<Table*>(arg);
	Cache* block_cache = table->rep_->options.block_cache;
	Block* block = nullptr;					// 目标 block 的 reader
	Cache::Handle* cache_handle = nullptr;	// block cache 中 index value 所对应的 block

	// handle 指向了 index value 所索引的 data block 的 offset 和 size
	BlockHandle handle;
	Slice input = index_value;
	Status s = handle.DecodeFrom(&input);
	// 我们有意允许在 index_value 中添加额外的信息，以便在未来可以添加更多的功能

	// 根据 index value 找到目标 block
	if (s.ok())
	{
		BlockContents contents;
		if (block_cache != nullptr) // 有 block cache
		{
			// cache key 为 <cache_id>[8] + <block_offset>[8]
			char cache_key_buffer[16];
			EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
			EncodeFixed64(cache_key_buffer + 8, handle.offset());

			// 尝试在 block cache 中找对应的 block
			Slice key(cache_key_buffer, sizeof(cache_key_buffer));
			cache_handle = block_cache->Lookup(key);
			if (cache_handle != nullptr) // Found
			{
				block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
			}
			else // Missed
			{
				s = ReadBlock(table->rep_->file, options, handle, &contents);
				if (s.ok())
				{
					block = new Block(contents);
					// 更新 block cache
					if (contents.cachable && options.fill_cache)
					{
						cache_handle = block_cache->Insert(key, block, block->size(),
														   &DeleteCachedBlock);
					}
				}
			}
		}
		else // 没有 block cache
		{
			s = ReadBlock(table->rep_->file, options, handle, &contents);
			if (s.ok())
			{
				block = new Block(contents);
			}
		}
	}

	Iterator* iter;
	if (block != nullptr)
	{
		iter = block->NewIterator(table->rep_->options.comparator);
		// 注册该 iterator 的 cleanup function
		if (cache_handle == nullptr)	// 没有 block cache
		{
			iter->RegisterCleanup(&DeleteBlock, block, nullptr);
		}
		else	// 有 block cache
		{
			iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
		}
	}
	else // Failed to get the corresponding block
	{
		iter = NewErrorIterator(s);
	}
	return iter;
}

Iterator*
Table::NewIterator(const ReadOptions& options) const
{
    return NewTwoLevelIterator(
        /*Iterator*/ rep_->index_block->NewIterator(rep_->options.comparator),
        /*BlockFunction*/ &Table::BlockReader,
		/*BlockFunc arg*/ const_cast<Table*>(this),
		/*ReadOptions*/ options);
}

/**
 * 在调用 Seek(key) 之后对找到的 entry 调用 (*handle_result)(arg, ...)
 * 如果 filter policy 显示 key 不存在则不能进行这样的调用
 * Used by TableCache
 * @param k, key to get
 * @param arg, handle_result func 所使用的参数
 * @param handle_result, 在 data block 中 seek 到了 key 后对 kv pair 使用的回调函数
 * @return 返回过程中是否存在错误
 */
Status
Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                   void (*handle_result)(void*, const Slice&, const Slice&))
{
    Status s;

    // 通过 index 查找 key 所在的 data block
    Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
    iiter->Seek(k);

    if (iiter->Valid())
    {
        Slice handle_value = iiter->value();
        FilterBlockReader* filter = rep_->filter;
        BlockHandle handle;

        // 如果有 filter 则通过 filter 快速查找 key 是否可能存在(可能 false positive)
        if (filter != nullptr
            && handle.DecodeFrom(&handle_value).ok()
            && !filter->KeyMayMatch(handle.offset(), k))
        {
            // Not found
        }
        else // 可能存在
        {
            // 定位到 block 中第一个 >= k 的位置
            Iterator* block_iter = BlockReader(this, options, iiter->value());
            block_iter->Seek(k);
            
            // 若 block_iter 有效则对该 key-value 调用传入的 handler func
            if (block_iter->Valid())
                (*handle_result)(arg, block_iter->key(), block_iter->value());
            
            s = block_iter->status();
            delete block_iter;
        }
    }

    if (s.ok())
    {
        s = iiter->status();
    }
    delete iiter;
    return s;
}

uint64_t
Table::ApproximateOffsetOf(const Slice& key) const
{
    // Seek input key 所在的 block
    Iterator* index_iter = rep_->index_block->NewIterator(rep_->options.comparator);
    index_iter->Seek(key);

    uint64_t result;
    if (index_iter->Valid())
    {
        BlockHandle handle;
        Slice input = index_iter->value();
        Status s = handle.DecodeFrom(&input);
        if (s.ok()) // 通过 index 找到 `key` 所在的 block, 返回该 block 的 offset
        {
            result = handle.offset();
        }
        else
        {
            // 异常场景: 找到了 key 所在 block, 但无法 Decode 该 block handle
            // 这种情况也返回 metaindex block 的 offset
            result = rep_->metaindex_handle.offset();
        }
    }
    else // 不在 index range 中
    {
        // `key` 已超过当前文件的 last key
        // 这里返回 metaindex block 的 offset 作为 Approximate offset
        result = rep_->metaindex_handle.offset();
    }
    delete index_iter;
    return result;
}

}  // namespace leveldb
