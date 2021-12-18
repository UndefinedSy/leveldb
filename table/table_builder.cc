// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
    Rep(const Options& opt, WritableFile* f)
        : options(opt)
        , index_block_options(opt)
        , file(f)
        , offset(0)
        , data_block(&options)
        , index_block(&index_block_options)
        , num_entries(0)
        , closed(false)
        , filter_block(opt.filter_policy == nullptr
                       ? nullptr
                       : new FilterBlockBuilder(opt.filter_policy))
        , pending_index_entry(false)
    {
        index_block_options.block_restart_interval = 1;
    }

    Options options;            // options for black block
    Options index_block_options;// options for index block
    WritableFile* file;         // sstable file
    uint64_t offset;            // 下一个要写入 sst file 的 data block 的 offset
    Status status;
    BlockBuilder data_block;    // 当前操作的 data block
    BlockBuilder index_block;
    std::string last_key;
    int64_t num_entries;
    bool closed;                // Either Finish() or Abandon() has been called.
    FilterBlockBuilder* filter_block;

    // 在尝试 Add 一个新 data block 的 first key 时才 emit 上一个 data block 的 index block
    // 这样的行为方式是为了可以在 index block 中使用 shorter key
    // 
    // 例如，一个 block boundary 位于 key: "the quick brown fox" 和 "the who" 之间,
    // 则可以用 "the r" 作为这个 index block 的 key，因为 "the r" >= 上一个 block 中的所有 entries
    // 并且 < 后继 blocks 中的所有条目
    // 
    // Invariant: r->pending_index_entry is true only if data_block is empty.
    bool pending_index_entry;
    // Handle to add to index block
    // 在对一个新 data block 做 Add() 时 dump, 在 Flush() 时更新
    // 每次更新后 set_offset 为 flush block 的起始 offset, set_size 为 block_content size
    BlockHandle pending_handle;

    std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file))
{
    if (rep_->filter_block != nullptr) {
        rep_->filter_block->StartBlock(0);
    }
}

TableBuilder::~TableBuilder() {
    assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
    delete rep_->filter_block;
    delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options)
{
    // Note: 如果有更多的字段被添加到 Options 中, 应更新这个函数的实现
    // 以捕捉那些不应被允许在 building a Table 的过程中改变的 options
    if (options.comparator != rep_->options.comparator) {
        return Status::InvalidArgument("changing comparator while building table");
    }

    // 需要注意，任何 live BlockBuilders 都指向 rep_->options，因此会自动使用更新后的 options
    rep_->options = options;
    rep_->index_block_options = options;
    rep_->index_block_options.block_restart_interval = 1;
    return Status::OK();
}

// 当尝试向 Table Builder 中添加一条 KV 时会进行如下操作:
// - 如果是 data block 发生了 rotate 则算出 shortest separator 并写 index block
// - 如果配置了 filter 则对 input key 做 filter
// - 将 kv 写到一个 buffer, batch size 到一个 block 时 Flush()
void TableBuilder::Add(const Slice& key, const Slice& value) {
    // Pre-check
    Rep* r = rep_;
    assert(!r->closed);
    if (!ok()) return;
    if (r->num_entries > 0) {
        assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
    }

    if (r->pending_index_entry) {
        assert(r->data_block.empty());
        // [last_key, key) 之间找一个 last_key 的 shortest 后继存入到 last_key
        r->options.comparator->FindShortestSeparator(&r->last_key, key);
        std::string handle_encoding;
        r->pending_handle.EncodeTo(&handle_encoding);
        r->index_block.Add(r->last_key, Slice(handle_encoding));
        r->pending_index_entry = false;
    }

    // 配置有 filter 则使用, 可插拔
    if (r->filter_block != nullptr) {
        r->filter_block->AddKey(key);
    }

    r->last_key.assign(key.data(), key.size());
    r->num_entries++;
    r->data_block.Add(key, value);

    const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
    // buffered size 超过一个 block size 才实际去 flush, batch write
    if (estimated_block_size >= r->options.block_size) {
        Flush();
    }
}


void TableBuilder::Flush() {
    // Pre-check
    Rep* r = rep_;
    assert(!r->closed);
    if (!ok()) return;
    if (r->data_block.empty()) return;
    assert(!r->pending_index_entry);

    WriteBlock(&r->data_block, &r->pending_handle);
    if (ok()) {
        r->pending_index_entry = true;
        r->status = r->file->Flush();
    }

    if (r->filter_block != nullptr) {
        r->filter_block->StartBlock(r->offset);
    }
}

// 
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
    // File format contains a sequence of blocks where each block has:
    //    block_data: uint8[n]
    //    type: uint8
    //    crc: uint32
    // File 包含了一连串的 blocks, 每个 block 的构成为:
    // ```
    // +------------+----------+
    // | block_data | uint8[n] |
    // +------------+----------+
    // | type       | uint8    |
    // +------------+----------+
    // | crc        | uint32   |
    // +------------+----------+
    // ```
    assert(ok());
    Rep* r = rep_;

    // 结束一个 block 的 build, 向尾部添加 trailer
    Slice raw = block->Finish();

    // 根据 options 对 block raw data 进行压缩
    Slice block_contents;
    CompressionType type = r->options.compression;
    // TODO(postrelease): Support more compression options: zlib?
    switch (type) {
        case kNoCompression:
            block_contents = raw;
            break;

        case kSnappyCompression: {
            std::string* compressed = &r->compressed_output;
            if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
                compressed->size() < raw.size() - (raw.size() / 8u)) {
                block_contents = *compressed;
            } else {
                // Snappy not supported, or compressed less than 12.5%, so just
                // store uncompressed form
                block_contents = raw;
                type = kNoCompression;
            }
            break;
        }
    }

    WriteRawBlock(block_contents, type, handle);
    r->compressed_output.clear();
    block->Reset();
}

/**
 * 将处理完成的 blcok contents, 所使用的 compression type 以及 checksum 写入到 sst file
 * @param block_contents, 
 * @param type, 该 block 的压缩方法类型
 * @param handle, pending handle
 */
void
TableBuilder::WriteRawBlock(const Slice& block_contents,
                            CompressionType type, BlockHandle* handle)
{
    Rep* r = rep_;

    handle->set_offset(r->offset);
    handle->set_size(block_contents.size());

    // 这里是 append 只是写到 buffer, 并没有实际 flush
    r->status = r->file->Append(block_contents);
    if (r->status.ok())
    {
        char trailer[kBlockTrailerSize];    // 1<type> + 4<crc32>
        trailer[0] = type;
        // 每个 block 的 checksum 是对 block contents + type 的
        uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
        crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
        EncodeFixed32(trailer + 1, crc32c::Mask(crc));

        r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
        if (r->status.ok()) {
            r->offset += block_contents.size() + kBlockTrailerSize;
        }
    }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish()
{
    Rep* r = rep_;
    Flush();    // write the last data block
    assert(!r->closed);
    r->closed = true;

    BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

    // Write meta filter block
    if (ok() && r->filter_block != nullptr)
        WriteRawBlock(r->filter_block->Finish(), kNoCompression, &filter_block_handle);

    // Write meta index block
    if (ok())
    {
        BlockBuilder meta_index_block(&r->options);
        if (r->filter_block != nullptr)
        {
            // 格式为 "filter.<Name>" key 到 filter data 的物理物质的映射
            std::string key = "filter.";
            key.append(r->options.filter_policy->Name());
            std::string handle_encoding;
            filter_block_handle.EncodeTo(&handle_encoding);
            meta_index_block.Add(key, handle_encoding);
        }

        // TODO(postrelease): Add stats and other meta blocks
        WriteBlock(&meta_index_block, &metaindex_block_handle);
    }

    // Write index block
    if (ok())
    {
        // Finish 会先尝试 flush 一把, 如果 Finish() 时最后一个 data block 且为出错
        // 为最后一个 block 生成一个 end key 加入到 index
        if (r->pending_index_entry)
        {
            r->options.comparator->FindShortSuccessor(&r->last_key);
            std::string handle_encoding;
            r->pending_handle.EncodeTo(&handle_encoding);
            r->index_block.Add(r->last_key, Slice(handle_encoding));
            r->pending_index_entry = false;
        }
        WriteBlock(&r->index_block, &index_block_handle);
    }

    // Write footer
    if (ok())
    {
        Footer footer;
        footer.set_metaindex_handle(metaindex_block_handle);
        footer.set_index_handle(index_block_handle);
        std::string footer_encoding;
        footer.EncodeTo(&footer_encoding);
        r->status = r->file->Append(footer_encoding);
        if (r->status.ok()) {
            r->offset += footer_encoding.size();
        }
    }

    return r->status;
}

// 只是把 closed 置为 true
void TableBuilder::Abandon() {
    Rep* r = rep_;
    assert(!r->closed);
    r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
