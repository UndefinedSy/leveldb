// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_FORMAT_H_
#define STORAGE_LEVELDB_TABLE_FORMAT_H_

#include <cstdint>
#include <string>

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/table_builder.h"

namespace leveldb {

class Block;
class RandomAccessFile;
struct ReadOptions;

// BlockHandle 可以理解为一个编解码器, 它是一个指向 data block / meta block 中数据的指针
class BlockHandle {
public:
    // Maximum encoding length of a BlockHandle
    enum { kMaxEncodedLength = 10 + 10 };

    BlockHandle();

    // The offset of the block in the file.
    uint64_t offset() const { return offset_; }
    void set_offset(uint64_t offset) { offset_ = offset; }

    // The size of the stored block
    uint64_t size() const { return size_; }
    void set_size(uint64_t size) { size_ = size; }

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(Slice* input);

private:
    uint64_t offset_;
    uint64_t size_;
};

// Footer 封装了存放在每个 table 尾部的固定的信息, 即
// metaindex block 和 index block 的 BlockHandle, 以及一个 magic number。
class Footer
{
public:
    // Encoded length of a Footer.
    // of two block handles and a magic number.
    // Footer 的序列化将总是占用这么多字节, 它由两个 block handles 和一个 magic number 组成。
    enum { kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8 };

    Footer() = default;

    // The block handle for the metaindex block of the table
    const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
    void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }

    // The block handle for the index block of the table
    const BlockHandle& index_handle() const { return index_handle_; }
    void set_index_handle(const BlockHandle& h) { index_handle_ = h; }

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(Slice* input);

private:
    BlockHandle metaindex_handle_;
    BlockHandle index_handle_;
};

// kTableMagicNumber was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.
static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

// 1-byte type + 32-bit crc
static const size_t kBlockTrailerSize = 5;

struct BlockContents {
    Slice data;           // Actual contents of data
    bool cachable;        // True iff data can be cached
    bool heap_allocated;  // True iff caller should delete[] data.data()
};

// Read the block identified by "handle" from "file".
// return non-OK iff failed
// return OK 并将结果填入 result iff sucess
Status ReadBlock(RandomAccessFile* file, const ReadOptions& options, const BlockHandle& handle,
                 BlockContents* result);

// Implementation details follow.  Clients should ignore,

inline BlockHandle::BlockHandle()
    : offset_(~static_cast<uint64_t>(0)), size_(~static_cast<uint64_t>(0)) {}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FORMAT_H_
