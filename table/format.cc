// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

void BlockHandle::EncodeTo(std::string* dst) const {
    // Sanity check that all fields have been set
    assert(offset_ != ~static_cast<uint64_t>(0));
    assert(size_ != ~static_cast<uint64_t>(0));
    PutVarint64(dst, offset_);
    PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
    if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
        return Status::OK();
    } else {
        return Status::Corruption("bad block handle");
    }
}

void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size();
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

// Decode footer part
// 如果没有出错则会 set 上 metaindex_hanle 和 index_handle. 并将 imput 指向文件末尾
Status Footer::DecodeFrom(Slice* input)
{
	// Magic Num part
	const char* magic_ptr = input->data() + kEncodedLength - 8;
	const uint32_t magic_lo = DecodeFixed32(magic_ptr);
	const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
	const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
							(static_cast<uint64_t>(magic_lo)));
	if (magic != kTableMagicNumber) {
		return Status::Corruption("not an sstable (bad magic number)");
	}

	Status result = metaindex_handle_.DecodeFrom(input);
	if (result.ok()) {
		result = index_handle_.DecodeFrom(input);
	}
	if (result.ok()) {
		// We skip over any leftover data (just padding for now) in "input"
		const char* end = magic_ptr + 8;
		*input = Slice(end, input->data() + input->size() - end);	// why
	}
	return result;
}

/**
 * Read the block identified by "handle" from "file".
 * 如果 block contents 做过压缩也会对 block contents 做解压
 * @param file[IN] 数据源 file
 * @param options[IN]
 * @param hanlde[IN] 标识要读取的 block 的 offset 和 size
 * @param result[OUT] 成功时会将 content 通过 result 传出
 *                    对于需要 reader 自行管理的情况会 set cachable 和 heap_allocated
 * @return non-OK iff failed
 *         OK 并将结果填入 result iff sucess
 */
Status
ReadBlock(RandomAccessFile* file, const ReadOptions& options, const BlockHandle& handle,
		  BlockContents* result)
{
    result->data = Slice();
    result->cachable = false;
    result->heap_allocated = false;

    // Read the block contents 以及 per-block footer <type> <crc>
    // "table_builder.cc" 是该 stucture 的构造方, 或者说是 Writer
    // +------------+----------+
    // | block_data | uint8[n] |
    // +------------+----------+
    // | type       | uint8    |
    // +------------+----------+
    // | crc        | uint32   |
    // +------------+----------+
    size_t n = static_cast<size_t>(handle.size());  // block_data 部分
    char* buf = new char[n + kBlockTrailerSize];
    Slice contents;
    Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
    if (!s.ok()) {
        delete[] buf;
        return s;
    }
    if (contents.size() != n + kBlockTrailerSize) {
        delete[] buf;
        return Status::Corruption("truncated block read");
    }

    // Check the crc of the type and the block contents
    const char* data = contents.data();  // Pointer to where Read put the data
    if (options.verify_checksums)
    {
        const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
        const uint32_t actual = crc32c::Value(data, n + 1);
        if (actual != crc)
        {
            delete[] buf;
            s = Status::Corruption("block checksum mismatch");
            return s;
        }
    }

    switch (data[n]) // type
    {
        case kNoCompression:
            if (data != buf)
            {
                // 上述 `file` 的 implementation 所返回的指针指向了一些其他的 data
                // 即 file 的实现会管理这块数据
                // 这里就直接使用 `file` 返回的指针，假设它在 file open 期间始终是 live 的
                delete[] buf;
                result->data = Slice(data, n);
                result->heap_allocated = false;
                result->cachable = false;  // Do not double-cache
            }
            else
            {
                // file 仅仅是读了数据, 数据本身需要 reader 自己去管理
                result->data = Slice(buf, n);
                result->heap_allocated = true;  // reader need to call delete
                result->cachable = true;    // set cache
            }

            // Ok
            break;
        case kSnappyCompression:
            {
                // get uncompressed length
                size_t ulength = 0;
                if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
                    delete[] buf;
                    return Status::Corruption("corrupted compressed block contents");
                }

                // uncompress contents
                char* ubuf = new char[ulength];
                if (!port::Snappy_Uncompress(data, n, ubuf)) {
                    delete[] buf;
                    delete[] ubuf;
                    return Status::Corruption("corrupted compressed block contents");
                }

                // 解压后的数据都需要 reader 自己去管理
                delete[] buf;
                result->data = Slice(ubuf, ulength);
                result->heap_allocated = true;
                result->cachable = true;
                break;
            }
        default:
            delete[] buf;
            return Status::Corruption("bad block type");
    }

    return Status::OK();
}

}  // namespace leveldb
