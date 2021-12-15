// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <cstdio>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() = default;

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {}

Reader::~Reader() { delete[] backing_store_; }

bool
Reader::SkipToInitialBlock()
{
    // offset_in_block 表示需要跳过的 offset 在最后一个块的位置
    const size_t offset_in_block = initial_offset_ % kBlockSize;
    // block_start_location 表示到第一个 required block 的 offset
    uint64_t block_start_location = initial_offset_ - offset_in_block;

    // Don't search a block if we'd be in the trailer
    // 不足 7B, trailer 中都是 pedding 0, 因此也跳过该 block
    if (offset_in_block > kBlockSize - 6)
        block_start_location += kBlockSize;

    end_of_buffer_offset_ = block_start_location;

    // Skip to start of first block that can contain the initial record
    if (block_start_location > 0)
    {
        Status skip_status = file_->Skip(block_start_location);
        if (!skip_status.ok()) {
            ReportDrop(block_start_location, skip_status);
            return false;
        }
    }

    return true;
}

bool
Reader::ReadRecord(Slice* record, std::string* scratch)
{
    // 当前的 read offset < initial offset, Do Seek.
    if (last_record_offset_ < initial_offset_)
    {
        if (!SkipToInitialBlock())
            return false;
    }

    scratch->clear();
    record->clear();
    bool in_fragmented_record = false;  // 是否遇到了 FIRST record

    // 用于记录我们正在读取的 logical record 的 offset
    // 0 is a dummy value to make compilers happy
    uint64_t prospective_record_offset = 0;

    Slice fragment;

    // repeat until meet:
    // * KLastType record
    // * KFullType record
    // * EOF
    while (true)
    {
        const unsigned int record_type = ReadPhysicalRecord(&fragment);

        // ReadPhysicalRecord may have only had an empty trailer remaining in its
        // internal buffer. Calculate the offset of the next physical record now
        // that it has returned, properly accounting for its header size.
        // ReadPhysicalRecord 的 internal buffer 中可能只有一个 empty trailer
        // 计算下一个 physical record 的 offset，正确考虑其标头大小。
        // 看起来是当前读出来的 entry 的 start offset
        uint64_t physical_record_offset =
            end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();

        // 当设置了 initial_offset_ 时, 如果从 initial_offset_ 开始读的 record
        // 是一个大的 log entry 的 MIDDLE / LAST, 则直接丢掉
        if (resyncing_)
        {
            if (record_type == kMiddleType)
            {
                continue;
            }
            else if (record_type == kLastType)
            {
                resyncing_ = false;
                continue;
            }
            else    // FIRST or FULL Type
            {
                resyncing_ = false;
            }
        }

        switch (record_type)
        {
            case kFullType:
                if (in_fragmented_record)
                {
                    // 负责 Handle log::Writer 早期版本中的 bug
                    // 该 bug 中 write 可能在一个 block 的 tail 发起一条 empty kFirstType record
                    // 然后在下一个 block 的开头发起一条 kFullType 或 kFirstType record
                    if (!scratch->empty()) {
                        ReportCorruption(scratch->size(), "partial record without end(1)");
                    }
                }
                prospective_record_offset = physical_record_offset;
                scratch->clear();
                *record = fragment;
                last_record_offset_ = prospective_record_offset;
                return true;

            case kFirstType:
                if (in_fragmented_record)
                {
                    // 负责 Handle log::Writer 早期版本中的 bug
                    // 该 bug 中 write 可能在一个 block 的 tail 发起一条 empty kFirstType record
                    // 然后在下一个 block 的开头发起一条 kFullType 或 kFirstType record
                    if (!scratch->empty()) {
                        ReportCorruption(scratch->size(), "partial record without end(2)");
                    }
                }
                prospective_record_offset = physical_record_offset;
                scratch->assign(fragment.data(), fragment.size());
                in_fragmented_record = true;
                break;

            case kMiddleType:
                if (!in_fragmented_record)
                {
                    ReportCorruption(fragment.size(),
                                    "missing start of fragmented record(1)");
                }
                else
                {
                    scratch->append(fragment.data(), fragment.size());
                }
                break;

            case kLastType:
                if (!in_fragmented_record) {
                    ReportCorruption(fragment.size(),
                                    "missing start of fragmented record(2)");
                }
                else
                {
                    scratch->append(fragment.data(), fragment.size());
                    *record = Slice(*scratch);
                    last_record_offset_ = prospective_record_offset;
                    return true;
                }
                break;

            case kEof:
                if (in_fragmented_record) {
                    // This can be caused by the writer dying immediately after
                    // writing a physical record but before completing the next; don't
                    // treat it as a corruption, just ignore the entire logical record.
                    scratch->clear();
                }
                return false;   // EOF 会返回

            // BadRecord 和 unknown type 都只是报错但是不会返回, 而是继续尝试读找下一个 record
            case kBadRecord:
                if (in_fragmented_record) {
                    ReportCorruption(scratch->size(), "error in middle of record");
                    in_fragmented_record = false;
                    scratch->clear();
                }
                break;

            default: {
                char buf[40];
                std::snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
                ReportCorruption(
                    (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
                    buf);
                in_fragmented_record = false;
                scratch->clear();
                break;
            }
        }
    }
    return false;
}

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

// 当 buffer 中的数据不足时会一次尝试读取一个 block 到 buffer
// 并尝试逐个 log record 的去消费，并通过 result 传出一个 log record
// 如果成功拿到一个 log record 则返回这个 log record 的 type(FULL/FIRST/MIDDLE/LAST)
// 如果失败则返回错误
unsigned int
Reader::ReadPhysicalRecord(Slice* result)
{
    while (true)
    {
        // 如果 buffer 中剩余的数据不足一个 Header
        if (buffer_.size() < kHeaderSize)
        {
            // 还没到当前 log file 的 EOF, 丢掉剩下的 trailer，再读一个 block
            if (!eof_)
            {
                // Last read was a full read, so this is a trailer to skip
                buffer_.clear();
                // 尝试读一个 block
                Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
                end_of_buffer_offset_ += buffer_.size();
                if (!status.ok())
                {
                    buffer_.clear();
                    ReportDrop(kBlockSize, status);
                    eof_ = true;
                    return kEof;
                }
                // 如果得到的 size 小于一个 Block size，则意味着本次 Read 遇到了 EOF
                else if (buffer_.size() < kBlockSize)
                {
                    eof_ = true;
                }
                continue;
            }
            else
            {
                // 当出现 EOF 的时候，需要注意的是如果 buffer_ 是非空的
                // 这意味着我们在一个文件的 EOF 处有一个 truncated header
                // 这可能是由于 writer 在写入 header 的过程中 crash 导致的
                // 这里不将这种情况作为一个 Error，仅仅 report 一个 EOF
                buffer_.clear();
                return kEof;
            }
        }

        // record :=
        //     checksum: uint32     // crc32c of type and data[] ; little-endian
        //     length: uint16       // little-endian
        //     type: uint8          // One of FULL, FIRST, MIDDLE, LAST
        //     data: uint8[length]

        // Parse the header
        const char* header = buffer_.data();
        const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
        const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
        const unsigned int type = header[6];
        const uint32_t length = a | (b << 8);

        // header + log_entry's length 超过的当前 buffer 剩余数据
        if (kHeaderSize + length > buffer_.size())
        {
            size_t drop_size = buffer_.size();
            buffer_.clear();

            // 如果当前不是 EOF，意味着(TODO ...)
            if (!eof_)
            {
                ReportCorruption(drop_size, "bad record length");
                return kBadRecord;
            }

            // 如果在还没有读完 |length| bytes 的 Payload 时就遇到了 EOF
            // 则可能是 writer 在写一个 record 的过程中挂掉
            // 这里不会 report corruption
            return kEof;
        }

        if (type == kZeroType && length == 0)
        {
            // Skip zero length record without reporting any drops since
            // such records are produced by the mmap based writing code in
            // env_posix.cc that preallocates file regions.
            buffer_.clear();
            return kBadRecord;
        }

        // Check crc
        if (checksum_)
        {
            uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
            uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
            if (actual_crc != expected_crc)
            {
                // 当 checksum 校验失败时，直接丢掉这个 buffer 的其余部分
                // 因为 length 字段本身可能已经损坏
                // 如果我们选择相信这个 length, 则可能会读到一些 log record 的片段
                // 而这些片段可能恰好看起来像一个有效的 log record, 但实际可能是错位的
                size_t drop_size = buffer_.size();
                buffer_.clear();
                ReportCorruption(drop_size, "checksum mismatch");
                return kBadRecord;
            }
        }

        // 消费掉 buffer 中的一个 log entry
        buffer_.remove_prefix(kHeaderSize + length);

        // 如果这个 log record 的开始位置在 initial_offset_ 之前，则 skip 掉
        if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length
                < initial_offset_)
        {
            result->clear();
            return kBadRecord;
        }

        *result = Slice(header + kHeaderSize, length);
        return type;
    }
}

}  // namespace log
}  // namespace leveldb
