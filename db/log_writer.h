// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_WRITER_H_
#define STORAGE_LEVELDB_DB_LOG_WRITER_H_

#include <cstdint>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class WritableFile;

namespace log {

class Writer {
public:
	// Create a writer that will append data to "*dest".
	// "*dest" must be initially empty.
	// "*dest" must remain live while this Writer is in use.
	explicit Writer(WritableFile* dest);

	// Create a writer that will append data to "*dest".
	// "*dest" must have initial length "dest_length".
	// "*dest" must remain live while this Writer is in use.
	Writer(WritableFile* dest, uint64_t dest_length);

	Writer(const Writer&) = delete;
	Writer& operator=(const Writer&) = delete;

	~Writer();

	Status AddRecord(const Slice& slice);

private:
    Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

    WritableFile* dest_;
    int block_offset_;  // Current offset in block

    // crc32c values for all supported record types.
	// Pre-computed 以减少计算存储在 header 中 record type 的 crc 的开销
    uint32_t type_crc_[kMaxRecordType + 1];
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_WRITER_H_
