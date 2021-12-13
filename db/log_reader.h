// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_READER_H_
#define STORAGE_LEVELDB_DB_LOG_READER_H_

#include <cstdint>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class SequentialFile;

namespace log {

class Reader {
public:
	// Interface for reporting errors.
	class Reporter {
	public:
		virtual ~Reporter();

		// Some corruption was detected.
		// @param “size” 是一个大约的由于 Corruption 而丢失的字节数
		virtual void Corruption(size_t bytes, const Status& status) = 0;
	};

	/**
	 * Create a reader that will return log records from "*file".
	 * @param file, log records 的数据源; file 应在 reader 可能被使用时保持 live
	 * @param reporter, 如果 non-null, 则每当检测到 corruption 而丢弃数据时, reporter 会被通知
	 * 					reader 可能被使用时, reporter 必须保持 live
	 * @param checksum, 若为 true, 则会校验 checksums
	 * @param initial_offset, Reader 从文件中物理位置 >= initial_offset 的第一条 record开始读取
	 */
	Reader(SequentialFile* file, Reporter* reporter, bool checksum,
		   uint64_t initial_offset);

	Reader(const Reader&) = delete;
	Reader& operator=(const Reader&) = delete;

	~Reader();

	// Read the next record into *record.  Returns true if read
	// successfully, false if we hit end of the input.  May use
	// "*scratch" as temporary storage.  The contents filled in *record
	// will only be valid until the next mutating operation on this
	// reader or the next mutation to *scratch.
	/**
	 * Read the next record into *record.
	 * @param scratch, 可以使用 scratch 作为临时存储
	 * @param record, The contents filled in *record will only be valid until the next mutating operation on this reader or the next mutation to *scratch.
					  填写在 *record 中的内容仅在对该 reader 的下一次变异操作或对 *scratch 的下一次变异之前有效。
	 * @return true if read successfully
	 * 		   false if hit end of the input
	 */ 
	bool ReadRecord(Slice* record, std::string* scratch);

	// 获取 ReadRecord 所返回的 last record 的物理位置 offset
	// 在没有调用过 ReadRecord 前调用该方法的行为是未定义的
	uint64_t LastRecordOffset();

private:
	// Extend record types with the following special values
	enum {
		kEof = kMaxRecordType + 1,
		// Returned whenever we find an invalid physical record.
		// Currently there are three situations in which this happens:
		// * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
		// * The record is a 0-length record (No drop is reported)
		// * The record is below constructor's initial_offset (No drop is reported)
		kBadRecord = kMaxRecordType + 2
	};

	// Skips all blocks that are completely before "initial_offset_".
	//
	// Returns true on success. Handles reporting.
	bool SkipToInitialBlock();

	// Return type, or one of the preceding special values
	unsigned int ReadPhysicalRecord(Slice* result);

	// Reports dropped bytes to the reporter.
	// buffer_ must be updated to remove the dropped bytes prior to invocation.
	void ReportCorruption(uint64_t bytes, const char* reason);
	void ReportDrop(uint64_t bytes, const Status& reason);

	SequentialFile* const file_;
	Reporter* const reporter_;
	bool const checksum_;
	char* const backing_store_;
	Slice buffer_;
	bool eof_;  // Last Read() indicated EOF by returning < kBlockSize

	// Offset of the last record returned by ReadRecord.
	uint64_t last_record_offset_;
	// Offset of the first location past the end of buffer_.
	uint64_t end_of_buffer_offset_;

	// Offset at which to start looking for the first record to return
	uint64_t const initial_offset_;

	// True if we are resynchronizing after a seek (initial_offset_ > 0). In
	// particular, a run of kMiddleType and kLastType records can be silently
	// skipped in this mode
	bool resyncing_;
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_
