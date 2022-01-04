// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

/**
 * 根据 *iter 的 contents 建立一个 Table file, 生成的 file 将根据 meta->number 来命名
 * 如果 *iter 中没有数据, 则 meta->file_size 将被置零，并且不会生成 table file
 * 如果出错，会保证清理掉生成的 Table file (如果生成了)
 * @param dbname[IN]
 * @param env[IN]
 * @param options[IN]
 * @param table_cache[IN]
 * @param iter[IN] 将根据 iter 的 contents 生成 table file
 * @param meta[IN] meta->number 与待生成文件的名字相关
 *            [OUT] 若成功, 则 meta 的其余部分填入关于生成的 table file 的元数据
 */
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta)
{
	Status s;
	meta->file_size = 0;
	iter->SeekToFirst();

  	std::string fname = TableFileName(dbname, meta->number); // <dbname>/<number>.ldb
	if (iter->Valid())
	{
		WritableFile* file;
		s = env->NewWritableFile(fname, &file);
		if (!s.ok()) return s;

		TableBuilder* builder = new TableBuilder(options, file);
		meta->smallest.DecodeFrom(iter->key());	// first key
		Slice key;
		// 遍历 iter, 将其 contents buffer 到 Table Builder
		for (; iter->Valid(); iter->Next())
		{
			key = iter->key();
			builder->Add(key, iter->value());
		}

		if (!key.empty()) meta->largest.DecodeFrom(key); // last key

		// Finish and check for builder errors
		s = builder->Finish();
		if (s.ok())
		{
			meta->file_size = builder->FileSize();
			assert(meta->file_size > 0);
		}
		delete builder;

		// Flush file and check for file errors
		if (s.ok()) s = file->Sync();
		if (s.ok()) s = file->Close();
		delete file;
		file = nullptr;

		if (s.ok())
		{
			// Verify that the table is usable
			// 生成 Table file 后验证 table 可用
			Iterator* it = table_cache->NewIterator(ReadOptions(),
													meta->number,
													meta->file_size);
			s = it->status();
			delete it;
		}
	}

	// Check for input iterator errors
	if (!iter->status().ok()) s = iter->status();

	if (s.ok() && meta->file_size > 0)
	{
		// Keep it
	}
	else
	{
		env->RemoveFile(fname);
	}
	return s;
}

}  // namespace leveldb
