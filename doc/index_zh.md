leveldb
=======

_Jeff Dean, Sanjay Ghemawat_

leveldb lib 提供了一个持久化的 kv 存储。keys 和 values 是任意字节的数组。keys 会根据用户指定的比较函数在 kv 存储中排序。

## Opening A Database
leveldb database 具有一个对应其文件系统目录的 name。数据库的所有数据都存储在这个目录中。

下例显示了如何打开数据库，并在必要时创建它：
```c++
#include <cassert>
#include "leveldb/db.h"

leveldb::DB* db;
leveldb::Options options;
options.create_if_missing = true;
leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
assert(status.ok());
...
```

如果期望在该 db 已经存在的情况下抛出一个 error，可以在 `leveldb::DB::Open` 中加上如下的 option:
```c++
options.error_if_exists = true;
```

## Status
那些可能出现错误的函数大多会返回一个类型为 `leveldb::Status` 的值，可以通过该结果检查执行是否成功，并可以打出相关的错误信息:
```c++
leveldb::Status s = ...;
if (!s.ok()) cerr << s.ToString() << endl;
```

## Closing A Database
当使用完一个 database 时，只需要 delete database object 即可:
```c++
... open the db as described above ...
... do something with db ...
delete db;
```

## Reads And Writes
database 提供了 Put, Delete, Get 方法以修改/查询数据库:
```c++
std::string value;
leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
if (s.ok()) s = db->Put(leveldb::WriteOptions(), key2, value);
if (s.ok()) s = db->Delete(leveldb::WriteOptions(), key1);
```

## Atomic Updates
假设上面的例子，进程在 Put key2 之后但在 Delete key1 之前宕掉，则相同的值可能会存储在多个键下。  
可以通过使用类 `WriteBatch` 来原子地应用一组的 updates：
```c++
#include "leveldb/write_batch.h"
...
std::string value;
leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
if (s.ok()) {
    leveldb::WriteBatch batch;
    batch.Delete(key1);
    batch.Put(key2, value);
    s = db->Write(leveldb::WriteOptions(), &batch);
}
```

`WriteBatch` 中包含要对数据库进行的一系列的修改，整个 batch 中的修改会按顺序被应用。

batch 除了能够提供原子性以外，还用于加速 bulk updates，因为其可以将许多的单独的修改放到一个 batch 操作中。

## Synchronous Writes
默认情况下，每个对 leveldb 的写操作都是异步的: 这里会在将写的数据从进程的用户态内存拷贝到系统内存后返回，从 kernel memory 到底层的持久化存储是异步完成的。可以通过打开 sync flag, 让写操作的调用在写的数据被实际推到存储硬件之前不会返回。（在 Posix 系统中，这通常是通过在写操作返回之前调用 `fsync(...)` 或 `fdatasync(...)` 或 `msync(..., MS_SYNC)` 实现的。
```c++
leveldb::WriteOptions write_options;
write_options.sync = true;
db->Put(write_options, ...);
```

Async writes 通常比 Sync writes 快一千倍。Async write 的缺点在于，如果出现 crash 可能会导致最近几次的更新数据丢失。
> 需要注意的是，如果仅仅是 write process crash，而不是整个机器重启的话，是不会出现丢数据的。因为即使没有设置 sync flag，更新操作也会在返回前把数据从进程内存推到内核的 cache 中。

通常可以安全地使用 Async writes。例如，当将大量数据加载到数据库中时，您可以通过在崩溃后重新启动批量加载来处理丢失的更新。混合方案也是可能的，其中每个第 N 次写入都是同步的，并且在发生崩溃时，在上次运行完成最后一次同步写入后立即重新启动批量加载。 （同步写入可以更新描述崩溃时重新启动位置的标记）

`WriteBatch` 提供了 Async writes 的替代方案。多个更新可以 batch 成一个 WriteBatch，并使用 Sync write。这样可以将多个 Sync writes 的成本平摊到批处理中的所有写入中。


## Concurrency
一个数据库一次只能被一个进程打开。leveldb 的实现通过获取一把 OS 的锁来防止误用。在单个进程中，多个并发线程可以安全地共享同一个 `leveldb::DB` 对象。即不同的线程可以在没有任何 external sync 的情况下直接进行 write 或 fetch iterators 或 Get（leveldb 的实现会自动实现所需的同步原语）。

然而，其他对象（如 `Iterator` 和 `WriteBatch`）则需要外部的同步原语。如果两个线程共享这些类的对象，它们必须使用自己的 locking protocol 来保护对它的访问。


## Iteration
下例展示如何打印出一个 db 中的所有 kv pairs:
```c++
leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
}
assert(it->status().ok());  // Check for any errors found during the scan
delete it;
```

下例展示了如何处理在 range [start,limit) 中的 keys:
```c++
for (it->Seek(start);
     it->Valid() && it->key().ToString() < limit;
     it->Next())
{
  ...
}
```

也可以逆向的遍历处理数据（但是 reverse iteration 会比 forward iteration 慢一些）
```c++
for (it->SeekToLast(); it->Valid(); it->Prev()) {
	...
}
```

## Snapshots
Snapshots 提供了整个 kv sotre 的 consistent read-only views。可以通过将 `ReadOptions::snapshot` 字段设置为非空，以表示对一个特定的 version 的 read。若 `ReadOptions::snapshot` 为 NULL 则表示使用当前状态的 snapshot。

Snapshot 通过方法 `DB::GetSnapshot()` 来创建:
```c++
leveldb::ReadOptions options;
options.snapshot = db->GetSnapshot();
... apply some updates to db ...
leveldb::Iterator* iter = db->NewIterator(options);
... read using iter to view the state when the snapshot was created ...
delete iter;
db->ReleaseSnapshot(options.snapshot);
```

当一个 snapshot 不再需要时，应通过接口 `DB::ReleaseSnapshot` 释放。这将允许回收那些仅为了支持 snapshot read 而维护的状态。

## Slice
上文中的 `it->key()` 和 `it->value()` 返回的值的类型为 `leveldb::Slice`。Slice 本身包含一个 length 和一个指向外部 byte array 的指针。之所以选择返回 Slice 而不是 std::string 主要是，我们不希望对于那些潜在的 large key / value 作拷贝。此外，leveldb 的方法不会返回一个 null-terminated C-style string，因为 leveldb 的 key / value 本身是支持 `'\0'` bytes 的.

可以很方便的将一个 C++ string 和 null-terminated C-style string 转换为 Slice:
```c++
leveldb::Slice s1 = "hello";

std::string str("world");
leveldb::Slice s2 = str;
```

Slice 也很容易转换为 C++ string:
```c++
std::string str = s1.ToString();
assert(str == std::string("hello"));
```

Be careful when using Slices since it is up to the caller to ensure that the
external byte array into which the Slice points remains live while the Slice is
in use. For example, the following is buggy:
使用 Slice 时需要注意的是，因为其数据部分是一个指向外部字节数组的指针，因此要保证使用时外部字节数组仍是存活的状态。
> 例如，以下代码就会出现问题：
> ```c++
> leveldb::Slice slice;
> if (...) {
>   std::string str = ...;
>   slice = str;
> }
> Use(slice);
> ```


## Comparators
可以在打开数据库时提供一个自定义的 comparator 来对 key 排序。

例如，假设每个数据库键由两个数值组成，我们期望按第一个数值排序，然后再按第二个数值进一步排序。
1. 首先，定义一个 `leveldb::Comparator` 的子类来表达这些规则：
```c++
class TwoPartComparator : public leveldb::Comparator
{
public:
    // Three-way comparison function:
    //   if a < b: negative result
    //   if a > b: positive result
    //   else: zero result
    int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const
    {
        int a1, a2, b1, b2;
        ParseKey(a, &a1, &a2);
        ParseKey(b, &b1, &b2);
        if (a1 < b1) return -1;
        if (a1 > b1) return +1;
        if (a2 < b2) return -1;
        if (a2 > b2) return +1;
        return 0;
    }

    // Ignore the following methods for now:
    const char* Name() const { return "TwoPartComparator"; }
    void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
    void FindShortSuccessor(std::string*) const {}
};
```

2. 在建立 db 时通过 options 传入一个 comparator 的实例:
```c++
TwoPartComparator cmp;
leveldb::DB* db;
leveldb::Options options;
options.create_if_missing = true;
options.comparator = &cmp;
leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
...
```

### Backwards compatibility
comparator 的 Name() 方法的 result 会在数据库创建时被 attach 到数据库上，并在随后每次打开数据库时进行检查。如果名字改变了，`leveldb::DB::Open`调用就会失败。因此，当且仅当新的 key format 和 comparison func 与现有的数据库不兼容时，才会去改变其 name，并抛弃现有数据库的所有内容。

然而，可以通过一些 pre-planning 使得 key format 可以随着时间而改变 key format。例如，可以在每个 key 的末尾存储一个版本号（一个字节应该足以满足大多数用途）。当你想切换到一个新的 key format 时，则可以:（a）保持相同的 comparator 名称（b）将新 key 的版本号递增（c）修改 comparator 的实现，使其根据 keys 中的版本号来决定如何解析。


## Performance
可以通过修改 `include/options.h` 中定义的默认值来对性能进行调优。


### Block size
Leveldb 会将相邻的 keys 给聚合到同一个 block 中，这样的一个 block 是与持久存储之间进行一次交互的单位。默认 block size 约为 4096 个未压缩的字节。主要的 workload 是要对数据库内容进行 bulk scan 的应用程序可能希望增加这个参数的大小。主要的 workload 是要对数据库进行大量点查的应用则可能希望该参数会小一些。

需要注意的是，使用小于 1KB 的 block size 或大于几 MB 的 block size 都不会有太大的益处。另请，压缩对于 block size 较大的场景会更有效。


### Compression
每个 block 在写入到持久存储之前都经过单独压缩。默认情况下压缩是打开的，因为默认压缩方法非常快，并且对于不可压缩的数据会自动禁用。在极少数情况下，应用程序可能想要完全禁用压缩，但只有在基准测试显示性能改进时才应该这样做：
Each block is individually compressed before being written to persistent
storage. Compression is on by default since the default compression method is
very fast, and is automatically disabled for uncompressible data. In rare cases,
applications may want to disable compression entirely, but should only do so if
benchmarks show a performance improvement:

```c++
leveldb::Options options;
options.compression = leveldb::kNoCompression;
... leveldb::DB::Open(options, name, ...) ....
```

### Cache

The contents of the database are stored in a set of files in the filesystem and
each file stores a sequence of compressed blocks. If options.block_cache is
non-NULL, it is used to cache frequently used uncompressed block contents.

```c++
#include "leveldb/cache.h"

leveldb::Options options;
options.block_cache = leveldb::NewLRUCache(100 * 1048576);  // 100MB cache
leveldb::DB* db;
leveldb::DB::Open(options, name, &db);
... use the db ...
delete db
delete options.block_cache;
```

Note that the cache holds uncompressed data, and therefore it should be sized
according to application level data sizes, without any reduction from
compression. (Caching of compressed blocks is left to the operating system
buffer cache, or any custom Env implementation provided by the client.)

When performing a bulk read, the application may wish to disable caching so that
the data processed by the bulk read does not end up displacing most of the
cached contents. A per-iterator option can be used to achieve this:

```c++
leveldb::ReadOptions options;
options.fill_cache = false;
leveldb::Iterator* it = db->NewIterator(options);
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  ...
}
```

### Key Layout
需要注意的是，磁盘传输和缓存的单位是一个 block。相邻的 keys 通常会被放在同一个 block 中。因此，应用程序可以通过将那些会被一起访问的 keys 放在一起，并将不经常使用的 keys 放到一个 key space 中的一个单独区域，从而提高 db 的性能。

> 例如，假设我们想在 leveldb 之上实现一个简单的文件系统。
> 我们可能希望存储的条目类型是：
> - filename -> permission-bits, length, list of file_block_ids
> - file_block_id -> data
> 
> 我们可能想在 `filename` keys 前加上一个字母（比如说'/'），并在 `file_block_id` keys 前加熵另一个不同的字母（比如'0'），这样在进行 scan 元数据时就不会读到并缓存那些庞大的文件内容。


### Filters
由于 leveldb 的数据在磁盘上的组织方式，一次 `Get()` 调用可能涉及到多次的磁盘读取。可以通过 FilterPolicy 机制用来大幅减少磁盘读的次数。
```c++
leveldb::Options options;
options.filter_policy = NewBloomFilterPolicy(10);
leveldb::DB* db;
leveldb::DB::Open(options, "/tmp/testdb", &db);
... use the database ...
delete db;
delete options.filter_policy;
```

上面的代码将一个基于 bloom filter 的 filtering policy 与数据库关联起来。基于布隆过滤器的 filtering 依赖于为每个 key 在内存中保留一定的数据（比如上述代码中通过参数制定了，每个 key 会有 10-bit）。这个过滤器将减少 Get() 调用中不必要的磁盘读取次数，大约能减少 100 倍。当然为每个 key 增加 bits 会消耗更多的内存。一般建议那些 working set 不适合在内存中使用和需要做大量随机读的应用程序设置一个 filter policy。


如果要使用一个自定义的 comparator，应确保所使用的 filter policy 与 comparator 兼容。
> 例如，考虑一个 comparator，其实现会在比较 keys 时忽略尾部的空格。这种情况下 `NewBloomFilterPolicy` 就不能与这样的 comparator 一起使用。
> 这种情况，应用程序应该提供一个自定义的 filter policy，该策略也会忽略尾部的空格。
> 例如：
> ```c++
> class CustomFilterPolicy : public leveldb::FilterPolicy {
> private:
> 	FilterPolicy* builtin_policy_;
> 
> public:
> 	CustomFilterPolicy() : builtin_policy_(NewBloomFilterPolicy(10)) {}
> 	~CustomFilterPolicy() { delete builtin_policy_; }
> 
> 	const char* Name() const { return "IgnoreTrailingSpacesFilter"; }
> 
> 	void CreateFilter(const Slice* keys, int n, std::string* dst) const {
> 		// Use builtin bloom filter code after removing trailing spaces
> 		std::vector<Slice> trimmed(n);
> 		for (int i = 0; i < n; i++) {
> 			trimmed[i] = RemoveTrailingSpaces(keys[i]);
> 		}
> 		return builtin_policy_->CreateFilter(trimmed.data(), n, dst);
> 	}
> };
> ```

一些应用可以提供一个不使用 Bloom filter 的过滤策略，而使用其他机制来 summarizing 一组 keys。详见`leveldb/filter_policy.h`。


## Checksums
leveldb将校验和与它存储在文件系统中的所有数据联系起来。对于如何积极地验证这些校验和，有两个单独的控制。
leveldb associates checksums with all data it stores in the file system. There
are two separate controls provided over how aggressively these checksums are
verified:

leveldb associates checksums with all data it stores in the file system. There are two separate controls provided over how aggressively these checksums are verified:

`ReadOptions::verify_checksums` may be set to true to force checksum
verification of all data that is read from the file system on behalf of a
particular read.  By default, no such verification is done.

`Options::paranoid_checks`可以在打开数据库之前设置为 "true"，以使数据库实现在检测到内部损坏时立即引发错误。根据数据库被破坏的部分，错误可能会在数据库被打开时引发，也可能在之后被其他数据库操作引发。默认情况下，偏执检查是关闭的，这样即使数据库的部分持久性存储被破坏，也可以使用。

`Options::paranoid_checks` may be set to true before opening a database to make
the database implementation raise an error as soon as it detects an internal
corruption. Depending on which portion of the database has been corrupted, the
error may be raised when the database is opened, or later by another database
operation. By default, paranoid checking is off so that the database can be used
even if parts of its persistent storage have been corrupted.

如果一个数据库被破坏了（也许在开启偏执检查时无法打开），可以使用`leveldb::RepairDB`函数来恢复尽可能多的数据。

If a database is corrupted (perhaps it cannot be opened when paranoid checking
is turned on), the `leveldb::RepairDB` function may be used to recover as much
of the data as possible

## Approximate Sizes

The `GetApproximateSizes` method can used to get the approximate number of bytes
of file system space used by one or more key ranges.

```c++
leveldb::Range ranges[2];
ranges[0] = leveldb::Range("a", "c");
ranges[1] = leveldb::Range("x", "z");
uint64_t sizes[2];
db->GetApproximateSizes(ranges, 2, sizes);
```

The preceding call will set `sizes[0]` to the approximate number of bytes of
file system space used by the key range `[a..c)` and `sizes[1]` to the
approximate number of bytes used by the key range `[x..z)`.

## Environment

All file operations (and other operating system calls) issued by the leveldb
implementation are routed through a `leveldb::Env` object. Sophisticated clients
may wish to provide their own Env implementation to get better control.
For example, an application may introduce artificial delays in the file IO
paths to limit the impact of leveldb on other activities in the system.

```c++
class SlowEnv : public leveldb::Env {
  ... implementation of the Env interface ...
};

SlowEnv env;
leveldb::Options options;
options.env = &env;
Status s = leveldb::DB::Open(options, ...);
```

## Porting

leveldb may be ported to a new platform by providing platform specific
implementations of the types/methods/functions exported by
`leveldb/port/port.h`.  See `leveldb/port/port_example.h` for more details.

In addition, the new platform may need a new default `leveldb::Env`
implementation.  See `leveldb/util/env_posix.h` for an example.

## Other Information

Details about the leveldb implementation may be found in the following
documents:

1. [Implementation notes](impl.md)
2. [Format of an immutable Table file](table_format.md)
3. [Format of a log file](log_format.md)
