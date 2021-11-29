## Files
leveldb 的实现在思想上类似于单个的 [Bigtable tablet (section 5.3)](https://research.google/pubs/pub27898/)。然而，文件的组织形式有些不同，下面将解释。

每个数据库都由存储在一个目录中的一组文件表示。有下文所述的几种不同类型的文件：

### Log files
一个 log file(*.log) 存储了一个 recent updates 的序列。每个更新都被 append 到当前的日志文件中。当日志文件达到预先确定的大小（默认情况下约为 4MB）时，它将被转换为一个 sorted table，然后创建一个新的 log file 用于之后的 updates。

当前的 log file 的一个副本会被保存在一个 in-memory 结构 (`memtable`) 中。这个副本用于加速读请求。

## Sorted tables
一个 sorted table(*.ldb) 存储了一连串的按 key 排序的 entries。每个 entry 或者是一个 key-valye，或者是一个 key 的删除标记。(删除标记会被保留下来，以覆盖隐藏掉旧的 sorted table 中存在的旧数据）。

这组 sorted tables 被组织成一个 levels 的序列。从一个 log file 中生成的 sorted table 被放置在一个特殊的 **young** level（也称为 level-0）。当 young files 的数量超过一定的阈值（目前是 4 个），所有的 young files 会与所有有交集的 level-1 的 files 合并，并产生一系列新的 level-1 files（我们为每 2MB 的数据创建一个新的 level-1 file）。

young level 中的文件可能包含有重叠的 keys。然而其他 levels 中的文件有明显的不重叠的 key ranges。考虑到 level-L（$L \geq 1$），当 level-L 中的文件的总大小超过 $10^L$MB 时（即 level-1 为 10MB，level-2 为 100MB），level-L 中的一个文件，以及 level-(L+1) 中所有有重叠文件被合并成一组新的 level-(L+1) 的新文件。这些合并的效果是逐步将新的 updates 从 young level 迁移到 largest level，且这样的迁移之需要使用 bulk reads & writes（即最大限度地减少昂贵的 seek 行为）。


### Manifest
MANIFEST 文件中列出了组成每个 level 的一组 sorted tables，相应的 key range，以及其他重要的元数据。每当数据库被重新打开时，就会创建一个新的 MANIFEST 文件（在文件名中会嵌入一个新的编号）。MANIFEST 文件的组织形式是一个 log，对服务状态所做的变更（即文件的添加或删除）会被 append 到该日志中。

### Current
CURRENT 是一个简单的文本文件，其内容为最新的 MANIFEST 文件的名称。

### Info logs
Infomational messages 会被打印到名为 LOG 和 LOG.old 的文件中。

### Others
还有一些用于其他各种用途的文件的存在 (LOCK, *.dbtmp)


## Level 0
当 log file 增长到超过一定大小时（默认为 4MB），会创建一个全新的 memtable 和 log file，并将之后的 updates 指向这里。

在后台会：
1. 把之前的 memtable 的内容写到 sstable 中。
2. 丢弃该 memtable。
3. 删除旧的 logfile 和旧的 memtable。
4. 将新的 sstable 添加到 young level (level-0)。


## Compactions
当 level-L 的大小超过其极限时，我们会通过一个后台线程去 compact 它。compaction 从 level-L 中选出一个文件，并找出 level-(L+1) 中的所有 overlapping 的文件。需要注意的是，如果一个 level-L 的文件只与一个 level-(L+1) 文件的一部分重叠，那么这个 level-(L+1) 文件的整体将被用作 compaction 的输入，并在 compaction 后被丢弃。

另外，因为 level-0 是特殊的（其中的文件可能彼此相互 overlap），我们对从 level-0 到 level-1 的 compactions 会进行特殊处理：如果其中一些文件相互重叠，每次 level-0 的 compaction 可能会挑选超过一个以上的 level-0 files。

一次 compaction 会合并所挑选文件的内容，以产生一系列的 level-(L+1) 的文件。
- 在当前的 output file 达到目标文件大小（2MB）后，我们会切换生成一个新的 level-(L+1) 文件。
- 在当前的 output file 的 key range 增长到与 10 个以上的 level-(L+2) 存在 overlap 时，我们也会切换生成一个新的 output file。
  - 这条规则确保了之后的对 level-(L+1) 文件的 compaction 不会从 level-(L+2) 中 pick up 太多数据。

丢弃旧文件 和 添加新文件 都会被添加到 serving state。

一个特定 level 的 compactions 是通过键空间来旋转的。更详细地说，对于每个级别L，我们记住L级别的最后一次压实的结束键，L级别的下一次压实将选择在这个键之后开始的第一个文件（如果没有这样的文件，就绕到键空间的开头）。


Compactions for a particular level rotate through the key space. In more detail,
for each level L, we remember the ending key of the last compaction at level L.
The next compaction for level L will pick the first file that starts after this
key (wrapping around to the beginning of the key space if there is no such
file).

压实会删除被覆盖的值。如果没有更高编号的级别包含一个范围与当前键重合的文件，它们也会放弃删除标记。
Compactions drop overwritten values. They also drop deletion markers if there
are no higher numbered levels that contain a file whose range overlaps the
current key.

### Timing
0级压缩将从0级读取最多4个1MB的文件，最坏的情况是 所有的1级文件（10MB）。也就是说，我们将读取14MB并写入14MB。
Level-0 compactions will read up to four 1MB files from level-0, and at worst
all the level-1 files (10MB). I.e., we will read 14MB and write 14MB.

除了特殊的0级压缩之外，我们将从0级中挑选一个2MB的文件。L. 在最坏的情况下，这将与L+1级的12个文件重叠（10个是因为 级别（L+1）的大小是L级的10倍，另外两个在边界处 因为L级的文件范围通常不会与L+1级的文件范围对齐）。在L级的文件范围通常不会与L+1级的文件范围对齐）。因此，压实将读取26MB，写入26MB。假设磁盘IO率为100MB/s（现代硬盘的大致范围），最差的压实成本将约为0.1美元。压实成本将是大约0.5秒。

Other than the special level-0 compactions, we will pick one 2MB file from level
L. In the worst case, this will overlap ~ 12 files from level L+1 (10 because
level-(L+1) is ten times the size of level-L, and another two at the boundaries
since the file ranges at level-L will usually not be aligned with the file
ranges at level-L+1). The compaction will therefore read 26MB and write 26MB.
Assuming a disk IO rate of 100MB/s (ballpark range for modern drives), the worst
compaction cost will be approximately 0.5 second.

如果我们把后台写入的速度控制在较小的范围内，比如说100MB/s全速的10%，那么压缩成本就会降低。100MB/s的速度，压实可能需要5秒。如果用户的写入速度是 10MB/s，我们可能会建立大量的0级文件（~50个以容纳5*10MB）。这 这可能会大大增加读取的成本，因为每次读取都要合并更多的 文件的开销。


If we throttle the background writing to something small, say 10% of the full
100MB/s speed, a compaction may take up to 5 seconds. If the user is writing at
10MB/s, we might build up lots of level-0 files (~50 to hold the 5*10MB). This
may significantly increase the cost of reads due to the overhead of merging more
files together on every read.

解决方案1：为了减少这个问题，我们可能要增加日志切换的阈值。当0级文件的数量较多时，我们可能要增加日志切换的阈值。尽管其缺点是 这个阈值越大，我们就需要更多的内存来容纳 相应的memtable。

Solution 1: To reduce this problem, we might want to increase the log switching
threshold when the number of level-0 files is large. Though the downside is that
the larger this threshold, the more memory we will need to hold the
corresponding memtable.

解决方案2：当0级文件的数量增加时，我们可能想人为地降低写入率。0级文件的数量增加时，我们可能要人为地降低写入率。

Solution 2: We might want to decrease write rate artificially when the number of
level-0 files goes up.

解决方案3：我们致力于降低非常宽的合并的成本。也许大部分的 0级文件的区块在缓存中没有被压缩，我们只需要担心那些被压缩的区块。我们只需要担心合并迭代器中的O(N)复杂性。

Solution 3: We work on reducing the cost of very wide merges. Perhaps most of
the level-0 files will have their blocks sitting uncompressed in the cache and
we will only need to worry about the O(N) complexity in the merging iterator.

### Number of files

我们可以不总是制作2MB的文件，而是为更大的级别制作更大的文件 来减少总的文件数，但代价是更多的突发性的 压实。 另外，我们也可以将文件集分到多个 目录。

Instead of always making 2MB files, we could make larger files for larger levels
to reduce the total file count, though at the expense of more bursty
compactions.  Alternatively, we could shard the set of files into multiple
directories.

2011年2月4日在ext3文件系统上的一个实验显示了以下时间 在具有不同数量文件的目录中进行100K的文件打开。

An experiment on an ext3 filesystem on Feb 04, 2011 shows the following timings
to do 100K file opens in directories with varying number of files:


| Files in directory | Microseconds to open a file |
|-------------------:|----------------------------:|
|               1000 |                           9 |
|              10000 |                          10 |
|             100000 |                          16 |

So maybe even the sharding is not necessary on modern filesystems?

## Recovery
* 读取CURRENT以找到最新提交的MANIFEST的名称
* 读取命名的MANIFEST文件
* 清理陈旧的文件
* 我们可以在这里打开所有的sstables，但可能最好是偷懒......
* 将日志块转换为一个新的0级sstable
*开始引导新的写操作到一个新的日志文件，并恢复序列#。

* Read CURRENT to find name of the latest committed MANIFEST
* Read the named MANIFEST file
* Clean up stale files
* We could open all sstables here, but it is probably better to be lazy...
* Convert log chunk to a new level-0 sstable
* Start directing new writes to a new log file with recovered sequence#

## Garbage collection of files
`RemoveObsoleteFiles()`在每次压缩结束和恢复结束时都会被调用。恢复的时候调用。它找出数据库中所有文件的名称。它删除了所有 不是当前日志文件的所有日志文件。它删除了所有没有被某个级别引用的表文件。的表文件，这些文件不是从某个级别引用的，也不是活动压实的输出。

`RemoveObsoleteFiles()` is called at the end of every compaction and at the end
of recovery. It finds the names of all files in the database. It deletes all log
files that are not the current log file. It deletes all table files that are not
referenced from some level and are not the output of an active compaction.
