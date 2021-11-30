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

对一个 level 的 compactions 是通过 key space 来 rotate 的。更具体来说，对于每个 level-L，我们会记住 level-L 的最后一次 compaction 的 ending key，对 level-L 的下一次 compaction 将选择在这个 ending key 之后开始的第一个文件（如果没有这样的文件，就会绕到 key space 的开头(WHY not terminate?)）。

Compactions 会丢弃掉那些被覆盖的 values。
如果一个 key 带有删除标记，且没有更高编号（更新）的 levels 中存在有文件的 key range 与该 key 有重叠，则也会丢弃该删除标记。（They also drop deletion markers if there are no higher numbered levels that contain a file whose range overlaps the current key.）


### Timing
Level-0 compactions 最多会从 level-0 中读取 4 个 1MB 的文件，以及最坏的情况下会读取所有的 level-1 的文件（10MB）（也就是说读取 14MB 并写入 14MB）。

除了特殊的 level-0 的 compactions 之外，我们将从 level-L 中挑选一个 2MB 的文件。
在最坏的情况下，这将与 level-(L+1) 的约 12 个文件存在 overlap:
> - 10 个是因为 level-(L+1) 的大小是 level-L 的 10 倍，
> - 另外 2 个是在 boundaries 处，因为 level-L 的 file ranges 通常不会与 level-(L+1) 的 file ranges 对齐。

因此，compaction 将读取 26MB，写入 26MB。假设磁盘 I/O 速率为 100MB/s，最差的情况下 compaction 将花费 0.5 秒。

如果我们把后台写入的速度控制在较小的范围内，比如说 100MB/s 的 10%，那么 compaction 则会需要 5 秒。如果用户的写入速度是 10MB/s，则可能会建立大量的 level-0 文件（约 50 个以容纳 50MB）。这 这可能会大大增加读操作的开销，因为每次读取都要合并更多的文件。

**Solution 1**: 当 level-0 files 的数量较多时，可以增加 log switching 的阈值。其缺点是: 这个阈值越大，我们就需要更多的内存来容纳相应的 memtable。

**Solution 2**: 当 level-0 files 的数量增加时，可以人为地降低写入速率。

**Solution 3**: 我们致力于降低 wide merges 的成本。大部分的 level-0 文件的 blocks 在缓存中没有被压缩，我们只需要考虑 merging iterator 中的 O(N) 的复杂性。


### Number of files
除了 2MB 的文件，我们也可以为更高 levels 生成更大的文件，从而减少总的文件数，但代价是更多的 bursty（突发的）compactions。 另外，我们也可以将文件 shard 到多个目录。

> 这是一个在 ext3 文件系统上的实验，显示了在具有不同文件数量的目录中进行 100K 次 file opens 的耗时:
> 
> | Files in directory | Microseconds to open a file |
> |-------------------:|----------------------------:|
> |               1000 |                           9 |
> |              10000 |                          10 |
> |             100000 |                          16 |

所以在现代文件系统中，对目录的 sharding 也许是不必要的。


## Recovery
* 读文件 CURRENT 以找到 latest committed MANIFEST 的文件名
* 读取对应的 MANIFEST 文件
* 清理 stale files
* 现在可以打开所有的 sstables，但可能最好是采用 lazy 的方式
* 将 log chunk 转换为一个新的 level-0 sstable
* 将新的写请求指向到一个新的 log file，该文件会带有 recovered sequence#


## Garbage collection of files
`RemoveObsoleteFiles()` 会在每次 compaction 结束和 recovery 结束时被调用。它会先找出数据库中所有文件名。
- 它会删除不是 current log file 的日志文件
- 它会删除没有被任何 level 所引用，且不是一个 active compaction 的 ouput 的 table files
