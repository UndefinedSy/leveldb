leveldb File format
===================
```
    <beginning_of_file>
    [data block 1]
    [data block 2]
    ...
    [data block N]
    [meta block 1]
    ...
    [meta block K]
    [metaindex block]
    [index block]
    [Footer]        (fixed size; starts at file_size - sizeof(Footer))
    <end_of_file>
```

文件包含内部的指针。这样的指针被称为 BlockHandle，它包含了如下信息:
```
    offset:   varint64
    size:     varint64
```

See [varints](https://developers.google.com/protocol-buffers/docs/encoding#varints)
for an explanation of varint64 format.

- 一个文件中的 kv-pairs 以一个有序序列的方式存储，并分区到多个 data blocks 中。
  - 这些 data blocks 从文件的开始处紧密地排列。
  - 每个 data block 的组织可以参看 `block_builder.cc` 中的代码，并且这些 block 是可选地压缩的。

- 在 data blocks 之后会存储若干个 meta blocks。
  - 下文有具体介绍支持的 meta block types。
  - 每个 meta block 的组织方式也是位于 `block_builder.ccl` 且可压缩。

- 之后是一个 metaindex block，它包含了每个 meta block 的条目。
  - key 是 meta block 的 name
  - value 是指向该 meta block 的 BlockHandle。

- 之后是一个 index block，它包含了每个 data block 的条目。
  - key 是一个 string，它 $\geq$ 对应 datablock 中的最后一个 key，且 $<$ 后继 data block 的第一个 key。
  - value 是对应 data block 的 BlockHandle。

- 文件的末尾是一个固定长度的 footer，其中包含了 metainfex block 和 index block 的 BlockHandle 以及一个 magic number。
    > ```
    >     metaindex_handle: char[p];     // Block handle for metaindex
    >     index_handle:     char[q];     // Block handle for index
    >     padding:          char[40-p-q];// zeroed bytes to make fixed length
    >                                    // (40==2*BlockHandle::kMaxEncodedLength)
    >     magic:            fixed64;     // == 0xdb4775248b80fb57 (little-endian)
    > ```

### "filter" Meta Block
如果在打开数据库时指定了一个 `FilterPolicy`，则每个表中将存储一个 filter block。
metaindex block 会包含一个从 `filter.<n>` 映射到该 filter block 的 Blockhandle，其中 `<n>` 是该 filter policy 的 `Name()` 方法所返回的字符串。

filter block 存储了一系列的 filters，其中 filter `i` 包含了 `FilterPolicy::CreateFilter()` 的 output
filter block 存储了一系列的 filters，其中 filter `i` 包含了对存储在文件偏移量在范围内的块中的所有键的 `FilterPolicy::CreateFilter()` 的输出
The filter block stores a sequence of filters, where filter i contains
the output of `FilterPolicy::CreateFilter()` on all keys that are stored
in a block whose file offset falls within the range

    [ i*base ... (i+1)*base-1 ]

目前，“base”是 2KB。因此，例如，如果块 X 和 Y 从范围`[ 0KB .. 2KB-1 ]` 开始，则 X 和 Y 中的所有键都将通过调用 `FilterPolicy::CreateFilter()` 转换为过滤器，并且生成的过滤器将作为过滤器块中的第一个过滤器存储。
Currently, "base" is 2KB.  So for example, if blocks X and Y start in
the range `[ 0KB .. 2KB-1 ]`, all of the keys in X and Y will be
converted to a filter by calling `FilterPolicy::CreateFilter()`, and the
resulting filter will be stored as the first filter in the filter
block.

filter block 的组织形式如下:
```
    [filter 0]
    [filter 1]
    [filter 2]
    ...
    [filter N-1]

    [offset of filter 0]                  : 4 bytes
    [offset of filter 1]                  : 4 bytes
    [offset of filter 2]                  : 4 bytes
    ...
    [offset of filter N-1]                : 4 bytes

    [offset of beginning of offset array] : 4 bytes
    lg(base)                              : 1 byte
```

过滤器块末尾的偏移数组允许从数据块偏移到相应过滤器的高效映射。
The offset array at the end of the filter block allows efficient
mapping from a data block offset to the corresponding filter.

### "stats" Meta Block
stats meta block 中包含了各种统计数据。其 key 是统计数据的 name，其 value 包含了实际的统计信息。

TODO(postrelease): record following stats.

    data size
    index size
    key size (uncompressed)
    value size (uncompressed)
    number of entries
    number of data blocks
