leveldb Log format
==================
log file 中是一连串的 32KB 的 blocks。唯一的例外是文件的尾部可能包含一个 partial block。

每个 bock 包含一连串的 records: `block := record* trailer?  // 若干个 record, 0/1 个 trailer`
```
record :=
    checksum: uint32     // crc32c of type and data[] ; little-endian
    length: uint16       // little-endian
    type: uint8          // One of FULL, FIRST, MIDDLE, LAST
    data: uint8[length]
```

一个 record 一定不会起始于一个 block 的最后 6 个字节。在一个 block 中剩余的 bytes 构成了 trailer，trailer 必须完全由零字节组成，且必须被 reader 所跳过。

另外，如果在当前的 block 中剩下了 7 个字节，并且准备添加一个新的 non-zero length 的 record，则 writer 必须发出一个 FIRST record（不包含用户数据）以填补该 block 的尾部的 7 个字节，然后在后续的 block 中再发出所有用户数据。

将来可能会添加更多类型。有些 Readers 可以跳过他们不知道的 record types，也可能会报告跳过了一些数据。

```
    FULL == 1
    FIRST == 2
    MIDDLE == 3
    LAST == 4
```
- FULL record 包含整个 user record 的内容。
- FIRST, MIDDLE, LAST 则是用于 user records 被拆分成多个片段的情况（通常是因为 block 的边界造成）。
  - FIRST 是 user record 的第一个片段
- LAST 是 user record 的最后一个片段
- MIDDLE 是 user record 的中间片段

> 例如一个 user records 的序列:
> ```
>     A: length 1000
>     B: length 97270
>     C: length 8000
> ```
> - **A** 将以一个 FULL record 的形式存储于第 1 个 block
> - **b** 将分为 3 个片段:
>   - 第 1 个片段会占据第 1 个 block 的剩余部分
>   - 第 2 个片段会占据第 2 个 block 的全部
>   - 第 3 个片段会占据第 3 个 block 的前缀部分。且这将在第 3 个 block 中留下 6 个字节，这将作为 trailer 部分留空
> - **C** 将以一个 FULL record 的形式存储于第 4 个 block

----

## Some benefits over the recordio format:
1. 不需要任何基于启发式的 resyncing: 只需转到下一个 block boundary 并进行扫描。
   - 如果数据有损坏，则跳至下一个 block。
   - 一个日志文件的一部分内容可能会嵌入在另一个日志文件中的 record。

2. 很容易在近似的边界做切分（例如 MapReduce）：找到下一个 block 的 boundary 向后找 records，直到我们找到一个 FULL / FIRST record。

3. 对于 large records 不需要额外的 buffering。

## Some downsides compared to recordio format:
1. 没有对 tiny records 做 packing。
    - 这可以通过添加新的 record type 来修复，因此它是当前实现的一个不足，但不是格式所导致的。

2. 没有 compression 的能力
    - 仍然，这可以通过新的 record type 来实现。