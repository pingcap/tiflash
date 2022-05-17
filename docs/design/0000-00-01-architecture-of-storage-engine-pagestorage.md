# Architecture Of Storage Engine - PageStorage

- Authors(order by last name): [JaySon-Huang](https://github.com/JaySon-Huang), [flowbehappy](https://github.com/flowbehappy), [Jiaqi Zhou](https://github.com/jiaqizho)
- Technical Writer: [shichun-0415](https://github.com/shichun-0415) , [Jiaqi Zhou](https://github.com/jiaqizho)

## Introduction

`PageStorage` is the place where DT(DeltaTree Engine) actually stores data into. The latest data (i.e. delta data), the metadata in the engine are serialized directly into Pages. While the main data, i.e. stable data, is written in DTFiles format and managed as `ExternalPage`s in PS.

PageStorage needs to handle a large amount of delta data read and write, this part of the data is hot data. And a small amount of DT meta information needs to be persisted, this part is cold data. The data of delta  will eventually be merged into the `stable part`(In DT). The `stable part` of DT can be referred to dmf file.

You can see the picture below. Described in the picture is the design of the DT. PageStorage stored `delta`, the delta part of data always be updated. After `delta merge` happend, the delta in PageStorage will be merge to the stable part(in picture, it named stable value space).

![tiflash-dt-architecture](./images/tiflash-dt-architecture.png)


As one of the important components of DT, PageStorage mainly provides a KV storage service which also support MVCC. Unlike other KV services, the KV interface provided by PageStorage is limited. Key is limited to uint64_t, Value is limited to a buffer or a array of buffer(we called it fields) or null.


## Capability

PageStorage supported: 

- Disk-based store
- Write/Read operation atomicity
- Full MVCC function 
- KV store function
- GC


## Design

Currently there are three versions of PageStorage (V1, V2, V3). The V1 version is no longer used by DT. Due to compatibility considerations, it's still used in some components, we considered to remove it the future. It won't be covered too much in this article.

At present, most of our customers use the V2 version. Due to design issues, the V2 version has some problems that cannot be improved(will be introduced below), so we propose the V3 version.

### V3 version

After our customers used TiFlash, we found some problems with the V2 version in the actual production environment.

1. **There is a risk of data loss in the meta part**. As long as the `checksum` and `buffer size` fields in a single meta buffer are damaged at the same time, the subsequent buffers will be unavailable.This situation may happen when When the disk failure or the meta been changed due to wrong operation.
2. **The snapshot of MVCC needs to be optimized**. First, the memory occupied by the snapshot can be reduced. Secondly, There are no need such a complex structure to implement MVCC.
3. **The GC write amplification in the data part is too severe, The GC task is too heavy**. Since the data part is composed of append write, `compact gc` is frequently triggered, The `compact gc` means PageStorage need read all of the valid data from the disk, then rewrite it into a new file. Each round of GC brings additional read and write overhead.

Besides these three problems, we also changed the `lock` implements and the `CRC` implements.

![tiflash-ps-v3-architecture](./images/tiflash-ps-v3-architecture.png)

The V3 version of PageStorage consists of two main components, one is `WALStore` and the other is `BlobStore`.

- WALStore(Write Ahead Log Store): Using the write ahead log file format to manager the meta part.
- PageDirectory: Provides the function of MVCC. Smaller memory usage and faster speed than V2.
- BlobStore: Provides an spaces management. Using the address multiplexing to manage the data part.

#### WALStore

WALStore using the fixed block space to manager the meta.

![tiflash-ps-v3-walstore](./images/tiflash-ps-v3-wal-store.png)

WALStore used fixed size of a block to manage meta part.

- A block contains multiple meta records.
- If a block has some space unused, Also unused space size less than a meta record size. Then using a `padding` to full the unused space.

If crc and meta size are broken at the same time. It is only need to discard the data in a single block, and the neighboring block(the prev block or next block) will be preserved. Because WALStore will read meta from single block, If single block got fault won't effect others. Also more convenient to troubleshoot the cause of meta errors.

WALStore provider two main interfaces:

- **apply**: After `apply`, the meta info will be serialized on disks. This happens after writing datas.
- **read**: Read serialized meta info from disks. This will happen after `PageStorage` restored.

In addition, WALStore also needs GC. Although the overall read/write sizes is not large(about 4-6 mb read and write), But WALStore still need sort out some invalid meta information on disks.

The sturction of meta similar with V2:

buffer(operate:put/upsert)

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:8                 | Write Type       | Write batch operation type |
8:16                | Flag             | The page Flag decide page detach or not       |
16:144              | Page Id          | The combine of namespace id and page id       |
144:272             | Version          | The combine of sequence and epoch             |
272:336             | Ref count        | The page be ref count  |
272:N					| page entry       | The page entry  |

page entry

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:32                | Blob File id     | The Blob File Id |
32:96               | Offset           | The Page Entry offset |
96:160              | Size             | The Page Entry size |
160:224             | Checksum         | The Page Entry checksum |
224:288             | Tag         	   | The Page tag |
288:352             | Field offsets length    | The length of field offset      |
352:N 					| Field offsets    | The length field offsets      |


Field offsets 

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:64                | Field Offset       | The field offset |
64:128              | Field Checksum | The field checksum |

buffer(operate:put/upsert)

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:8                 | Write Type       | Write batch operation type |
8:16                | Flag             | The page Flag decide page detach or not       |
16:144              | Page Id          | The combine of namespace id and page id       |
144:272             | Version          | The combine of sequence and epoch             |
272:336             | Ref count        | The page be ref count  |
272:N					| page entry       | The page entry  |


buffer(operate:ref)

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:8                 | Write Type       | Write batch operation type |
8:132               | Page Id          | The combine of namespace id and page id       |
132:260             | Origin Page Id   | The combine of namespace id and page id       |
260:388             | Version          | The combine of sequence and epoch             |

buffer(operate:put_ext)

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:8                 | Write Type       | Write batch operation type |
8:132               | Page Id          | The combine of namespace id and page id       |
132:260             | Version          | The combine of sequence and epoch             |
260:324             | Ref count        | The page be ref count  |

buffer(operate:del)

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:8                 | Write Type       | Write batch operation type |
8:132               | Page Id          | The combine of namespace id and page id       |
132:260             | Version          | The combine of sequence and epoch             |

The page id in V3 is is a 128bit unsigned integer replace the 64bit(in v2). Because instead of generating three instances(meta/data/log) of PageStorage for each table, there are only four instances of PageStorage globally(log/data/meta/KVStore). So PageStorage need additional 64bit space (ie. namespace id) to distinguish different tables.

Multi of meta info will be conbimed into a block. So it is different with V2, WALStore added a WAL format to pack the meta struction. 

Legacy record format:

```
+--------------+-----------+-----------+--- ... ---+
|CheckSum (8B) | Size (2B) | Type (1B) | Payload   |
+--------------+-----------+-----------+--- ... ---+
```

- CheckSum: 64bit hash computed over the record type and payload using checksum algo (CRC64)
- Size: Length of the payload data
- Type: Type of record(ZeroType, FullType, FirstType, LastType, MiddleType)
  - The type is used to group a bunch of records together to represent
  - blocks that are larger than kBlockSize
- Payload: Byte stream as long as specified by the payload size

Recyclable record format:

```
+--------------+-----------+-----------+----------------+--- ... ---+
|CheckSum (8B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
+--------------+-----------+-----------+----------------+--- ... ---+
```

Same as above, with the addition of

- Log number: 32bit log file number, so that WALStore can distinguish between records written by the most recent log writer vs a previous one.


#### PageDirectory

PageDirectory supports the function of PageDirectory MVCC. Its main component is a hashmap. The key in map is `page id` and the value in map is the `version chian`.

The `version chain` consists of `[seq|epoch]` and `page entry`, sequence + epoch determines the position of the page entry in the chain, `page entry` have similar implementation with V2, It is the embodiment of the page in memory, recording the location of the data.

Here is a PageDirectory example:

```
page id 1 : {[seq 1 + epoch 0, entry 1], [seq 2 + epoch 0, entry 2], [seq 3 + epoch 1, entry 3]}
page id 2 : {[seq 1 + epoch 0, entry 4], [seq 2 + epoch 0, entry 5], [seq 5 + epoch 0, entry 6]}
page id 100 : {[seq 1 + epoch 0, entry 7], [seq 10 + epoch 2, entry 8]}
```

In this example, MVCC have 3 page with differe id. 

- page 1 has 3 different versions corresponding to 3 entries. page 100 has 2 different versions corresponding to 2 entries.
- The seq will increase If MVCC have a corresponding new page written.
- The epoch will be increase If current entry have been full GC.

`getSnapshot()` in V3 is very different from V2, In V2, MVCC actually generate snapshots, there are some copies of memory. But in V3, Snapshot only contains a `sequence id` which can filter the right pages from the PageDirectory.

In upper example. If sequence in snapshot is 2 and query page id is 2. Then MVCC will return 
the entry 5.


#### BlobStore

BlobStore's name is earthy, but apt. It mainly stores `Blob`, which consists of three parts

1. **BlobFile**: Blob stored file, used to write and read.
2. **BlobStat**: A space manager, one-to-one correspondence with blobfile. used to find/alloc/free spaces in BlobFile.
3. **BlobStats**: Manage all BlobStat. Used to schedule all write requests.

Different to V2 design, we decided to abandon the append write mode. Instead, a data structure called SpaceMap used to manage the file spaces.

![tiflash-ps-v3-freemap](./images/tiflash-ps-v3-freemap.png)


This idea come form a rb-tree bitmap implements. Every node in `rb-tree`(red black tree) have a space which conbime with offset + size. But the difference is that bitmap uses `rb-node`(the node from red black tree) to record used locations, BlobStore used `rb-node` to record free locations. Because record free locations better to find a free space which used to store a buffer of data.

The SpaceMap inside `BlobStat`, BlobStat can use it to reuse the reclaimed space, thereby reducing write amplification. 

In addition, BlobStat also needs to provide external statistical status, such as the valid rate of the current BlobFile, the maximum capacity of the current SpaceMap, and so on. These data must be calculated when inserting and removing, otherwise BlobStat have to traverse the SpaceMap to get the data we need.

The statistical status from BlobStat is very necessary and useful. 

1. Provide it to BlobStats so that BlobStats can determine whether to create a new Blobfile or reuse the old one.
2. When BlobStats selects BlobFile to write, it will write according to the BlobFile with the lowest valid rate.
3. When GC occurs, it can quickly determine whether the current BlobFile needs to be `full GC`(similar with `compact GC`).
4. Because the release of the file space may cause invalid data to be stored at the end of the BlobFile, the truncate operation can also be performed in time during GC to reduce space enlargement.

Finally, from the description of BlobStat, it is not difficult to find that BlobStats is used to schedule all write IO. Then write request happend, BlobStore will ask a unused space from BlobStats, Then BlobStore can put the buffer into that disk space. These operate all happend in memory, So BlobStore can maximize IO parallelization.

#### GC

The GC of V3 will be more complicated, but compared with V2, the GC of V3 is faster. The IO from V3 GC will be much less.

1. Begin to GC, PageStorage need do WALStore GC, this part is lightweight. When the meta file recorded by wal reach a certain level, WALStore will dump it and update it.
2. After that, PageStorage need do MVCC GC, MVCC will cleanup the expired snapshot in PageDirectory, also these expired page entry from expired snapshot will be mark clean in BlobStore. This means that all of SpaceMap in BlobStore will be updated.
3. Some of space in BlobStore be free after MVCC GC, Then BlobStore will check all BlobStat, find out if there is a blob with a low valid rate, and perform full gc on it.The full GC simliar with V2, it will copy the valid data ,and store it to other blob. 
4. If There do have full GC happend in BlobStore(from step 3), Then BlobStore need to tell PageDirectory that some of data has been migrated and where is the new location. PageDirectory will apply the change from BlobStore.

Although this looks complicated, in practice, the probability of BlobStore triggering full gc is very low, which means that we will not generate too many read and write IO during the GC process.
