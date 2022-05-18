# Architecture Of Storage Engine - PageStorage

- Authors(order by last name): [JaySon-Huang](https://github.com/JaySon-Huang), [flowbehappy](https://github.com/flowbehappy), [Jiaqi Zhou](https://github.com/jiaqizho)
- Technical Writer: [shichun-0415](https://github.com/shichun-0415) , [Jiaqi Zhou](https://github.com/jiaqizho)

## Introduction

`PageStorage` is the place where DT(DeltaTree Engine) actually stores data into. The latest data (i.e. delta data), and the metadata in the engine are serialized directly into Pages. While the main data (i.e. stable data), is written in DTFiles format and managed as `ExternalPage`s in PageStorage.

The below picture describes the "Delta ValueSpace" and "Stable ValueSpace" of DT. The data in "Delta ValueSpace" is continuously updated. After `delta merge` happened, the delta data in PageStorage will be read and compacted into the "Stable ValueSpace".

![tiflash-dt-architecture](./images/tiflash-dt-architecture.png)


As one of the important components of DT, PageStorage mainly provides a KV storage service which also support MVCC. Unlike other KV services, the KV interface provided by PageStorage is limited. Key is limited to uint64_t, Value is limited to a buffer or a array of buffer(we called it fields) or null.


## Capability

PageStorage supported: 

- Disk-based store
- Write/Read operation atomicity
- Full MVCC function 
- KV store function
- GC


## Background

Currently, there are three versions of PageStorage (V1, V2, V3). We won't describe the details of the V1/V2 in this article. The V2 design and implementation lead to high write amplification and CPU usage under some scenarios, so we propose the V3 version.


We found some problems with the V2 version in the actual production environment.

1. **There is a risk of data loss in the meta part**. As long as the `checksum` and `buffer size` fields in a single meta buffer are damaged at the same time, the subsequent buffers will be unavailable. This situation may happen when the disk failure happens or the meta part is changed by an unexpected operation.
2. **The snapshot of MVCC needs to be optimized**. First, the CPU usage occupied by the snapshot should be reduced. Secondly, the implementation of MVCC structure is too complex to be understood and maintained.
3. **The GC write amplification in the data part is high and the GC task is too heavy**. Since the data part is composed of append write, `compact gc` is frequently triggered. Besides, the meta part is bound to a data part by "PageFile", meaning that we have to apply `compact gc` to compact the small data in the meta part. The `compact gc` means PageStorage need read all of the valid data from the disk, then rewrite it into a new file. Each round of GC brings additional read and write overhead.

Besides these three problems, we also changed the `lock` implements and the `CRC` implements.

## Design

![tiflash-ps-v3-architecture](./images/tiflash-ps-v3-architecture.png)

The V3 version of PageStorage is composed of three main components, `PageDirectory`, `WALStore`, and `BlobStore`.

- BlobStore: Provides an spaces management. Using the address multiplexing to manage the data part.
- PageDirectory: Provides the function of MVCC. Smaller memory usage and faster speed than V2.
- WALStore(Write Ahead Log Store): Using the write ahead log file format to manager the meta part.


#### PageDirectory

PageDirectory supports the function of MVCC, providing a read-only snapshot that does not block writes. It is mainly composed of an RB-tree map with the key is `page id` and the value is the `version chain`.

The `version chain` is another RB-tree map with key `PageVersion` and value `PageEntry`. `PageEntry` represents the location of the data in `BlobStore` and is sorted by `PageVersion`(`<sequence, epoch>`) in the `version chain`. `PageVersion` is used for filtering by a read-only snapshot. PageDirectory will increase the `sequence` in serial when applying WriteBatches. The page entries created in the same WriteBatch use the same `sequence`, and the epoch is initialized to 0. After we applied "full GC" that moves the data into another location, we will create page entries with the same sequence but the epoch is last epoch + 1.

Creating a snapshot for PageStorage is simply atomically getting the `sequence id` from PageDirectory. And the `sequence id` in the snapshot will be used to filter the result and provide an immutable result from PageDirectory. PageDirectory always return the latest entry version that is less than `sequence id + 1`.

Here is a example:

```
page id 1 : {[(seq 1, epoch 0), entry 1], [(seq 2, epoch 0), entry 2], [(seq 3, epoch 1), entry 3]}
page id 2 : {[(seq 1, epoch 0), entry 4], [(seq 2, epoch 0), entry 5], [(seq 5, epoch 0), entry 6]}
page id 100 : {[(seq 1, epoch 0), entry 7], [(seq 10, epoch 2), entry 8]}
```

In this example, PageDirectory have 3 page with different id. Page 1 has 3 different versions corresponding to 3 entries. page 100 has 2 different versions corresponding to 2 entries. If caller get a entry for page id 2 with a snapshot that sequence is 2, PageDirectory will return the entry 5.


### WALStore

WALStore provides two main interfaces:

- **apply**: After `apply`, the WriteBatch info will be atomicity serialized on disks. This happens after writing data to BlobStore.
- **read**: Read serialized meta info from disks. This will happen when `PageStorage` is being restored.

In order to control the time of restoring WriteBatches and reconstruct the PageDirectory while startups, PageDirectory will dump its snapshot into WALStore and clear the old log files by the snapshot.

#### File format
![tiflash-ps-v3-walstore](./images/tiflash-ps-v3-wal-store.png)

WALStore builds upon its serialized write batches (the meta part) base on log file format. The log file format is similar to the log file format as rocksdb [Write Ahead Log File Format](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format#log-file-format).

This log file format is convenient to detect disk failure. If a disk failure happens, we can stop TiFlash from startup and returning wrong results. In the worst case when CRC and the length of a meta record are broken at the same time, we can try to discard the broken data by the fixed-length block, other blocks can be preserved and try to recover some data.

WALStore serializes the WriteBatch into an atomic record and writes it to the log file upon on "log file format". As the log file reader ensure we can get a complete record from the log file format, we don't need to serialize the byte length of the WriteBatch into record. The serialize structure of WriteBatch:

buffer(WriteBatch)
Bits                | Name              | Description.          |
--------------------|-------------------|-----------------------|
0:32                | WriteBatch version| Write batch version   |
32:N                | Operations        | A series of operations|

buffer(operate:put)

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:8                 | Write Type       | Write batch operation type |
8:16                | Flag             | The page Flag decide page detach or not       |
16:144              | Page Id          | The combine of namespace id and page id       |
144:272             | Version          | The combine of sequence and epoch             |
272:336             | Ref count        | The page be ref count  |
272:N				| Page entry       | The page entry  |

Page entry

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:32                | Blob File id     | The Blob File Id |
32:96               | Offset           | The Page Entry offset |
96:160              | Size             | The Page Entry size |
160:224             | Checksum         | The Page Entry checksum |
224:288             | Tag         	   | The Page tag |
288:352             | Field offsets length    | The length of field offset      |
352:N 				| Field offsets    | The length field offsets      |

Field offsets 

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:64                | Field Offset       | The field offset |
64:128              | Field Checksum | The field checksum |

buffer(operate:put)

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:8                 | Write Type       | Write batch operation type |
8:16                | Flag             | The page Flag decide page detach or not       |
16:144              | Page Id          | The combine of namespace id and page id       |
144:272             | Version          | The combine of sequence and epoch             |
272:336             | Ref count        | The page be ref count  |
272:N				| Page entry       | The page entry  |

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
