# Architecture Of Storage Engine - PageStorage

- Authors(order by last name): [JaySon-Huang](https://github.com/JaySon-Huang), [flowbehappy](https://github.com/flowbehappy), [Jiaqi Zhou](https://github.com/jiaqizho)

## Introduction

PageStorage is one of the earliest components used in delta merge storage. In the early days of the TIFlash, PageStorage was used as the basic storage moudle, All of IO from DT will store into PageStorage. Over time, we only place elements of `delta` and `metadata of DT` in PageStorage.

PageStorage needs to handle a large amount of delta data read and write, this part of the data is hot data. And a small amount of DT meta information needs to be persisted, this part is cold data. The data of delta  will eventually be merged into the stable part, eq. stored in the DT file.

You can see the picture below. Described in the picture is the design of the entire DT. PageStorage carries the traffic of the upper half of DT.

![tiflash-dt-architecture](./images/tiflash-dt-architecture.png)


As one of the important components of DMS, PageStorage mainly provides a KV storage service which also support MVCC. Unlike other KV services, the KV interface provided by PageStorage is limited. Key is limited to uint64_t, Value is limited to a buffer or a array of buffer(we called it fields) or null.


## Capability

PageStorage supported: 

- Disk-based store
- Write/Read operation atomicity
- Full MVCC function 
- KV store function
- GC


## Design

Currently there are three versions of PageStorage (V1, V2, V3). The V1 version is no longer used by DT. Due to compatibility considerations, it's still used in some components, we considered to remove it the future. It won't be covered too much in this article.

At present, most of our customers use the V2 version, which is also the version with the longest service time. Due to design issues, the V2 version has some problems that cannot be improved(will be introduced below), so we propose the V3 version.

The V3 version has been proposed for a period of time, and it was officially used by customers in TiFlash v6.1.0. This version has improved the shortcomings of V2 very well. The redesign has made the module very good in terms of performance and maturity. 

### 1. V2 version

![tiflash-ps-v2-architecture](./images/tiflash-ps-v2-architecture.png)

The picture below describes the design in PageStorage V2.
We provider a big PageMap which stored data and meta of pages. Also provider Writable/Readable pagefile to support write/read of page.

Here is the basic elements in PageStorage V2:

- WriteBatch: a batch of write
   - upper layer used this struct to write a batch of buffer into PageStorage.
- Page: basic data elements
   - it contains id, buffer, fields...
   - A set of pages will as return value when upper layer read by a set of page id.
- PageFile: contains meta and data
   - data: buffer will store into the this part
   - meta: used to manager the data part record every data of page.
- PageFileReader: used to read a single pagefile.
- PageFileWriter: used to write a single pagefile.
- PageEntriesVersionSetWithDelta: The MVCC object


#### meta and data

Pagefile is a directory on the filesystem which is named with type_id_level. For example page_55_1, “page” means that pagefile type is “formal”, “55” is the pagefile id, “1” is the level.

Pagefile is composed of multiple meta buffers and multiple data buffers. Meta buffer keep the pagefile meta info,  it contains multiple write batch buffer. 

Here is the meta sturct:

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:32                | Meta Byte Size   | The buffer length     |
32:64               | version          | The page format version, should be V1 or V2 |
64:128               | SequenceID          | The ID of sequence | 
128:N               | Buffer list          | Collection of meta buffer, length is flexible | 
N:+64               | Checksum          | Checksum | 

A write batch can contain a sequence of operations(put/upsert/del/ref), so a buffer list is a flexible list to record that. 

buffer list(operate:put/upsert)

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:8                 | Write Type       | Write batch operation type |
8:72                | Page ID          | The page Id           |
72:136              | File ID          | The pagefile Id       |
136:168             | Level            | The page level        |
168:200             | Flag             | The page Flag decide page detach or not       |
200:264             | Page offset      | The page offset which record data offset in current pagefile |
264:328             | Page size        | The page size which record data size        |
328:392             | Page checksum    | The page checksum        |
392:456             | Field offsets length    | The length of field offset      |

buffer list(operate:del)


Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:8                 | Write Type       | Write batch operation type |
8:72                | Page ID  		  | The page Id           |

buffer list(op:ref)

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:8                 | Write Type       | Write batch operation type |
8:72                | Page ID  		  | The page Id           |
72:136              | Origin Page ID   | The Origin page id    |

Field offsets 

Bits                | Name             | Description.          |
--------------------|------------------|-----------------------|
0:64                | Field Offset       | The field offset |
64:128              | Field Checksum | The field checksum |

The data part is more simple, it have not contain any of meta info, it only consists of buffer.

Both of data and meta, we used append write + sync IO to write the file, data clean will be done by GC part. Also the file size of both is limited. If the data is larger than 128M(default), a new file will be automatically generated.


#### MVCC

`PageEntriesVersionSetWithDelta` is the main class of the MVCC, It's a big list, collect all version of page entries(it's a extension of Page object), The entries contain with every single page. Once snapshot created, the list will add a new version of page entries.

The API from PageStorage have a method named `getSnapshot()`. After call this method, MVCC will generate a snapshot which record all of page entries(in memory) in current time.

Then every read API from PageStorage request the snapshot as one of the parameters.But by default, the snapshot can be nullptr, then we will create a new snapshot for it.

In read method, PageStorage will use snapshot + page ids to find the page entries. After we got the entries, we will read the data buffer from pagefile and combine it to a set of page, Then return to the caller.

Also anytime there is a del operation come, we will not immediately mark the data on the disk to be cleaned up. Instead GC part will do the clean job.

#### GC

GC relative the snapshots release. If none of the snapshots in memory are released, then the GC will not clean up any data. In general, snapshots are released every once in a while, This will generate a lot of "expired" data on disk.

At each round of GC, the GC thread scans each pagefiles and calculates the valid rate of the pagefile through MVCC. valid rate equal to total valid size divide by total file size, If valid rate lower than 0.5(by default), we will do the compact GC. 

The compact GC will compact the valid page into a new pagefile. This process is in order not to generate spatial magnification.But the same, it will produce write amplification.

During the GC process, we do not lock the entire PageStorage, but use more fine-grained locks to ensure that the current PageStorage can respond to read and write.

#### write/read example

After write request comes:

- Get a idle PageFileWriter(which is not locked), and locked it to avoid anther thread write. 
- Generate the meta record.
- Write buffer into the disk.
- Write the meta record into meta.

After read request comes:

- Get a snapshot from caller or create a snapshot.
- Find the entries from the MVCC by snapshot + page ids.
- Using entries, read from disk.
- Combine the buffer and entries into pages and then return to the caller.

### 2. V3 version

After our customers used TiFlash, we found some problems with the V2 version in the actual production environment.

1. **There is a risk of data loss in the meta part**. The probability of this kind of risk happening is very small, and we have never encountered it. But in theory, as long as the checksum and buffer size fields in a single meta buffer are damaged at the same time, the subsequent buffers will be unavailable.
2. **The snapshot of MVCC needs to be optimized**. First, the memory occupied by the snapshot can be reduced, and secondly, we do not need such a complex structure to implement MVCC.
3. **The GC write amplification in the data part is too severe, The GC task is too heavy**. Since the data part is composed of append write, compact gc is frequently triggered, and the read and write volume of each gc will be large.

Besides these three problem, we also changed lock mode and CRC implements, make the V3 better than V2.

![tiflash-ps-v3-architecture](./images/tiflash-ps-v3-architecture.png)

The V3 version of PageStorage consists of two main components, one is WALStore and the other is BlobStore.

- WALStore(Write Ahead Log Store): Using the write ahead log file format to manager the meta part.
- PageDirectory: Provides the function of MVCC. Smaller memory usage and faster speed.
- BlobStore: Provides an address space management. Using the address multiplexing to manage the data part.

#### WALStore

WALStore using the fixed block space to manager the meta.

![tiflash-ps-v3-walstore](./images/tiflash-ps-v3-wal-store.png)

As you can see, we fixed the meta into one block in V3.
- A block contains multiple meta record.
- If a block have some spacemap remain, But it can't insert a meta. Then using a padding to full it.

If crc and meta size are broken at the same time. It is only necessary to discard the data in a single block, and the block before or after will be preserved. This is because we will read meta in by single block, single block error won't effect others. This is also more convenient to troubleshoot the cause of meta errors.

WALStore provider two main interfaces

- **apply**: after apply, the meta info will be will be serialized to disk.This happens after writing data.
- **read**: Read serialized meta info from disk. This will happen after pagestorage restore.

In addition, meta also needs to perform gc. Although the overall read/write sizes is not large, But it will also sort out some invalid meta information.

The sturct of meta similar with V2, but still have some differents.

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

The page id in V3 is is a 128bit unsigned int replace the 64bit. That is because instead of generating an instance of PageStorage for each table, there are only four instances of PageStorage globally(Type log/Type data/Type meta/KVStore). So we need additional 64bit space (eq. namespace id) to distinguish different tables.

Multi of meta info will conbime into a block. So it is different with V2, we added a WAL format to pack the meta struct. 

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

- Log number: 32bit log file number, so that we can distinguish between records written by the most recent log writer vs a previous one.


#### PageDirectory

PageDirectory supports the function of PageDirectory MVCC. Its main component is a map. The key in map is page id and the value in map is the version chian.

The version chain consists of [seq|epoch] and page entry, sequence + epoch determines the position of the page entry in the chain, page entry have similar implementation with V2, but simplifies unnecessary structures.

Here is a PageDirectory example:

```
page id 1 : {[seq 1 + epoch 0, entry 1], [seq 2 + epoch 0, entry 2], [seq 3 + epoch 1, entry 3]}
page id 2 : {[seq 1 + epoch 0, entry 4], [seq 2 + epoch 0, entry 5], [seq 5 + epoch 0, entry 6]}
page id 100 : {[seq 1 + epoch 0, entry 7], [seq 10 + epoch 2, entry 8]}
```

In this example, we have 3 page with differe id. 

- page 1 has 3 different versions corresponding to 3 entries. page 100 has 2 different versions corresponding to 2 entries.
- The seq will increase If we have a corresponding new page written.
- The epoch will be increase If current entry have been compact GC.

`getSnapshot()` in V3 is very different from V2, In V2, we actually generate snapshots, there are some copies of memory. But in V3, Snapshot only contains a sequence id which can filter the right pages from the PageDirectory.

In upper example. If sequence in snapshot is 2 and query page id is 2. Then we will got the entry 5.


#### BlobStore

BlobStore's name is earthy, but apt. It mainly stores Blob, which consists of three parts

1. **BlobFile**: Blob stored file, used to write and read.
2. **BlobStat**: A space manager, one-to-one correspondence with blobfile. used to find/alloc/free address space in BlobFile.
3. **BlobStats**: Manage all BlobStat. Used to schedule all write requests.

Different with V2 design, we decided to cancel append write mode, Instead, a data structure called a space map is used to manage the file address space.

![tiflash-ps-v3-freemap](./images/tiflash-ps-v3-freemap.png)


This idea come form a rb-tree bitmap implements. Every node in rb-tree have a space which conbime with offset + size.But the difference is that bitmap uses rb-tree to record used locations, and we use rb-tree to record free locations.This is because record free locations will better to find space where you can insert.

The spacemap(freemap) inside BlobStat, BlobStat can use it to reuse the reclaimed address space, thereby reducing write amplification. 

In addition, BlobStat also needs to provide external statistical status, such as the valid rate of the current BlobFile, the maximum capacity of the current Space map, and so on. These data must be calculated when inserting and releasing, otherwise we have to traverse the space map to get the data we need.

The statistical status from BlobStat is very necessary and useful. It has two main functions

1. Provide it to BlobStats so that BlobStats can determine whether to create a new Blobfile or reuse the old one.
2. When BlobStats selects BlobFile to write, it will write according to the BlobFile with the lowest valid rate.
3. When GC occurs, it can quickly determine whether the current BlobFile needs to be compact GC.
4. Because the release of the file address space may cause invalid data to be stored at the end of the BlobFile, the truncate operation can also be performed in time during GC to reduce space enlargement

Finally, from the description of BlobStat, it is not difficult to find that BlobStats is used to schedule all write IO. Then write request happend, we will request a free space from BlobStats, Then we can put the buffer into that disk space. These operate all happend in memory, So we can maximize IO parallelization.

#### GC

The GC of V3 will be more complicated, but compared with V2, the GC of V3 is faster. In the test, the IO from V3 GC will be much less.

1. Begin to GC, we need do WALStore GC at first, this part is lightweight. When the meta file recorded by wal reaches a certain level, we will dump it and update it.
2. After that, We need do MVCC GC, we will cleanup the expired snapshot in PageDirectory, also these expired page entry from expired snapshot will be mark clean in BlobStore. This means that we will mark the address space originally occupied by these expired page entries as unused.
3. Some of space in BlobStore be free after MVCC GC, Then BlobStore will check all BlobStat, find out if there is a blob with a low valid rate, and perform compact gc on it.The compact GC simliar with V2, it will copy the valid data ,and store it to other place. 
4. If we do have compact GC happend in BlobStore, Then we need to tell PageDirectory that some data has been migrated and where is the new location.

Although this looks complicated, in practice, the probability of BlobStore triggering full gc is very low, which means that we will not generate too many read and write IO during the GC process.









