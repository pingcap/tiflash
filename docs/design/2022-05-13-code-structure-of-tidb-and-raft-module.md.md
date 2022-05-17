# Code structure of TiDB and Raft Module

- Author(s): hzh0425
- Last updated: May. 13, 2022
- Discussion at: [Breakdown the "dbms/src/Storages/Transaction" module · Issue #4646 · pingcap/tiflash (github.com)](https://github.com/pingcap/tiflash/issues/4646)

## Background

The codes under "dbms/src/Storages/Transaction" are some codes we adapt TiFlash with TiDB. After years of development, it become kind of crowded and confusing.

Suggest breakdown "dbms/src/Storages/Transaction” into these modules:

- Some global context for TiFlash management
- Basic structure from TiDB (TableInfo/DatabaseInfo, etc)
- Syncing schema with TiDB
- Decoding data from row to column (Block)
- Raft layer
  - Region info
  - Handling admin command
  - Handling write command
  - Handling apply snapshot/ingest sst
  - Handling learner read

## Proposal

### Code structure overview

```
dbms / src
------ TIDB
    ------ Codec
    ------ Schema
    ------ Other components
------ Raft
    ------ Region
    ------ Other components
```

#### TiDB Module

First, we need to create a new module -- dbms/src/TIDB to store:
- Some data structures present in transaction/tidb.h: TableInfo / DBInfo etc..
- TiFlashContext and ManagedStorages.
- Module Codec: Decoding data from row to column (Block).
- Module Schema: Syncing schema with TiDB.
- Other components.

#### Codec Module

Codec Module is used to place 'Decoding data from row to column and some serialization/encoding related components', including:
- RowCodec
- JsonCodec
- Datum
  - Datum
  - DatumCodec
- SerializationHelper

#### Schema Module

Schema module is used to place 'Syncing schema with TiDB', including:
- SchemaSyncer & TiDBSchemaSyncer
- SchemaSyncSerivce
- SchemaBuilder
- SchemaGetter
- SchemaNameMapper

#### Raft Module

When the previous module is completed, the rest of the components in Transaction are basically related to Raft, so we can rename Transaction to Raft, and add the following folders:
- Raft / Region: Used to place 'Region-related components', such as RegionMeta, RegionData, RegionTable, etc.
- **Do we need to add more folder?**

#### Region Module

Used to store Region-related components, including:
- Region
- RegionBlockReader
- RegionCFDataBase
- RegionData
- RegionManager
- etc …