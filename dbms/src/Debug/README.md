# Raft Debug Functions

A bundle of functions used for raft-related debugging that can be directly invoked using storage client commands like:
`DBGInvoke <func>(<arg1>, <arg2>, ...)`

By far these functions mimic stuffs before (inclusive) `kvstore`, and behave as a source of the full raft flow, the rest of which are `TMT context`, `flusher`, `region-partition manager`, `schema syncer` and the destination of the data written `TMT storage`.

NOTE: as `kvstore` keeps evolving towards independent from raft commands, we can replace some of the mocked stuff , i. e. `MockRegionRegistry`, by `kvstore` itself, so that the debug functions will behave exactly as a full raft flow w/o TiKV and raft service (sort of a mocked raft service).

## Basic concepts

To run through a raft flow, we'll start by mocking a TiDB table, given a database name, a table name and a schema, see function `mock_tidb_table` for details.

Before actually inserting or generating data to the mocked TiDB table, at least one region must be created (given a region ID) and assigned to the table, see function `put_region` and `map_region` for details. Note that we chose to expose concept region because we'll explicitly manipulate regions by operations like merge, split and delete.

Once regions are assigned to the table, we can insert or generate data for it, see function `mock_tidb_data` or `mock_tidb_data_batch_insert` for details. This step will kick the full raft flow (after `kvstore` of course) running. Schema will get synced (from the mocked TiDB table to the underlying TMT storage), regions will be assigned to partitions of the TMT storage, data will get flushed from region to parts.

You can now query the data from the table (with the same name as the mocked TiDB table), in normal CH SQL.

## Function list

### mock_tidb_table(database-name, table-name, schema)

Mock a TiDB table with the give database name, table name and schema. Database name and table name are specified using identifier. But for implementation simplicity, schema must be specified using string literal in form of:
`'col1 type1, col2 type2, ...'`.

Note that for implementation simplicity as well, column types are CH types (`Int8/32/64`, `Uint8/32/64`, `String`, etc.). They will be mapped to the actual TiDB column types internally with the column flags correctly set. Besides, one cannot specify primary keys - in other words, the mocked TiDB table will always contain an implicit column `_tidb_rowid` typed `Int64` as primary key.

### put_region(region-id [, database-name, table-name])

Create a region with the given region ID. Optionally, this region can be directly mapped to TiDB table with the given database name and table name.

### map_region(region-id, database-name, table-name)

Map the region with the given region ID to the TiDB table with the given database name and table name. This is useful when you want to mimic the scenario that the data of multiple tables are stored in the same region.

### mock_tidb_data(database-name, table-name, value1, value2, ...)

Mock a row of data composed from the third parameter on and insert to the specified TiDB table. One can specify arbitrary literal column values in the parameter list (starting from the third one), nevertheless the number and the types of the values must match the schema of the table, as required in any databases.

### mock_tidb_data_batch_insert

Mock a batch of rows of data composed from the third parameter on and insert to the specified TiDB table. Parameters are: database_name, table_name, thread_num, flush_num, batch_num, min_strlen, max_strlen.

Each thread will write `thread_num * flush_num` rows into new region.
* If the type of column is `TiDB::CodecFlagCompactBytes`, it's length would be made between `min_strlen` and `max_strlen`. `min_strlen` and `max_strlen` should not be smaller than 8.

## Samples

### Sample 1: Hello Congge

Open storage client CLI, run the following commands:
 - `DBGInvoke mock_tidb_table(db, t, 's String, i Int64')`
 - `DBGInvoke put_region(1, db, t)`
 - `DBGInvoke mock_tidb_data(db, t, 'Hello Congge!', 666)`
 - `select * from db.t`

### Sample 2: TBD
