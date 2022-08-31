# TiFlash Support Fast Mode

- Author(s): [Yunyan Hong](http://github.com/hongyunyan)

## Introduction

This RFC makes TiFlash provide a brand new mode called "fast scan", to provide better query performance with a small loss of data consistency and timeliness.

## Motivation or Background

Currently, the query results provided by TiFlash provide the guarantee of snapshot isolation and strong data consistency. Thus, TiFlash provides accurate query results (MVCC filtering will be performed). However, in some AP scenarios, users do not have very high requirements for query accuracy, and can tolerate a certain degree of accuracy loss. Therefore, we propose corresponding modes to support the needs of users in different scenarios. That is, for scenarios that require high-precision query results, we can use our TiFlash Normal mode (default mode) to ensure the accuracy of query results. While for scenarios where query results can tolerate a certain error, and higher query performance is desired, we can use our new TiFlash mode (Fast Scan) to obtain higher query performance.

The capabilities of Fast Scan will be divided into multiple phases for continuous iteration and optimization.
The query results of table scan in the first phase of Fast Scan are as follows:

1. Does not support filtering of multiple versions of data
2. Filter the data of the row where the filter delete operation is located (the del_mark in the row equals 1)

## Product Behavior

We added a session/global variable ```tiflash_fastscan``` in TiDB  to control whether TiFlash enables fast scan. In default ```tiflash_fastscan = false```.

If ```tiflash_fastscan``` is enabled at the session level, all tables in TiFlash in this session will query in fast scan mode. If ```tiflash_fastscan``` is enabled at the global level, in all sessions connected after the global variable is set, all tables in TiFlash will query in fast scan mode.
We can query the variable with the following statement.

```sql
SHOW SESSION VARIABLE LIKE 'tiflash_fastscan'
SHOW GLOBAL VARIABLE LIKE 'tiflash_fastscan'
```

```sql
+------------------+-------+
| Variable_name    | Value |
+------------------+-------+
| tiflash_fastscan | OFF   |
+------------------+-------+
```

## Detailed Design

### Read Data Flow

The TableScan operator in Normal Mode includes the following steps:

1. Read data from Delta + Stable: Establish 3 data streams to read data from the MemTableSet and PersistedFileSet of the Delta layer, as well as the Stable layer.
2. Sort Merge: Merge the 3 data streams established in step 1, and return the data in the increasing order of (handle, version).
3. Handle Filter: Filter the data according to the requested handle ranges.
4. MVCC + Column Filter: filter the data by MVCC according to version and del_mark, and filter out the unnecessary internal columns to return the data.

Considering that MVCC filtering is a time-consuming step, but the impact on statistical results is limited, in Fast Mode, we propose omitting the MVCC filtering step to speed up read operations. At the same time, because we omit the MVCC step, we also do not need to merge the data of the delta layer and the stable layer and return them in the order of (handle, version).

Therefore, we can simplify the TableScan process in fast mode to (omit step 2 and MVCC filtering in step 4 of the Normal Mode)

1. Read data from Delta + Stable: Establish three data streams to read data from the MemTableSet and PersistedFileSet of the Delta layer, as well as the Stable layer.
2. Handle Filter: Filter the data according to the data range.
3. Delete Filter: Filter the data according to del_mark (filter out the row with del_mark = 1), and filter out the unnecessary columns to return the data.
4. Merge : Merge and return the above data streams directly (no need to merge in order, just concat)

### Optimization for Read on Handle Column, Version Column and Tag Column

In Fast Mode, we do not perform the filtering operation of MVCC, so we don't need read the version column data. Besides, we also don't need to read the tag column when the pack with no delete rows. Since we do not need to filter the MVCC after merging the delta layer data and the stable layer data, the handle column will only be used for range filtering during the reading process. Thus we optimize the reading of handle column. In the stable layer data, if the entire data range of the pack is totally in the  data range that needs to be read, we do not read the handle column of the pack, thereby reducing IO overhead.

