# TiFlash Support Fast Mode

- Author(s): [Yunyan Hong](http://github.com/hongyunyan)

## Introduction

This RFC adds a brand new feature for TiFlash called "fast scan mode", providing better query performance with a small loss of data consistency and timeliness.

## Motivation or Background

Currently, TiFlash guarantees snapshot isolation and strong data consistency, i.e. MVCC filtering will be performed. However, in some AP scenarios, users do not have very high requirements for query accuracy, and can tolerate a certain degree of accuracy loss. Therefore, we propose corresponding scan modes to support different user scenarios. That is, for scenarios that require high-precision query results (by default), user can use the normal scan mode. For scenarios where user can tolerate a certain error in the query results, user can use the new fast scan mode to obtain higher query performance.

The capabilities of Fast Scan will be divided into multiple phases for continuous iteration and optimization.
The query results of table scan in the first phase of Fast Scan are as follows:

- Multiple versions of the row will be all preserved and not filtered out
- Deleted versions will be filtered out

## Product Behavior

We will add a session / global variable `tiflash_fastscan` in TiDB to control whether to enable fast scan during the query. By default `tiflash_fastscan = false`.

If `tiflash_fastscan` is enabled in the session, all tables in TiFlash in this session will be queried in fast scan mode. If `tiflash_fastscan` is enabled globally, in all new sessions after the global variable is set, all tables in TiFlash will be queried in fast scan mode.

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

### Simplified Read Data Flow

Currently, the TableScan process in the Normal Scan Mode includes the following steps:

1. Read data from Delta + Stable: Establish 3 data streams to read data from the MemTableSet and PersistedFileSet of the Delta layer, as well as the Stable layer.
2. Sort Merge: Merge the 3 data streams established in step 1, and return the data in the increasing order of (handle, version).
3. Handle Filter: Filter the data according to the requested handle ranges.
4. MVCC + Column Filter: filter the data by MVCC according to version and del_mark, and filter out the unnecessary internal columns to return the data.

MVCC filtering is a time-consuming step. In the Fast Scan Mode, **the MVCC filtering step will be omitted** to speed up read operations. As a result, we also **omit the step 2** above, i.e. no need to merge the data of the delta layer and the stable layer which returns in the order of (handle, version).

In conclusion, in the Fast Scan Mode, the TableScan process is as follows:

1. Read data from Delta + Stable: Establish three data streams to read data from the MemTableSet and PersistedFileSet of the Delta layer, as well as the Stable layer.
2. Handle Filter: Filter the data according to the data range.
3. Delete Filter: Filter the data according to del_mark (filter out the row with del_mark = 1), and filter out the unnecessary columns to return the data.
4. Concat: Merge and return the above data streams directly (no need to merge in order, just concatenation).

### Optimization for Read on Handle Column, Version Column and Tag Column

In Fast Mode, we do not perform the filtering operation of MVCC, so we don't need read the version column data. Besides, we also don't need to read the tag column when the pack with no delete rows. Since we do not need to filter the MVCC after merging the delta layer data and the stable layer data, the handle column will only be used for range filtering during the reading process. Thus we optimize the reading of handle column. In the stable layer data, if the entire data range of the pack is totally in the  data range that needs to be read, we do not read the handle column of the pack, thereby reducing IO overhead.
