# TiFlash supports stale read

- Author: [hehechen](https://github.com/hehechen)

## Introduction

This proposal aims to make TiFlash support Stale Read.

## Motivation or Background

Currently TiKV has already supported [stale read](https://docs.pingcap.com/tidb/dev/stale-read), but TiFlash does not.
There are two benefits of TiFlash to support stale read:
- Reduces the read/wait index duration during reading. Small queries can run faster.
- Avoid calling read_index API of TiKV leaders, to reduce the impact of OLTP workloads.

## Goals

1. TiFlash supports stale read, including [AS OF TIMESTAMP](https://docs.pingcap.com/tidb/dev/as-of-timestamp), [TIDB_BOUNDED_STALENES](https://docs.pingcap.com/tidb/dev/as-of-timestamp#syntax), [tidb_read_staleness](https://docs.pingcap.com/tidb/dev/tidb-read-staleness), [tidb_external_ts](https://docs.pingcap.com/tidb/dev/tidb-external-ts).
2. Since TiFlash MPP may involve the computation among all TiFlash nodes, supporting [using stale read to read TiFlash data nearby](https://docs.pingcap.com/tidb/dev/three-dc-local-read) is not the goal of this RFC.

## Design
1. Check the [safe ts](https://github.com/pingcap/tiflash/blob/e732eaba68e309a0aec0e443c7f1a0e9368731b3/dbms/src/Storages/Transaction/RegionTable.cpp#L508) before read_index, if the stale read requirements are met (safe ts > read tso), read_index can be skipped.
2. The assumption of the current MPP framework is that start_ts is globally unique and increasing in most cases. But in stale read scenarios, users can specify the timestamp, so the start_ts of different MPP queries issued by TiDB may be repeated. So the MPP framework needs to be modified.
   Specifically, we need to use a globally unique query_id to replace start_ts as the identifier of MPP queries. The query_id consists of three parts of ids:
  - The local time of TiDB when the query is initiated: query_ts.
  - An id incremented in TiDB local memory: local_query_id.
  - ETCD guarantees a globally unique serverID for each TiDB server: server_id.
  
    And The MinTSOScheduler will schedule MPP queries [in the order of query_ts](https://github.com/pingcap/tiflash/blob/e732eaba68e309a0aec0e443c7f1a0e9368731b3/dbms/src/Flash/Mpp/MPPTaskId.cpp#L27) in most cases.
1. TiDB client-go supports to get and cache the min_safe_ts from TiFlash stores.
   client-go will query and cache min_safe_ts from all TiKV and TiFlash stores for all of their local regions periodically. When users use [TIDB_BOUNDED_STALENES](https://docs.pingcap.com/tidb/dev/as-of-timestamp#syntax) or [tidb_read_staleness](https://docs.pingcap.com/tidb/dev/tidb-read-staleness) to specify the stale read time range, TiDB will get min_safe_ts from the client-go cache, and generate the start_ts of the query based on min_safe_ts.