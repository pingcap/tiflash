# Reduce raft wait index in random write

- Author: [Rongzhen Luo](https://github.com/CalvinNeo)

## Introduction

This RFC introduces a new flush condition for every Region in TiFlash's KVStore, which will be triggered when the count of applied raft entries since the Region's last flush time has reached a certain threshold.

Meanwhile, we will also deprecate the flush condition based on random timeout.

## Background

After received a CompactLog command, there are three conditions that determine whether an actual flush will take place. Two of them are associated with the size of the actual data in the Region. When the number of rows or the size of the data in the Region exceeds the corresponding threshold, flushing to disk will be triggered. The other condition is a random timeout. If the time since the last flush exceeds this timeout, a flush will be performed.

In scenarios where small transactions are replicated, the first two size-related conditions are not easily triggered. The flush frequency is determined only by the random timeout. Furthermore, if the writes are highly random, when a flush is triggered, only a small amount of data is to be written, which is a waste to our write bandwidth.

Therefore, we need to replace this random timeout condition.

## Detailed Design

When flushing each Region, the current `applied_index` is recorded into `last_flushed_applied_index`. Different from using random timeout, after the flush operation triggered by Split/Merge commands, `last_flushed_applied_index` also needs to be updated. After receiving the CompactLog command, the difference between the corresponding `applied_index` and `last_flushed_applied_index` is checked to see if it exceeds the log gap threshold configured. If it does, a flush is triggered; otherwise, the CompactLog is treated as an empty raft log entry.

After TiFlash restarts, the `applied_index` recorded in the disk will be stored into `last_flushed_applied_index`. Because after restarted, TiFlash needs to catch up on logs from TiKV, which could result in a significant number of active Regions even for highly random writes. Therefore, after startup, the threshold for triggering a flush based on log gap will be reduced to half.

In the current architecture, reducing the frequency of flushes often increases memory overhead. This is because we need to cache more entries in the raft entry cache and more KV data in the KVStore. Choosing `applied_index` can more accurately reflect the size of memory, thus avoiding additional flushes.

The original random timeout based configuration will be ignored after upgrading.