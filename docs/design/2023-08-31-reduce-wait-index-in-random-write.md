# Reduce raft wait index in random write

- Author: [Rongzhen Luo](https://github.com/CalvinNeo)

## Introduction

This RFC introduces a new flush condition for every Region in TiFlash's KVStore, which will be triggered when the count of applied raft entries since the Region's last flush time has reached a certain threshold.

Meanwhile, we will also deprecate the flush condition based on random timeout.

## Background

After receiving a CompactLog command, there are three conditions that determine whether an actual flush will take place: 

1. The number of rows in the Region exceeds the corresponding threshold.
2. The size of the data in the Region exceeds the corresponding threshold.
3. The time since the last flush exceeds a random timeout.

In scenarios where small transactions are replicated, the first two size-related conditions can not be easily triggered. The flush frequency is only determined by the random timeout. Furthermore, if the writes are highly random, when a flush is triggered, only a small amount of data in a Region is to be written, which is a waste of write bandwidth.

Therefore, we need to replace this random timeout condition.

## Detailed Design

When flushing each Region, the current `applied_index` is recorded as `last_flushed_applied_index`. Each time the flush operation is triggered by Split/Merge commands, `last_flushed_applied_index` will be updated. Different from using a random timeout, after receiving the CompactLog command, the difference between the corresponding `applied_index` and `last_flushed_applied_index` is checked to see if it exceeds the log gap threshold configured. If it does, a flush is triggered; otherwise, the CompactLog is treated as an empty raft log entry.

After TiFlash restarts, the `applied_index` recorded in the disk will be stored into `last_flushed_applied_index`. And because TiFlash needs to catch up on logs from TiKV after restarting, which could result in a significant number of active Regions even for highly random writes. Therefore, after startup, the threshold for triggering a flush based on the log gap will be reduced to half.

In the current architecture, reducing the frequency of flushes often increases memory overhead. This is because we need to cache more entries in the raft entry cache and more KV data in the KVStore. Instead of the random timeout mechanism, the gap from `last_flushed_applied_index` can more accurately reflect the memory overhead, thus avoiding additional flushes.

The original random timeout based configuration will be ignored after upgrading.