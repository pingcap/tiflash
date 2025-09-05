// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <common/defines.h>
#include <common/types.h>

#define DBMS_DEFAULT_HOST "localhost"
#define DBMS_DEFAULT_PORT 9000
#define DBMS_DEFAULT_SECURE_PORT 9440
#define DBMS_DEFAULT_CONNECT_TIMEOUT_SEC 10
#define DBMS_DEFAULT_CONNECT_TIMEOUT_WITH_FAILOVER_MS 50
#define DBMS_DEFAULT_SEND_TIMEOUT_SEC 300
#define DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC 300
/// Timeout for synchronous request-result protocol call (like Ping or TablesStatus).
#define DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC 5
#define DBMS_DEFAULT_POLL_INTERVAL 10

/// The size of the I/O buffer by default.
#define DBMS_DEFAULT_BUFFER_SIZE 1048576ULL

/// When writing data, a buffer of `max_compress_block_size` size is allocated for compression. When the buffer overflows or if into the buffer
/// more or equal data is written than `min_compress_block_size`, then with the next mark, the data will also compressed
/// As a result, for small columns (numbers 1-8 bytes), with index_granularity = 8192, the block size will be 64 KB.
/// And for large columns (Title - string ~100 bytes), the block size will be ~819 KB. Due to this, the compression ratio almost does not get worse.
#define DEFAULT_MIN_COMPRESS_BLOCK_SIZE 65536
#define DEFAULT_MAX_COMPRESS_BLOCK_SIZE 1048576

#define DEFAULT_MAX_READ_TSO 0xFFFFFFFFFFFFFFFF
#define DEFAULT_UNSPECIFIED_SCHEMA_VERSION (-1)

// Timeout for building one disagg task in the TiFlash write node.
// Including read index / wait index / generate segments snapshots.
static constexpr UInt64 DEFAULT_DISAGG_TASK_BUILD_TIMEOUT_SEC = 60;
// Timeout for how long one disagg task is valid in the TiFlash write node.
// It is now a short period to avoid long stale snapshots causing system
// instable.
static constexpr UInt64 DEFAULT_DISAGG_TASK_TIMEOUT_SEC = 5 * 60;
// Timeout for FetchDisaggPages in the TiFlash compute node.
static constexpr UInt64 DEFAULT_DISAGG_FETCH_PAGES_TIMEOUT_SEC = 30;

#define DEFAULT_DAG_RECORDS_PER_CHUNK 1024L
#define DEFAULT_BATCH_SEND_MIN_LIMIT (-1)

/** Which blocks by default read the data (by number of rows).
  * Smaller values give better cache locality, less consumption of RAM, but more overhead to process the query.
  */
#define DEFAULT_BLOCK_SIZE 65536

constexpr size_t DEFAULT_BLOCK_BYTES = DEFAULT_BLOCK_SIZE * 256; // 256B per row, total 16MB.

/** Which blocks should be formed for insertion into the table, if we control the formation of blocks.
  * (Sometimes the blocks are inserted exactly such blocks that have been read / transmitted from the outside, and this parameter does not affect their size.)
  * More than DEFAULT_BLOCK_SIZE, because in some tables a block of data on the disk is created for each block (quite a big thing),
  *  and if the parts were small, then it would be costly then to combine them.
  */
#define DEFAULT_INSERT_BLOCK_SIZE 1048576

/** The same, but for merge operations. Less DEFAULT_BLOCK_SIZE for saving RAM (since all the columns are read).
  * Significantly less, since there are 10-way mergers.
  */
#define DEFAULT_MERGE_BLOCK_SIZE 8192

#define DEFAULT_MAX_QUERY_SIZE 262144
#define SHOW_CHARS_ON_SYNTAX_ERROR ptrdiff_t(160)
#define DEFAULT_MAX_DISTRIBUTED_CONNECTIONS 1024
#define DEFAULT_INTERACTIVE_DELAY 100000
#define DBMS_DEFAULT_DISTRIBUTED_CONNECTIONS_POOL_SIZE 1024
#define DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES 3
/// each period reduces the error counter by 2 times
/// too short a period can cause errors to disappear immediately after creation.
#define DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD (2 * DBMS_DEFAULT_SEND_TIMEOUT_SEC)
#define DEFAULT_QUERIES_QUEUE_WAIT_TIME_MS 5000 /// Maximum waiting time in the request queue.
#define DBMS_DEFAULT_BACKGROUND_POOL_SIZE 16

/// Version of ClickHouse TCP protocol. Set to git tag with latest protocol change.
#define DBMS_TCP_PROTOCOL_VERSION 54226

/// The boundary on which the blocks for asynchronous file operations should be aligned.
#define DEFAULT_AIO_FILE_BLOCK_SIZE 4096

#define DEFAULT_QUERY_LOG_FLUSH_INTERVAL_MILLISECONDS 7500

#define PLATFORM_NOT_SUPPORTED "The only supported platforms are x86_64 and AArch64 (work in progress)"

#define DEFAULT_MARK_CACHE_SIZE (1ULL * 1024 * 1024 * 1024)

#define DEFAULT_METRICS_PORT 8234

#if !defined(__x86_64__) && !defined(__aarch64__)
//    #error PLATFORM_NOT_SUPPORTED
#endif
