// Copyright 2025 PingCAP, Inc.
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

#include <Common/Logger_fwd.h>
#include <common/types.h>

namespace Poco::Util
{
class AbstractConfiguration;
} // namespace Poco::Util

namespace DB
{
struct KVStoreConfig
{
    KVStoreConfig();

    void reloadConfig(const Poco::Util::AbstractConfiguration & config, const LoggerPtr & log);

    void debugSetCompactLogConfig(UInt64 rows, UInt64 bytes, UInt64 gap, UInt64 eager_gc_gap)
    {
        region_compact_log_min_rows = rows;
        region_compact_log_min_bytes = bytes;
        region_compact_log_gap = gap;
        region_eager_gc_log_gap = eager_gc_gap;
    }

public: // Flush
    UInt64 regionComactLogMinRows() const { return region_compact_log_min_rows.load(std::memory_order_relaxed); }
    UInt64 regionComactLogMinBytes() const { return region_compact_log_min_bytes.load(std::memory_order_relaxed); }
    UInt64 regionComactLogGap() const { return region_compact_log_gap.load(std::memory_order_relaxed); }
    UInt64 regionEagerGCLogGap() const { return region_eager_gc_log_gap.load(std::memory_order_relaxed); }

public: // Raft Read
    UInt64 batchReadIndexTimeout() const { return batch_read_index_timeout_ms.load(std::memory_order_relaxed); }
    // timeout for wait index (ms). "0" means wait infinitely
    UInt64 waitIndexTimeout() const { return wait_index_timeout_ms.load(std::memory_order_relaxed); }
    void debugSetWaitIndexTimeout(UInt64 timeout)
    {
        return wait_index_timeout_ms.store(timeout, std::memory_order_relaxed);
    }
    UInt64 readIndexWorkerTick() const { return read_index_worker_tick_ms.load(std::memory_order_relaxed); }

    Int64 waitRegionReadyTick() const { return wait_region_ready_tick.load(std::memory_order_relaxed); }
    Int64 waitRegionReadyTimeout() const { return wait_region_ready_timeout_sec.load(std::memory_order_relaxed); }

private:
    std::atomic<UInt64> region_compact_log_min_rows;
    std::atomic<UInt64> region_compact_log_min_bytes;
    std::atomic<UInt64> region_compact_log_gap;
    // `region_eager_gc_log_gap` is checked after each write command applied,
    // It should be large enough to avoid unnecessary flushes and also not
    // too large to control the memory when there are down peers.
    // The 99% of passive flush is 512, so we use it as default value.
    // 0 means eager gc is disabled.
    std::atomic<UInt64> region_eager_gc_log_gap;

    std::atomic<UInt64> batch_read_index_timeout_ms;
    std::atomic<UInt64> wait_index_timeout_ms;
    std::atomic<UInt64> read_index_worker_tick_ms;

    std::atomic<UInt64> wait_region_ready_tick;
    std::atomic<Int64> wait_region_ready_timeout_sec;
};

} // namespace DB
