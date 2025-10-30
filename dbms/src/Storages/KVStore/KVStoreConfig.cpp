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

#include <Common/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Storages/KVStore/KVStoreConfig.h>
#include <common/logger_useful.h>

namespace DB
{
// default config about compact-log: rows 40k, bytes 32MB, gap 200
const UInt64 DEFAULT_COMPACT_LOG_ROWS = 40 * 1024;
const UInt64 DEFAULT_COMPACT_LOG_BYTES = 32 * 1024 * 1024;
const UInt64 DEFAULT_COMPACT_LOG_GAP = 200;
const UInt64 DEFAULT_EAGER_GC_LOG_GAP = 512;

// default batch-read-index timeout is 4_000ms.
const uint64_t DEFAULT_BATCH_READ_INDEX_TIMEOUT_MS = 4 * 1000;
// default wait-index timeout is 5 * 60_000ms.
const uint64_t DEFAULT_WAIT_INDEX_TIMEOUT_MS = 5 * 60 * 1000;

const int64_t DEFAULT_WAIT_REGION_READY_TIMEOUT_SEC = 20 * 60;

const int64_t DEFAULT_READ_INDEX_WORKER_TICK_MS = 10;

KVStoreConfig::KVStoreConfig()
    : region_compact_log_min_rows(DEFAULT_COMPACT_LOG_ROWS)
    , region_compact_log_min_bytes(DEFAULT_COMPACT_LOG_BYTES)
    , region_compact_log_gap(DEFAULT_COMPACT_LOG_GAP)
    , region_eager_gc_log_gap(DEFAULT_EAGER_GC_LOG_GAP)
    , batch_read_index_timeout_ms(DEFAULT_BATCH_READ_INDEX_TIMEOUT_MS)
    , wait_index_timeout_ms(DEFAULT_WAIT_INDEX_TIMEOUT_MS)
    , read_index_worker_tick_ms(DEFAULT_READ_INDEX_WORKER_TICK_MS)
    , wait_region_ready_tick(0)
    , wait_region_ready_timeout_sec(DEFAULT_WAIT_REGION_READY_TIMEOUT_SEC)
{}

void KVStoreConfig::reloadConfig(const Poco::Util::AbstractConfiguration & config, const LoggerPtr & log)
{
    // static constexpr const char * COMPACT_LOG_MIN_PERIOD = "flash.compact_log_min_period"; // deprecated
    static constexpr const char * COMPACT_LOG_MIN_ROWS = "flash.compact_log_min_rows";
    static constexpr const char * COMPACT_LOG_MIN_BYTES = "flash.compact_log_min_bytes";
    static constexpr const char * COMPACT_LOG_MIN_GAP = "flash.compact_log_min_gap";
    static constexpr const char * EAGER_GC_LOG_GAP = "flash.eager_gc_log_gap";

    static constexpr const char * BATCH_READ_INDEX_TIMEOUT_MS = "flash.batch_read_index_timeout_ms";
    static constexpr const char * WAIT_INDEX_TIMEOUT_MS = "flash.wait_index_timeout_ms";
    static constexpr const char * WAIT_REGION_READY_TICK = "flash.wait_region_ready_tick";
    static constexpr const char * WAIT_REGION_READY_TIMEOUT_SEC = "flash.wait_region_ready_timeout_sec";
    static constexpr const char * READ_INDEX_WORKER_TICK_MS = "flash.read_index_worker_tick_ms";

    {
        region_compact_log_min_rows = std::max(config.getUInt64(COMPACT_LOG_MIN_ROWS, DEFAULT_COMPACT_LOG_ROWS), 1);
        region_compact_log_min_bytes = std::max(config.getUInt64(COMPACT_LOG_MIN_BYTES, DEFAULT_COMPACT_LOG_BYTES), 1);
        region_compact_log_gap = std::max(config.getUInt64(COMPACT_LOG_MIN_GAP, DEFAULT_COMPACT_LOG_GAP), 1);
        region_eager_gc_log_gap = config.getUInt64(EAGER_GC_LOG_GAP, DEFAULT_EAGER_GC_LOG_GAP);

        LOG_INFO(
            log,
            "Region compact log thresholds, rows={} bytes={} gap={} eager_gc_gap={}",
            region_compact_log_min_rows,
            region_compact_log_min_bytes,
            region_compact_log_gap,
            region_eager_gc_log_gap);
    }
    {
        batch_read_index_timeout_ms
            = config.getUInt64(BATCH_READ_INDEX_TIMEOUT_MS, DEFAULT_BATCH_READ_INDEX_TIMEOUT_MS);
        wait_index_timeout_ms = config.getUInt64(WAIT_INDEX_TIMEOUT_MS, DEFAULT_WAIT_INDEX_TIMEOUT_MS);
        read_index_worker_tick_ms = config.getUInt64(READ_INDEX_WORKER_TICK_MS, DEFAULT_READ_INDEX_WORKER_TICK_MS);

        wait_region_ready_tick = config.getUInt64(WAIT_REGION_READY_TICK, 0);
        wait_region_ready_timeout_sec = ({
            int64_t t = config.getInt64(WAIT_REGION_READY_TIMEOUT_SEC, /*20min*/ DEFAULT_WAIT_REGION_READY_TIMEOUT_SEC);
            t = t >= 0 ? t : std::numeric_limits<int64_t>::max(); // set -1 to wait infinitely
            t;
        });

        LOG_INFO(
            log,
            "read-index timeout: {}ms; wait-index timeout: {}ms; wait-region-ready timeout: {}s; "
            "read-index-worker-tick: {}ms",
            batchReadIndexTimeout(),
            waitIndexTimeout(),
            waitRegionReadyTimeout(),
            readIndexWorkerTick());
    }
}

} // namespace DB
