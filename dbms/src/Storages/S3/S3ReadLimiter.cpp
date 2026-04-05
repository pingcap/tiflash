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

#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/S3/S3ReadLimiter.h>

#include <algorithm>
#include <ext/scope_guard.h>

namespace DB::S3
{
namespace
{
// We only emit wait metrics after the call actually blocked, so the hot path keeps the zero-wait case cheap.
template <typename F>
void recordWaitIfNeeded(bool waited, const Stopwatch & sw, F && observe)
{
    if (!waited)
        return;
    observe(sw.elapsedSeconds());
}
} // namespace

void DB::S3::S3ReadMetricsRecorder::recordBytes(UInt64 bytes, S3ReadSource source) const
{
    if (bytes == 0)
        return;

    switch (source)
    {
    case S3ReadSource::DirectRead:
        GET_METRIC(tiflash_storage_io_limiter, type_s3_direct_read_bytes).Increment(bytes);
        break;
    case S3ReadSource::FileCacheDownload:
        GET_METRIC(tiflash_storage_io_limiter, type_s3_filecache_download_bytes).Increment(bytes);
        break;
    }
}

DB::S3::S3ReadLimiter::S3ReadLimiter(UInt64 max_read_bytes_per_sec_, UInt64 refill_period_ms_)
    : refill_period_ms(refill_period_ms_)
    , max_read_bytes_per_sec(max_read_bytes_per_sec_)
    , available_bytes(static_cast<double>(burstBytesPerPeriod(max_read_bytes_per_sec_)))
    , last_refill_time(Clock::now())
    , stop(false)
    , log(Logger::get("S3ReadLimiter"))
{
    GET_METRIC(tiflash_storage_io_limiter_curr, type_s3_read_bytes).Set(max_read_bytes_per_sec_);
}

DB::S3::S3ReadLimiter::~S3ReadLimiter()
{
    setStop();
}

void DB::S3::S3ReadLimiter::updateConfig(UInt64 max_read_bytes_per_sec_)
{
    {
        std::lock_guard lock(bytes_mutex);
        max_read_bytes_per_sec.store(max_read_bytes_per_sec_, std::memory_order_relaxed);
        available_bytes = std::min(available_bytes, static_cast<double>(burstBytesPerPeriod(max_read_bytes_per_sec_)));
        if (max_read_bytes_per_sec_ == 0)
            available_bytes = 0;
        last_refill_time = Clock::now();
    }
    GET_METRIC(tiflash_storage_io_limiter_curr, type_s3_read_bytes).Set(max_read_bytes_per_sec_);
    bytes_cv.notify_all();
}

void DB::S3::S3ReadLimiter::requestBytes(UInt64 bytes, S3ReadSource /*source*/)
{
    if (bytes == 0)
        return;

    const auto limit = max_read_bytes_per_sec.load(std::memory_order_relaxed);
    if (limit == 0)
        return;

    Stopwatch sw;
    bool waited = false;
    std::unique_lock lock(bytes_mutex);
    SCOPE_EXIT({
        recordWaitIfNeeded(waited, sw, [](double seconds) {
            GET_METRIC(tiflash_storage_io_limiter_pending_seconds, type_s3_read_byte).Observe(seconds);
        });
    });
    while (!stop)
    {
        const auto current_limit = max_read_bytes_per_sec.load(std::memory_order_relaxed);
        // Config reload can disable the limiter while callers are waiting.
        if (current_limit == 0)
            return;

        const auto now = Clock::now();
        refillBytesLocked(now);
        const auto requested_bytes = static_cast<double>(bytes);
        const auto burst_bytes = static_cast<double>(burstBytesPerPeriod(current_limit));
        if (available_bytes >= requested_bytes)
        {
            available_bytes -= requested_bytes;
            return;
        }

        // Preserve the strict token-bucket behavior for requests that fit into one burst. When one
        // caller asks for more than the bucket can ever accumulate, allow it to borrow once some
        // budget is available so the request still makes forward progress. Upper layers are expected
        // to call getSuggestedChunkSize() and keep this branch rare.
        if (requested_bytes > burst_bytes && available_bytes > 0)
        {
            available_bytes -= requested_bytes;
            return;
        }

        if (!waited)
        {
            GET_METRIC(tiflash_storage_io_limiter_pending_count, type_s3_read_byte).Increment();
            waited = true;
        }

        // Sleep only for the missing budget instead of a fixed interval so large readers converge quickly
        // after budget becomes available again.
        const auto missing = requested_bytes - available_bytes;
        const auto wait_us
            = std::max<UInt64>(1, static_cast<UInt64>(missing * 1000000.0 / static_cast<double>(current_limit)));
        bytes_cv.wait_for(lock, std::chrono::microseconds(wait_us));
    }
}

UInt64 DB::S3::S3ReadLimiter::getSuggestedChunkSize(UInt64 preferred_chunk_size) const
{
    const auto limit = max_read_bytes_per_sec.load(std::memory_order_relaxed);
    if (limit == 0)
        return preferred_chunk_size;
    return std::max<UInt64>(1, std::min(preferred_chunk_size, burstBytesPerPeriod(limit)));
}

void DB::S3::S3ReadLimiter::setStop()
{
    {
        std::lock_guard lock_bytes(bytes_mutex);
        if (stop)
            return;
        stop = true;
    }
    bytes_cv.notify_all();
}

void DB::S3::S3ReadLimiter::refillBytesLocked(Clock::time_point now)
{
    const auto current_limit = max_read_bytes_per_sec.load(std::memory_order_relaxed);
    if (current_limit == 0)
    {
        available_bytes = 0;
        last_refill_time = now;
        return;
    }

    const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - last_refill_time).count();
    if (elapsed_ns <= 0)
        return;

    const auto burst_bytes = static_cast<double>(burstBytesPerPeriod(current_limit));
    // Clamp to one refill-period burst so a temporarily idle reader cannot accumulate an unbounded burst.
    available_bytes = std::min(
        burst_bytes,
        available_bytes + static_cast<double>(current_limit) * static_cast<double>(elapsed_ns) / 1000000000.0);
    last_refill_time = now;
}

UInt64 DB::S3::S3ReadLimiter::burstBytesPerPeriod(UInt64 max_read_bytes_per_sec_) const
{
    if (max_read_bytes_per_sec_ == 0)
        return 0;
    return std::max<UInt64>(1, max_read_bytes_per_sec_ * refill_period_ms / 1000);
}
} // namespace DB::S3
