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

#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/S3/S3ReadLimiter.h>

#include <algorithm>
#include <ext/scope_guard.h>

namespace CurrentMetrics
{
extern const Metric S3ActiveGetObjectStreams;
} // namespace CurrentMetrics

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

S3ReadLimiter::StreamToken::~StreamToken()
{
    reset();
}

void S3ReadLimiter::StreamToken::reset()
{
    if (owner == nullptr)
        return;
    owner->releaseStream();
    owner = nullptr;
}

S3ReadLimiter::S3ReadLimiter(UInt64 max_read_bytes_per_sec_, UInt64 max_streams_, UInt64 refill_period_ms_)
    : refill_period_ms(refill_period_ms_)
    , max_read_bytes_per_sec(max_read_bytes_per_sec_)
    , max_streams(max_streams_)
    , active_streams(0)
    , available_bytes(static_cast<double>(burstBytesPerPeriod(max_read_bytes_per_sec_)))
    , last_refill_time(Clock::now())
    , stop(false)
    , log(Logger::get("S3ReadLimiter"))
{
    GET_METRIC(tiflash_storage_io_limiter_curr, type_s3_read_bytes).Set(max_read_bytes_per_sec_);
    GET_METRIC(tiflash_storage_s3_read_limiter_status, type_max_get_object_streams).Set(max_streams_);
    GET_METRIC(tiflash_storage_s3_read_limiter_status, type_active_get_object_streams).Set(0);
}

S3ReadLimiter::~S3ReadLimiter()
{
    setStop();
}

void S3ReadLimiter::updateConfig(UInt64 max_read_bytes_per_sec_, UInt64 max_streams_)
{
    {
        std::lock_guard lock(bytes_mutex);
        max_read_bytes_per_sec.store(max_read_bytes_per_sec_, std::memory_order_relaxed);
        available_bytes = std::min(available_bytes, static_cast<double>(burstBytesPerPeriod(max_read_bytes_per_sec_)));
        if (max_read_bytes_per_sec_ == 0)
            available_bytes = 0;
        last_refill_time = Clock::now();
    }
    {
        std::lock_guard lock(stream_mutex);
        max_streams.store(max_streams_, std::memory_order_relaxed);
    }
    GET_METRIC(tiflash_storage_io_limiter_curr, type_s3_read_bytes).Set(max_read_bytes_per_sec_);
    GET_METRIC(tiflash_storage_s3_read_limiter_status, type_max_get_object_streams).Set(max_streams_);
    bytes_cv.notify_all();
    stream_cv.notify_all();
}

std::unique_ptr<S3ReadLimiter::StreamToken> S3ReadLimiter::acquireStream()
{
    const auto limit = max_streams.load(std::memory_order_relaxed);
    if (limit == 0)
        return nullptr;

    Stopwatch sw;
    bool waited = false;
    std::unique_lock lock(stream_mutex);
    // A token is held for the whole lifetime of one `GetObject` body, not just the initial request.
    while (!stop && max_streams.load(std::memory_order_relaxed) != 0
           && active_streams.load(std::memory_order_relaxed) >= max_streams.load(std::memory_order_relaxed))
    {
        if (!waited)
        {
            GET_METRIC(tiflash_storage_io_limiter_pending_count, type_s3_read_stream).Increment();
            waited = true;
        }
        stream_cv.wait(lock);
    }

    recordWaitIfNeeded(waited, sw, [](double seconds) {
        GET_METRIC(tiflash_storage_io_limiter_pending_seconds, type_s3_read_stream).Observe(seconds);
    });

    if (stop || max_streams.load(std::memory_order_relaxed) == 0)
        return nullptr;

    auto cur = active_streams.fetch_add(1, std::memory_order_relaxed) + 1;
    CurrentMetrics::add(CurrentMetrics::S3ActiveGetObjectStreams);
    GET_METRIC(tiflash_storage_s3_read_limiter_status, type_active_get_object_streams).Set(cur);
    return std::make_unique<StreamToken>(this);
}

void S3ReadLimiter::requestBytes(UInt64 bytes, S3ReadSource source)
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
        if (available_bytes >= static_cast<double>(bytes))
        {
            available_bytes -= static_cast<double>(bytes);
            return;
        }

        if (!waited)
        {
            GET_METRIC(tiflash_storage_io_limiter_pending_count, type_s3_read_byte).Increment();
            waited = true;
        }

        // Sleep only for the missing budget instead of a fixed interval so large readers converge quickly
        // after budget becomes available again.
        const auto missing = static_cast<double>(bytes) - available_bytes;
        const auto wait_us
            = std::max<UInt64>(1, static_cast<UInt64>(missing * 1000000.0 / static_cast<double>(current_limit)));
        bytes_cv.wait_for(lock, std::chrono::microseconds(wait_us));
    }
}

UInt64 S3ReadLimiter::getSuggestedChunkSize(UInt64 preferred_chunk_size) const
{
    const auto limit = max_read_bytes_per_sec.load(std::memory_order_relaxed);
    if (limit == 0)
        return preferred_chunk_size;
    return std::max<UInt64>(1, std::min(preferred_chunk_size, burstBytesPerPeriod(limit)));
}

void S3ReadLimiter::setStop()
{
    {
        std::lock_guard lock_stream(stream_mutex);
        std::lock_guard lock_bytes(bytes_mutex);
        if (stop)
            return;
        stop = true;
    }
    stream_cv.notify_all();
    bytes_cv.notify_all();
}

void S3ReadLimiter::releaseStream()
{
    auto cur = active_streams.fetch_sub(1, std::memory_order_relaxed) - 1;
    CurrentMetrics::sub(CurrentMetrics::S3ActiveGetObjectStreams);
    GET_METRIC(tiflash_storage_s3_read_limiter_status, type_active_get_object_streams).Set(cur);
    stream_cv.notify_one();
}

void S3ReadLimiter::refillBytesLocked(Clock::time_point now)
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

UInt64 S3ReadLimiter::burstBytesPerPeriod(UInt64 max_read_bytes_per_sec_) const
{
    if (max_read_bytes_per_sec_ == 0)
        return 0;
    return std::max<UInt64>(1, max_read_bytes_per_sec_ * refill_period_ms / 1000);
}
} // namespace DB::S3
