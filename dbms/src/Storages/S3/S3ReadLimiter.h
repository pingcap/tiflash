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

#include <Common/Logger.h>
#include <common/types.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>

namespace DB::S3
{
enum class S3ReadSource
{
    DirectRead,
    FileCacheDownload,
};

class S3ReadLimiter
{
public:
    /// Stream-based limiting looks attractive because a token could track one live `GetObject`
    /// body stream.
    ///
    /// Important: such a token must not be interpreted as a safe upper bound for the number of
    /// `S3RandomAccessFile` objects. One reader can hold a response body open while being idle in a
    /// pipeline stage, so limiting tokens too aggressively can stall unrelated readers even when
    /// there is little ongoing S3 network I/O.
    ///
    /// Stream-based limiting is therefore removed for now. Keep this note here so future changes do
    /// not accidentally re-introduce the same unsafe hard cap on `S3RandomAccessFile` concurrency.

    /// A lightweight node-level limiter for S3 remote reads.
    ///
    /// It currently limits one dimension:
    /// - total remote-read bytes consumed by direct reads and FileCache downloads
    ///
    /// The stream dimension is best-effort protection against too many live response bodies, not a
    /// replacement for byte throttling and not a safe cap on reader object count. In TiFlash a
    /// `S3RandomAccessFile` may keep its body stream open across scheduling gaps, so a low stream
    /// limit can block forward progress even when the node is no longer transferring many bytes.
    explicit S3ReadLimiter(UInt64 max_read_bytes_per_sec_ = 0, UInt64 refill_period_ms_ = 100);

    ~S3ReadLimiter();

    void updateConfig(UInt64 max_read_bytes_per_sec_);

    /// Charge remote-read bytes. The call blocks when the current node-level budget is exhausted.
    void requestBytes(UInt64 bytes, S3ReadSource source);

    /// Suggest a chunk size that keeps limiter-enabled readers from creating large bursts.
    UInt64 getSuggestedChunkSize(UInt64 preferred_chunk_size) const;

    UInt64 maxReadBytesPerSec() const { return max_read_bytes_per_sec.load(std::memory_order_relaxed); }

    void setStop();

private:
    using Clock = std::chrono::steady_clock;

    /// Refill the token bucket according to elapsed wall time. Caller must hold `bytes_mutex`.
    void refillBytesLocked(Clock::time_point now);
    /// Limit the instantaneous burst so long reads are naturally split into small limiter-aware chunks.
    UInt64 burstBytesPerPeriod(UInt64 max_read_bytes_per_sec_) const;

    const UInt64 refill_period_ms;
    std::atomic<UInt64> max_read_bytes_per_sec;

    mutable std::mutex bytes_mutex;
    std::condition_variable bytes_cv;
    // Token-bucket state for S3 byte throttling.
    double available_bytes;
    Clock::time_point last_refill_time;
    bool stop;

    LoggerPtr log;
};
} // namespace DB::S3
