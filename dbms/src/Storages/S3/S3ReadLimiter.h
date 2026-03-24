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
#include <memory>
#include <mutex>

namespace DB::S3
{
enum class S3ReadSource
{
    DirectRead,
    FileCacheDownload,
};

class S3ReadLimiter : public std::enable_shared_from_this<S3ReadLimiter>
{
public:
    class StreamToken
    {
    public:
        explicit StreamToken(S3ReadLimiter * owner_)
            : owner(owner_)
        {}

        ~StreamToken();

        StreamToken(const StreamToken &) = delete;
        StreamToken & operator=(const StreamToken &) = delete;

        StreamToken(StreamToken && other) noexcept
            : owner(other.owner)
        {
            other.owner = nullptr;
        }

        StreamToken & operator=(StreamToken && other) noexcept
        {
            if (this == &other)
                return *this;
            reset();
            owner = other.owner;
            other.owner = nullptr;
            return *this;
        }

        void reset();

    private:
        S3ReadLimiter * owner;
    };

    /// A lightweight node-level limiter for S3 remote reads.
    ///
    /// It limits two dimensions together:
    /// - concurrently active `GetObject` body streams
    /// - total remote-read bytes consumed by direct reads and FileCache downloads
    explicit S3ReadLimiter(UInt64 max_read_bytes_per_sec_ = 0, UInt64 max_streams_ = 0, UInt64 refill_period_ms_ = 100);

    ~S3ReadLimiter();

    /// Update both byte-rate and stream limits. `0` disables the corresponding limit.
    void updateConfig(UInt64 max_read_bytes_per_sec_, UInt64 max_streams_);

    /// Acquire a token that must live as long as the `GetObject` body stream remains active.
    /// Returns `nullptr` when the stream limit is disabled.
    [[nodiscard]] std::unique_ptr<StreamToken> acquireStream();

    /// Charge remote-read bytes. The call blocks when the current node-level budget is exhausted.
    void requestBytes(UInt64 bytes, S3ReadSource source);

    /// Suggest a chunk size that keeps limiter-enabled readers from creating large bursts.
    UInt64 getSuggestedChunkSize(UInt64 preferred_chunk_size) const;

    UInt64 maxReadBytesPerSec() const { return max_read_bytes_per_sec.load(std::memory_order_relaxed); }
    UInt64 maxStreams() const { return max_streams.load(std::memory_order_relaxed); }
    UInt64 activeStreams() const { return active_streams.load(std::memory_order_relaxed); }

    void setStop();

private:
    using Clock = std::chrono::steady_clock;

    void releaseStream();
    void refillBytesLocked(Clock::time_point now);
    UInt64 burstBytesPerPeriod(UInt64 max_read_bytes_per_sec_) const;

    const UInt64 refill_period_ms;
    std::atomic<UInt64> max_read_bytes_per_sec;
    std::atomic<UInt64> max_streams;
    std::atomic<UInt64> active_streams;

    mutable std::mutex stream_mutex;
    std::condition_variable stream_cv;

    mutable std::mutex bytes_mutex;
    std::condition_variable bytes_cv;
    double available_bytes;
    Clock::time_point last_refill_time;
    bool stop;

    LoggerPtr log;
};
} // namespace DB::S3
