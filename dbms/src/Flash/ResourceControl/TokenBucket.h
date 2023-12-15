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
#include <common/logger_useful.h>

#include <cassert>
#include <chrono>
#include <memory>

namespace DB
{

// There are two mode of TokenBucket:
// 1. fill_rate == 0: Bucket is static, will not fill tokens itself.
//                    When the number of tokens is insufficient, will retrieve from the GAC.
// 2. fill_rate > 0: Bucket is dynamic. Will serve as a local token bucket.
// NOTE: not thread safe!
class TokenBucket final
{
public:
    using TimePoint = std::chrono::steady_clock::time_point;

    TokenBucket(
        double fill_rate_,
        double init_tokens_,
        const std::string & log_id,
        double capacity_ = std::numeric_limits<double>::max())
        : fill_rate(fill_rate_)
        , fill_rate_ms(fill_rate_ / 1000)
        , tokens(init_tokens_)
        , capacity(capacity_)
        , last_compact_timepoint(std::chrono::steady_clock::now())
        , low_token_threshold(LOW_TOKEN_THRESHOLD_RATE * capacity_)
        , log(Logger::get(log_id))
    {}

    ~TokenBucket() = default;

    struct TokenBucketConfig
    {
        TokenBucketConfig()
            : tokens(0.0)
            , fill_rate(0.0)
            , capacity(0.0)
        {}

        TokenBucketConfig(double tokens_, double fill_rate_, double capacity_)
            : tokens(tokens_)
            , fill_rate(fill_rate_)
            , capacity(capacity_)
        {}

        double tokens;
        double fill_rate;
        double capacity;
    };

    // Put n tokens into bucket.
    void put(double n);

    bool consume(double n);

    // Return current tokens count.
    double peek() const { return peek(std::chrono::steady_clock::now()); }

    double peek(const TimePoint & timepoint) const;

    void reConfig(const TokenBucketConfig & config);

    TokenBucketConfig getConfig(const std::chrono::steady_clock::time_point & tp = std::chrono::steady_clock::now())
    {
        compact(tp);
        return {tokens, fill_rate, capacity};
    }

    bool lowToken() const { return peek() <= low_token_threshold; }

    bool isStatic() const { return fill_rate == 0.0; }

    std::string toString() const
    {
        return fmt::format("tokens: {}, fill_rate: {}, capacity: {}", tokens, fill_rate, capacity);
    }

    uint64_t estWaitDuraMS(uint64_t max_wait_dura_ms) const
    {
        const auto tokens = peek();
        static const uint64_t min_wait_dura_ms = 100;
        assert(max_wait_dura_ms > min_wait_dura_ms);

        if (tokens >= 0 || fill_rate_ms == 0.0)
            return min_wait_dura_ms;
        const auto est_dura_ms = static_cast<uint64_t>(std::ceil(-tokens / fill_rate_ms)) + min_wait_dura_ms;
        return std::min(est_dura_ms, max_wait_dura_ms);
    }

private:
    static constexpr auto LOW_TOKEN_THRESHOLD_RATE = 0.3;
    static constexpr auto MIN_COMPACT_INTERVAL = std::chrono::milliseconds(10);

    // Merge dynamic token into static token.
    void compact(const TokenBucket::TimePoint & timepoint);
    double getDynamicTokens(const TimePoint & timepoint) const;

    double fill_rate;
    double fill_rate_ms;
    double tokens;
    double capacity;

    TimePoint last_compact_timepoint;

    double low_token_threshold;

    LoggerPtr log;
};

using TokenBucketPtr = std::unique_ptr<TokenBucket>;
} // namespace DB
