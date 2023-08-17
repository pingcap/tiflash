// Copyright 2023 PingCAP, Ltd.
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

    TokenBucket(double fill_rate_, double init_tokens_, double capacity_ = std::numeric_limits<double>::max())
        : fill_rate(fill_rate_)
        , tokens(init_tokens_)
        , capacity(capacity_)
        , last_compact_timepoint(std::chrono::steady_clock::now())
        , last_get_avg_speed_timepoint(std::chrono::steady_clock::time_point::min())
        , last_get_avg_speed_tokens(init_tokens_)
        , avg_speed_per_sec(0.0)
        , low_token_threshold(LOW_TOKEN_THRESHOLD_RATE * capacity_)
    {}

    ~TokenBucket() = default;

    // Put n tokens into bucket.
    void put(double n);

    bool consume(double n);

    // Return current tokens count.
    double peek() const { return peek(std::chrono::steady_clock::now()); }

    double peek(const TimePoint & timepoint) const;

    void reConfig(double new_tokens, double new_fill_rate, double new_capacity);

    std::tuple<double, double, double> getCurrentConfig() const { return std::make_tuple(tokens, fill_rate, capacity); }

    double getAvgSpeedPerSec();

    bool lowToken() const { return peek() <= low_token_threshold; }

    bool isStatic() const { return fill_rate == 0.0; }

private:
    static constexpr auto LOW_TOKEN_THRESHOLD_RATE = 0.8;

    // Merge dynamic token into static token.
    void compact(const TokenBucket::TimePoint & timepoint);
    double getDynamicTokens(const TimePoint & timepoint) const;

    double fill_rate;
    double tokens;
    double capacity;

    TimePoint last_compact_timepoint;

    TimePoint last_get_avg_speed_timepoint;
    double last_get_avg_speed_tokens;
    double avg_speed_per_sec;

    double low_token_threshold;
};

using TokenBucketPtr = std::unique_ptr<TokenBucket>;
} // namespace DB
