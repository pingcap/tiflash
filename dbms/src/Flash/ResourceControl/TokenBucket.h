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

#include <Common/Exception.h>
#include <chrono>
#include <memory>

namespace DB
{

// There are two mode of TokenBucket:
// 1. refill_rate == 0: bucket is static, a.k.a stops filling token.
// 2. refill_rate > 0: bucket is dynamic.
// NOTE: not thread safe!
class TokenBucket final
{
public:
    using TimePoint = std::chrono::steady_clock::time_point;

    TokenBucket(double refill_rate_, double init_tokens_, double capacity_ = std::numeric_limits<double>::max())
        : refill_rate(refill_rate_)
        , tokens(init_tokens_)
        , capacity(capacity_) {}

    ~TokenBucket() = default;

    // Put n tokens into bucket.
    void put(double n);

    // Consume tokens in bucket:
    // 1. If has enough tokens, return true and update bucket.
    // 2. Otherwise, return false and dont touch bucket.
    bool consume(double n);

    // Returns current tokens.
    double peek() const
    {
        return peek(std::chrono::steady_clock::now());
    }

    double peek(const TimePoint & timepoint) const
    {
        return tokens + getDynamicTokens(timepoint);
    }

    // Reconfig refill rate and capacity.
    void reConfig(double new_refill_rate, double new_capacity);

private:
    double getDynamicTokens(const TokenBucket::TimePoint & timepoint) const 
    {
        RUNTIME_CHECK(timepoint >= last_compact_timepoint);
        auto elspased = timepoint - last_compact_timepoint;
        auto elapsed_second = std::chrono::duration_cast<std::chrono::seconds>(elspased).count();
        return elapsed_second * refill_rate;
    }

    void compact(const TokenBucket::TimePoint & timepoint)
    {
        tokens += getDynamicTokens(timepoint);
        if (tokens >= capacity)
            tokens = capacity;
        last_compact_timepoint = timepoint;
    }

    double refill_rate;
    double tokens;
    double capacity;

    TimePoint last_compact_timepoint;
};

using TokenBucketPtr = std::unique_ptr<TokenBucket>;
} // namespace DB
