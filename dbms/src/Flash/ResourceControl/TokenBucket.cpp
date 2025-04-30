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

#include <Common/Exception.h>
#include <Flash/ResourceControl/TokenBucket.h>

namespace DB
{
void TokenBucket::put(double n)
{
    RUNTIME_CHECK(n >= 0.0);
    tokens += n;
}

bool TokenBucket::consume(double n, const std::chrono::steady_clock::time_point & tp)
{
    RUNTIME_CHECK(n >= 0.0);

    compact(tp);

    tokens -= n;
    return tokens >= 0.0;
}

double TokenBucket::peek(const TimePoint & tp) const
{
    return tokens + getDynamicTokens(tp);
}

void TokenBucket::reConfig(const TokenBucketConfig & config, const TimePoint & tp)
{
    RUNTIME_CHECK(config.fill_rate >= 0.0);
    RUNTIME_CHECK(config.capacity >= 0.0);

    tokens = config.tokens;
    fill_rate = config.fill_rate;
    fill_rate_ms = config.fill_rate / 1000;
    capacity = config.capacity;
    low_token_threshold = config.low_token_threshold;

    compact(tp);
}

void TokenBucket::compact(const TimePoint & tp)
{
    if (tp - last_compact_timepoint <= MIN_COMPACT_INTERVAL)
        return;

    tokens += getDynamicTokens(tp);
    if (tokens >= capacity)
        tokens = capacity;
    last_compact_timepoint = tp;
}

double TokenBucket::getDynamicTokens(const TimePoint & tp) const
{
    RUNTIME_CHECK(tp >= last_compact_timepoint);
    auto elspased = tp - last_compact_timepoint;
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elspased).count();
    return elapsed_ms * fill_rate_ms;
}

} // namespace DB
