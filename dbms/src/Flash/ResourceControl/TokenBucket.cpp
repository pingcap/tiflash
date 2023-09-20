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

#include <iostream>

namespace DB
{
void TokenBucket::put(double n)
{
    RUNTIME_CHECK(n >= 0.0);
    tokens += n;
}

bool TokenBucket::consume(double n)
{
    RUNTIME_CHECK(n >= 0.0);

    auto now = std::chrono::steady_clock::now();
    compact(now);

    tokens -= n;
    return tokens >= 0.0;
}

double TokenBucket::peek(const TokenBucket::TimePoint & timepoint) const
{
    return tokens + getDynamicTokens(timepoint);
}

void TokenBucket::reConfig(const TokenBucketConfig & config)
{
    RUNTIME_CHECK(config.fill_rate >= 0.0);
    RUNTIME_CHECK(config.capacity >= 0.0);

    auto now = std::chrono::steady_clock::now();
    tokens = config.tokens;
    fill_rate = config.fill_rate;
    fill_rate_ms = config.fill_rate / 1000;
    capacity = config.capacity;

    compact(now);
    // Update because token number may increase, which may cause token_changed be negative.
    last_get_avg_speed_tokens = tokens;
    last_get_avg_speed_timepoint = std::chrono::steady_clock::now();
}

double TokenBucket::getAvgSpeedPerSec()
{
    auto now = std::chrono::steady_clock::now();
    RUNTIME_CHECK(now >= last_get_avg_speed_timepoint);
    auto dura = std::chrono::duration_cast<std::chrono::seconds>(now - last_get_avg_speed_timepoint);

    compact(now);
    double token_changed = last_get_avg_speed_tokens - tokens;

    // If dura less than 1 sec, return last sec avg speed.
    if (dura.count() >= 1)
    {
        avg_speed_per_sec = token_changed / dura.count();
        last_get_avg_speed_tokens = tokens;
        last_get_avg_speed_timepoint = now;
    }
    LOG_TRACE(
        log,
        "getAvgSpeedPerSec dura: {}, last_get_avg_speed_tokens: {}, cur tokens: {}, avg_speed_per_sec: {}",
        dura.count(),
        last_get_avg_speed_tokens,
        tokens,
        avg_speed_per_sec);
    return avg_speed_per_sec;
}

void TokenBucket::compact(const TokenBucket::TimePoint & timepoint)
{
    if (timepoint - last_compact_timepoint <= MIN_COMPACT_INTERVAL)
        return;

    tokens += getDynamicTokens(timepoint);
    if (tokens >= capacity)
        tokens = capacity;
    last_compact_timepoint = timepoint;
}

double TokenBucket::getDynamicTokens(const TokenBucket::TimePoint & timepoint) const
{
    RUNTIME_CHECK(timepoint >= last_compact_timepoint);
    auto elspased = timepoint - last_compact_timepoint;
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elspased).count();
    return elapsed_ms * fill_rate_ms;
}

} // namespace DB
