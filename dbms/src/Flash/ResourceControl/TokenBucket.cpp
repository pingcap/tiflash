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

    LOG_TRACE(log, "consume ori token: {}, will consume: {}", tokens, n);
    tokens -= n;
    return tokens >= 0.0;
}

double TokenBucket::peek(const TokenBucket::TimePoint & timepoint) const
{
    return tokens + getDynamicTokens(timepoint);
}

void TokenBucket::reConfig(double new_tokens, double new_fill_rate, double new_capacity)
{
    RUNTIME_CHECK(new_fill_rate >= 0.0);
    RUNTIME_CHECK(new_capacity >= 0.0);

    auto now = std::chrono::steady_clock::now();
    tokens = new_tokens;
    fill_rate = new_fill_rate;
    capacity = new_capacity;

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
    tokens += getDynamicTokens(timepoint);
    if (tokens >= capacity)
        tokens = capacity;
    last_compact_timepoint = timepoint;
}

double TokenBucket::getDynamicTokens(const TokenBucket::TimePoint & timepoint) const
{
    RUNTIME_CHECK(timepoint >= last_compact_timepoint);
    auto elspased = timepoint - last_compact_timepoint;
    auto elapsed_second = std::chrono::duration_cast<std::chrono::seconds>(elspased).count();
    return elapsed_second * fill_rate;
}

} // namespace DB
