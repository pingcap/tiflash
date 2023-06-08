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

#include <Flash/ResourceControl/TokenBucket.h>

namespace DB
{
void TokenBucket::put(double n)
{
    RUNTIME_CHECK(n >= 0.0);
    tokens += n;
    compact(std::chrono::steady_clock::now());
}

bool TokenBucket::consume(double n)
{
    RUNTIME_CHECK(n >= 0.0);

    auto now = std::chrono::steady_clock::now();
    compact(now);

    if (tokens < n)
        return false;

    tokens -= n;
    return true;
}

void TokenBucket::reConfig(double new_refill_rate, double new_capacity)
{
    RUNTIME_CHECK(new_refill_rate >= 0.0);
    RUNTIME_CHECK(new_capacity >= 0.0);

    auto now = std::chrono::steady_clock::now();
    compact(now);

    refill_rate = new_refill_rate;
    capacity = new_capacity;
    compact(std::chrono::steady_clock::now());
}
} // namespace DB
