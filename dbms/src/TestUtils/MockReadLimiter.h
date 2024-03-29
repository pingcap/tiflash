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

#include <IO/BaseFile/RateLimiter.h>

namespace DB
{
class MockReadLimiter final : public ReadLimiter
{
public:
    MockReadLimiter(
        std::function<Int64()> getIOStatistic_,
        Int64 rate_limit_per_sec_,
        LimiterType type_ = LimiterType::UNKNOW,
        UInt64 refill_period_ms_ = 100)
        : ReadLimiter(getIOStatistic_, rate_limit_per_sec_, type_, refill_period_ms_)
    {}

protected:
    void consumeBytes(Int64 bytes) override
    {
        alloc_bytes += std::min(available_balance, bytes);
        available_balance -= bytes;
    }
};

} // namespace DB