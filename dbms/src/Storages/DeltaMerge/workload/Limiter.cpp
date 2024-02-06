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
#include <Storages/DeltaMerge/workload/Limiter.h>
#include <Storages/DeltaMerge/workload/Options.h>
#include <fmt/core.h>

#include <cmath>

namespace DB::DM::tests
{
class ConstantLimiter : public Limiter
{
public:
    explicit ConstantLimiter(uint64_t rate_per_sec)
        : limiter(rate_per_sec, LimiterType::UNKNOW)
    {}
    void request() override { limiter.request(1); }

private:
    WriteLimiter limiter;
};

std::unique_ptr<Limiter> Limiter::create(const WorkloadOptions & opts)
{
    uint64_t per_sec = std::ceil(opts.max_write_per_sec * 1.0 / opts.write_thread_count);
    return std::make_unique<ConstantLimiter>(per_sec);
}

} // namespace DB::DM::tests