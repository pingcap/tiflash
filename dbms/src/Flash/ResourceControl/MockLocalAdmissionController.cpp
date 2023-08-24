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

#include <Flash/ResourceControl/LocalAdmissionController.h>

namespace DB
{
void MockLocalAdmissionController::refillTokenBucket()
{
    while (true)
    {
        {
            std::unique_lock lock(mu);
            if (cv.wait_for(lock, std::chrono::seconds(1), [&]() { return stopped; }))
                return;

            for (auto & ele : resource_groups)
            {
                auto & rg = ele.second;
                if (rg->bucket->isStatic())
                {
                    auto config = rg->bucket->getConfig();
                    // Use capacity as new token, because for static token bucket, its fill rate is zero.
                    config.tokens += config.capacity;
                    rg->bucket->reConfig(config);
                }
            }
        }

        std::lock_guard lock(call_back_mutex);
        if (refill_token_callback)
            refill_token_callback();
    }
}

std::string MockLocalAdmissionController::dump() const
{
    FmtBuffer fmt_buf;
    std::lock_guard lock(mu);
    for (const auto & ele : resource_groups)
    {
        const auto & rg = ele.second;
        fmt_buf.fmtAppend(
            "rg: {}, cpu usage: {}, token bucket: {};",
            rg->name,
            rg->cpu_time_in_ns,
            rg->bucket->toString());
    }
    return fmt_buf.toString();
}
} // namespace DB
