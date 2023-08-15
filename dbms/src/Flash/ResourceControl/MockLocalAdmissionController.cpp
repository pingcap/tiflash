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

#include <Flash/ResourceControl/LocalAdmissionController.h>

namespace DB
{
void MockLocalAdmissionController::refillTokenBucket()
{
    while (true)
    {
        bool resource_group_empty = true;
        {
            std::unique_lock lock(mu);
            if (cv.wait_for(lock, std::chrono::seconds(1), [&]() { return stopped; }))
                return;

            resource_group_empty = resource_groups.empty();
            for (auto & ele : resource_groups)
            {
                auto & rg = ele.second;
                if (rg->bucket->isStatic())
                {
                    std::cout << "refill token bucket of rg " << rg->name << std::endl;
                    // Use capacity as new token, because for static token bucket, its fill rate is zero.
                    const double new_token = rg->bucket->tokens + rg->bucket->capacity;
                    const double new_fill_rate = rg->bucket->fill_rate;
                    const double new_capacity = rg->bucket->capacity;
                    rg->bucket->reConfig(new_token, new_fill_rate, new_capacity);
                }
            }
        }

        if (!resource_group_empty)
            refill_token_callback();
    }
}

std::string MockLocalAdmissionController::dump() const
{
    std::string info_str;
    std::lock_guard lock(mu);
    for (const auto & ele : resource_groups)
    {
        const auto & rg = ele.second;
        info_str += fmt::format(
            "rg: {}, remaining ru: {}, cpu usage: {};",
            rg->name,
            rg->bucket->peek(),
            rg->cpu_time_in_ns);
    }
    return info_str;
}
} // namespace DB
