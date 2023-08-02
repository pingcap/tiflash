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

#include <Flash/Executor/toRU.h>
#include <Storages/Transaction/Types.h>

#include <string>
#include <cstdint>

namespace DB
{

// This is only for ResourceControlQueue gtest.
class MockLocalAdmissionController final
{
public:
    MockLocalAdmissionController() = default;
    ~MockLocalAdmissionController() = default;

    void consumeResource(const std::string & name, const KeyspaceID &, double, uint64_t cpu_time_ns)
    {
        auto iter = resource_groups.find(name);
        if (iter == resource_groups.end())
            __builtin_unreachable();

        iter->second.first += toCPUTimeMillisecond(cpu_time_ns);
        iter->second.second -= toRU(cpu_time_ns)
    }

    double getPriority(const std::string &, const KeyspaceID &)
    {
        return 1000;
    }

    bool isResourceGroupThrottled(const std::string &)
    {
        return false;
    }

    // <resource_group_name, <cpu_usage_in_ms, remaining_ru>
    std::unordered_map<std::string, std::pair<int64_t, int64_t>> resource_groups;
};

} // namespace DB
