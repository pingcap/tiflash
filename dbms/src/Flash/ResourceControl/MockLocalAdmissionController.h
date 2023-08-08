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

#pragma once

#include <Flash/Executor/toRU.h>
#include <Storages/Transaction/Types.h>

#include <cstdint>
#include <string>

namespace DB
{

class ResourceGroup;

// This is only for ResourceControlQueue gtest.
class MockLocalAdmissionController final : private boost::noncopyable
{
public:
    MockLocalAdmissionController()
        : consume_resource_func(nullptr)
        , get_priority_func(nullptr)
        , is_resource_group_throttled_func(nullptr)
    {}

    ~MockLocalAdmissionController() = default;

    using ConsumeResourceFuncType = void (*)(const std::string &, const KeyspaceID &, double, uint64_t);
    using GetPriorityFuncType = double (*)(const std::string &, const KeyspaceID &);
    using IsResourceGroupThrottledFuncType = bool (*)(const std::string &);

    void consumeResource(const std::string & name, const KeyspaceID & keyspace_id, double ru, uint64_t cpu_time_ns)
    {
        consume_resource_func(name, keyspace_id, ru, cpu_time_ns);
    }

    double getPriority(const std::string & name, const KeyspaceID & keyspace_id)
    {
        return get_priority_func(name, keyspace_id);
    }

    bool isResourceGroupThrottled(const std::string & name)
    {
        return is_resource_group_throttled_func(name);
    }

    void resetAll()
    {
        resource_groups.clear();
        consume_resource_func = nullptr;
        get_priority_func = nullptr;
        is_resource_group_throttled_func = nullptr;
        max_ru_per_sec = 0;
    }

    std::mutex mu;
    std::unordered_map<std::string, std::shared_ptr<ResourceGroup>> resource_groups;

    ConsumeResourceFuncType consume_resource_func;
    GetPriorityFuncType get_priority_func;
    IsResourceGroupThrottledFuncType is_resource_group_throttled_func;

    uint64_t max_ru_per_sec = 0;
};

} // namespace DB
