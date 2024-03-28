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

#pragma once

#include <Storages/KVStore/Utils.h>

#include <atomic>
#include <memory>
#include <unordered_map>

namespace DB
{
struct PreHandlingTrace : MutexLockWrap
{
    struct Item
    {
        Item()
            : abort_flag(false)
        {}
        std::atomic_bool abort_flag;
    };

    // Prehandle use thread pool from Proxy's side, so it can't benefit from AsyncTasks.
    std::unordered_map<uint64_t, std::shared_ptr<Item>> tasks;

    std::shared_ptr<Item> registerTask(uint64_t region_id)
    {
        // Automaticlly override the old one.
        auto _ = genLockGuard();
        auto b = std::make_shared<Item>();
        tasks[region_id] = b;
        return b;
    }
    std::shared_ptr<Item> deregisterTask(uint64_t region_id)
    {
        auto _ = genLockGuard();
        auto it = tasks.find(region_id);
        if (it != tasks.end())
        {
            auto b = it->second;
            tasks.erase(it);
            return b;
        }
        return nullptr;
    }
    bool hasTask(uint64_t region_id)
    {
        auto _ = genLockGuard();
        return tasks.find(region_id) != tasks.end();
    }
};
} // namespace DB