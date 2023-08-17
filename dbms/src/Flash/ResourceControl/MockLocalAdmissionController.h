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

#include <Common/ThreadManager.h>
#include <Flash/Executor/toRU.h>

#include <thread>

namespace DB
{
class ResourceGroup;
inline void nopConsumeResource(const std::string &, double, uint64_t) {}
inline uint64_t nopGetPriority(const std::string &)
{
    return 1.0;
}
inline bool nopIsResourceGroupThrottled(const std::string &)
{
    return false;
}


// This is only for ResourceControlQueue gtest.
class MockLocalAdmissionController final : private boost::noncopyable
{
public:
    MockLocalAdmissionController()
        : consume_resource_func(nopConsumeResource)
        , get_priority_func(nopGetPriority)
        , is_resource_group_throttled_func(nopIsResourceGroupThrottled)
    {
        refill_token_thread = std::thread([&]() { refillTokenBucket(); });
    }

    ~MockLocalAdmissionController() { stop(); }

    using ConsumeResourceFuncType = void (*)(const std::string &, double, uint64_t);
    using GetPriorityFuncType = uint64_t (*)(const std::string &);
    using IsResourceGroupThrottledFuncType = bool (*)(const std::string &);

    void consumeResource(const std::string & name, double ru, uint64_t cpu_time_ns)
    {
        consume_resource_func(name, ru, cpu_time_ns);
    }

    uint64_t getPriority(const std::string & name) { return get_priority_func(name); }

    bool isResourceGroupThrottled(const std::string & name) { return is_resource_group_throttled_func(name); }

    void registerRefillTokenCallback(const std::function<void()> & cb) { refill_token_callback = cb; }

    void stop()
    {
        {
            std::lock_guard lock(mu);
            if (stopped)
                return;
            stopped = true;
            cv.notify_all();
        }
        refill_token_thread.join();
    }

    void refillTokenBucket();

    void resetAll()
    {
        resource_groups.clear();
        consume_resource_func = nullptr;
        get_priority_func = nullptr;
        is_resource_group_throttled_func = nullptr;
        max_ru_per_sec = 0;
    }

    std::string dump() const;

    mutable std::mutex mu;
    std::condition_variable cv;
    std::unordered_map<std::string, std::shared_ptr<ResourceGroup>> resource_groups;

    ConsumeResourceFuncType consume_resource_func;
    GetPriorityFuncType get_priority_func;
    IsResourceGroupThrottledFuncType is_resource_group_throttled_func;

    uint64_t max_ru_per_sec = 0;
    bool stopped = false;
    std::function<void()> refill_token_callback;
    std::thread refill_token_thread;
};

} // namespace DB
