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

#include <Common/Exception.h>
#include <Common/ThreadManager.h>
#include <Flash/Executor/toRU.h>

#include <mutex>
#include <thread>
#include <unordered_map>

namespace DB
{
class ResourceGroup;
inline void nopConsumeResource(const std::string &, double, uint64_t) {}
inline uint64_t nopGetPriority(const std::string &)
{
    return 1.0;
}

// This is only for ResourceControlQueue gtest.
class MockLocalAdmissionController final : private boost::noncopyable
{
public:
    static constexpr uint64_t HIGHEST_RESOURCE_GROUP_PRIORITY = 0;

    MockLocalAdmissionController()
        : consume_resource_func(nopConsumeResource)
        , get_priority_func(nopGetPriority)
    {
        refill_token_thread = std::thread([&]() { refillTokenBucket(); });
    }

    ~MockLocalAdmissionController() { stop(); }

    using ConsumeResourceFuncType = void (*)(const std::string &, double, uint64_t);
    using GetPriorityFuncType = uint64_t (*)(const std::string &);
    using IsResourceGroupThrottledFuncType = bool (*)(const std::string &);

    void consumeCPUResource(const std::string & name, double ru, uint64_t cpu_time_ns) const
    {
        consumeResource(name, ru, cpu_time_ns);
    }
    void consumeBytesResource(const std::string & name, double ru) const { consumeResource(name, ru, 0); }
    void consumeResource(const std::string & name, double ru, uint64_t cpu_time_ns) const
    {
        if (name.empty())
            return;

        consume_resource_func(name, ru, cpu_time_ns);
    }
    std::optional<uint64_t> getPriority(const std::string & name) const
    {
        if (name.empty())
            return {HIGHEST_RESOURCE_GROUP_PRIORITY};

        return {get_priority_func(name)};
    }
    void warmupResourceGroupInfoCache(const std::string &) {}
    static uint64_t estWaitDuraMS(const std::string &) { return 100; }

    void registerRefillTokenCallback(const std::function<void()> & cb)
    {
        std::lock_guard lock(call_back_mutex);
        RUNTIME_CHECK_MSG(refill_token_callback == nullptr, "callback cannot be registered multiple times");
        refill_token_callback = cb;
    }
    void unregisterRefillTokenCallback()
    {
        std::lock_guard lock(call_back_mutex);
        RUNTIME_CHECK_MSG(refill_token_callback != nullptr, "callback cannot be nullptr before unregistering");
        refill_token_callback = nullptr;
    }

    void safeStop() { stop(); }

    void stop()
    {
        {
            std::lock_guard lock(mu);
            if (stopped)
                return;
            stopped = true;
            cv.notify_all();
        }
        if (refill_token_thread.joinable())
            refill_token_thread.join();
    }

    void refillTokenBucket();

    std::string dump() const;

    mutable std::mutex mu;
    std::condition_variable cv;
    std::unordered_map<std::string, std::shared_ptr<ResourceGroup>> resource_groups;

    ConsumeResourceFuncType consume_resource_func;
    GetPriorityFuncType get_priority_func;

    uint64_t max_ru_per_sec = 0;
    bool stopped = false;

    std::mutex call_back_mutex;
    std::function<void()> refill_token_callback{nullptr};

    std::thread refill_token_thread;
};
} // namespace DB
