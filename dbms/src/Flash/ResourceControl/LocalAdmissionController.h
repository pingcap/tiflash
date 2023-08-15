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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Flash/Executor/toRU.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Flash/ResourceControl/MockLocalAdmissionController.h>
#include <Flash/ResourceControl/TokenBucket.h>
#include <common/logger_useful.h>
#include <kvproto/resource_manager.pb.h>
#include <pingcap/kv/Cluster.h>

#include <atomic>
#include <memory>
#include <mutex>

namespace DB
{
class LocalAdmissionController;

class ResourceGroup final : private boost::noncopyable
{
public:
    explicit ResourceGroup(const resource_manager::ResourceGroup & group_pb_)
        : name(group_pb_.name())
        , user_priority(group_pb_.priority())
        , user_ru_per_sec(group_pb_.r_u_settings().r_u().settings().fill_rate())
        , group_pb(group_pb_)
        , cpu_time_in_ns(0)
        , log(Logger::get("rg:" + group_pb_.name()))
    {
        const auto & setting = group_pb.r_u_settings().r_u().settings();
        initStaticTokenBucket(setting.burst_limit());
    }

#ifdef DBMS_PUBLIC_GTEST
    ResourceGroup(const std::string & group_name_, uint32_t user_priority_, uint64_t user_ru_per_sec_, bool burstable_)
        : name(group_name_)
        , user_priority(user_priority_)
        , user_ru_per_sec(user_ru_per_sec_)
        , burstable(burstable_)
        , log(Logger::get("rg:" + group_name_))
    {
        initStaticTokenBucket(user_ru_per_sec_);
    }
#endif

    ~ResourceGroup() = default;

    enum TokenBucketMode
    {
        normal_mode,
        degrade_mode,
        trickle_mode,
    };

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    // Priority of resource group set by user.
    // This is specified by tidb: parser/model/model.go
    static constexpr int32_t LowPriorityValue = 1;
    static constexpr int32_t MediumPriorityValue = 8;
    static constexpr int32_t HighPriorityValue = 16;

    // Minus 1 because uint64 max is used as special flag.
    static constexpr uint64_t MAX_VIRTUAL_TIME = (std::numeric_limits<uint64_t>::max() >> 4) - 1;

    friend class LocalAdmissionController;
    std::string getName() const { return name; }

    void consumeResource(double ru, uint64_t cpu_time_in_ns_)
    {
        std::lock_guard lock(mu);
        cpu_time_in_ns += cpu_time_in_ns_;
        bucket->consume(ru);
        ru_consumption_delta += ru;
    }

    // Priority greater than zero: Less number means higher priority.
    // Zero priority means has no RU left, should not schedule this resource group at all.
    uint64_t getPriority(uint64_t max_ru_per_sec) const
    {
        std::lock_guard lock(mu);

        const auto remaining_token = bucket->peek();
        if (!burstable && remaining_token <= 0.0)
            return std::numeric_limits<uint64_t>::max();

        // This should not happens because tidb will check except for unittest(test static token bucket).
        if unlikely (user_ru_per_sec == 0)
            return std::numeric_limits<uint64_t>::max() - 1;

        double weight = static_cast<double>(max_ru_per_sec) / user_ru_per_sec;

        uint64_t virtual_time = cpu_time_in_ns * weight;
        if unlikely (virtual_time > MAX_VIRTUAL_TIME)
            virtual_time = MAX_VIRTUAL_TIME;

        uint64_t priority = (((static_cast<uint64_t>(user_priority) - 1) << 60) | virtual_time);

        LOG_TRACE(
            log,
            "getPriority detailed info: resource group name: {}, weight: {}, virtual_time: {}, user_priority: {}, "
            "priority: {}, remaining_token: {}",
            name,
            weight,
            virtual_time,
            user_priority,
            priority,
            remaining_token);
        return priority;
    }

    void initStaticTokenBucket(uint64_t capacity = std::numeric_limits<uint64_t>::max())
    {
        // If token bucket is normal mode, it's static, so fill_rate is zero.
        const double init_fill_rate = 0.0;
        const double init_tokens = user_ru_per_sec;
        const double init_cap = capacity;
        bucket = std::make_unique<TokenBucket>(init_fill_rate, init_tokens, init_cap);
        assert(
            user_priority == LowPriorityValue || user_priority == MediumPriorityValue
            || user_priority == HighPriorityValue);
    }

    const std::string name;

    uint32_t user_priority;
    uint64_t user_ru_per_sec;

    bool burstable = false;

    // Definition of the RG, e.g. RG settings, priority etc.
    resource_manager::ResourceGroup group_pb;

    mutable std::mutex mu;

    // Local token bucket.
    TokenBucketPtr bucket;

    // Total used cpu_time_in_ns of this ResourceGroup.
    uint64_t cpu_time_in_ns = 0;

    double ru_consumption_delta = 0.0;

    LoggerPtr log;
};

using ResourceGroupPtr = std::shared_ptr<ResourceGroup>;

class LocalAdmissionController final : private boost::noncopyable
{
public:
    static bool isRUExhausted(uint64_t priority) { return priority == std::numeric_limits<uint64_t>::max(); }

    static std::unique_ptr<MockLocalAdmissionController> global_instance;
};
} // namespace DB
