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
    static const std::string DEFAULT_RESOURCE_GROUP_NAME;

    explicit ResourceGroup(const resource_manager::ResourceGroup & group_pb_, KeyspaceID keyspace_id_)
        : name(group_pb_.name())
        , user_priority(group_pb_.priority())
        , user_ru_per_sec(group_pb_.r_u_settings().r_u().settings().fill_rate())
        , group_pb(group_pb_)
        , cpu_time_in_ns(0)
        , last_fetch_tokens_from_gac_timepoint(std::chrono::steady_clock::now())
        , keyspace_id(keyspace_id_)
        , log(Logger::get("resource_group-" + group_pb_.name()))
    {
        const auto & setting = group_pb.r_u_settings().r_u().settings();
        bucket = std::make_unique<TokenBucket>(setting.fill_rate(), setting.fill_rate(), setting.burst_limit());
    }

#ifdef DBMS_PUBLIC_GTEST
    ResourceGroup(const std::string & group_name_, uint32_t user_priority_, uint64_t user_ru_per_sec_, bool burstable_)
        : name(group_name_)
        , user_priority(user_priority_)
        , user_ru_per_sec(user_ru_per_sec_)
        , burstable(burstable_)
        , keyspace_id(NullspaceID)
        , log(Logger::get("resource_group-" + group_name_))
    {
        bucket = std::make_unique<TokenBucket>(user_ru_per_sec, user_ru_per_sec_);
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

    static constexpr uint64_t MAX_VIRTUAL_TIME = (std::numeric_limits<uint64_t>::max() >> 4);

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
        RUNTIME_CHECK_MSG(user_priority == LowPriorityValue || user_priority == MediumPriorityValue || user_priority == HighPriorityValue, "unexpected user_priority {}", user_priority);

        if (!burstable && bucket->peek() <= 0.0)
            return 0;

        double weight = 0.0;
        if (name == DEFAULT_RESOURCE_GROUP_NAME)
            weight = 1.0;
        else
            weight = static_cast<double>(max_ru_per_sec) / user_ru_per_sec;

        uint64_t virtual_time = cpu_time_in_ns * weight;
        if unlikely (virtual_time > MAX_VIRTUAL_TIME)
            virtual_time = MAX_VIRTUAL_TIME;

        uint64_t priority = (((user_priority - 1) << 60) | virtual_time); // NOLINT(shift-count-overflow)

        LOG_TRACE(log, "getPriority detailed info: resource group name: {}, weight: {}, virtual_time: {}, user_priority: {}, priority: {}", name, weight, virtual_time, user_priority, priority);
        return priority;
    }

    const std::string name;

    uint32_t user_priority;
    uint64_t user_ru_per_sec;

    // gjt todo burstable?
    bool burstable = false;

    // Definition of the RG, e.g. RG settings, priority etc.
    resource_manager::ResourceGroup group_pb;

    mutable std::mutex mu;

    // Local token bucket.
    TokenBucketPtr bucket;

    // Total used cpu_time_in_ns of this ResourceGroup.
    uint64_t cpu_time_in_ns = 0;

    std::chrono::time_point<std::chrono::steady_clock> last_fetch_tokens_from_gac_timepoint;

    TokenBucketMode bucket_mode = TokenBucketMode::normal_mode;

    std::chrono::steady_clock::time_point last_gac_update_timepoint;

    const KeyspaceID keyspace_id = NullspaceID;

    double ru_consumption_delta = 0.0;

    LoggerPtr log;
};

using ResourceGroupPtr = std::shared_ptr<ResourceGroup>;

class LocalAdmissionController final : private boost::noncopyable
{
public:
#ifndef DBMS_PUBLIC_GTEST
    static std::unique_ptr<LocalAdmissionController> global_instance;
#else
    static std::unique_ptr<MockLocalAdmissionController> global_instance;
#endif
};
} // namespace DB
