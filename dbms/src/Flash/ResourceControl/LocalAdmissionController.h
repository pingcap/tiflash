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
#include <Common/ThreadManager.h>
#include <Flash/Executor/toRU.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Pipeline/Schedule/TaskQueues/MultiLevelFeedbackQueue.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Flash/ResourceControl/TokenBucket.h>
#include <Flash/ResourceControl/MockLocalAdmissionController.h>

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

    // gjt todo del
    // static uint32_t getUserPriorityWeight(uint32_t user_priority)
    // {
    //     if (user_priority == LowPriorityValue)
    //         return 100;
    //     else if (user_priority == MediumPriorityValue)
    //         return 20;
    //     else
    //         return 0;
    // }

    friend class LocalAdmissionController;
    std::string getName() const { return name; }

    bool lowToken() const
    {
        std::lock_guard lock(mu);
        return bucket_mode == TokenBucketMode::normal_mode && bucket->lowToken();
    }

    void consumeResource(double ru, uint64_t cpu_time_in_ns_)
    {
        std::lock_guard lock(mu);
        cpu_time_in_ns += cpu_time_in_ns_;
        bucket->consume(ru);
        ru_consumption_delta += ru;
    }

    // Get remaining RU of this resource group.
    double getRU() const
    {
        std::lock_guard lock(mu);
        return bucket->peek();
    }

    double getAcquireRUNum(uint32_t avg_token_speed_duration, double amplification) const
    {
        assert(amplification >= 1.0);

        double avg_speed = 0.0;
        double remaining_ru = 0.0;
        double base = 0.0;
        {
            std::lock_guard lock(mu);
            avg_speed = bucket->getAvgSpeedPerSec();
            remaining_ru = bucket->peek();
            base = static_cast<double>(user_ru_per_sec);
        }

        // Appropriate amplification is necessary to prevent situation that GAC has sufficient RU,
        // while user query speed is limited due to LAC requests too few RU.
        double num = avg_speed * avg_token_speed_duration * amplification;

        // Prevent avg_speed from being 0 due to RU exhaustion.
        if (num == 0.0)
            num = base;

        // Prevent a specific LAC from taking too many RUs,
        // with each LAC only obtaining the required RU for the next second.
        if (num > remaining_ru)
            num -= remaining_ru;
        return num;
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

        uint64_t priority = (((user_priority - 1) << 60) | virtual_time);

        LOG_TRACE(log, "getPriority detailed info: resource group name: {}, weight: {}, virtual_time: {}, user_priority: {}, priority: {}", name, weight, virtual_time, user_priority, priority);
        return priority;
    }

    // New tokens fetched from GAC, update remaining tokens.
    // gjt todo new_capacity < 0? ==0? >0?
    void reConfigTokenBucketInNormalMode(double add_tokens, double new_capacity)
    {
        std::lock_guard lock(mu);
        bucket_mode = TokenBucketMode::normal_mode;
        if (new_capacity <= 0.0)
        {
            burstable = true;
            return;
        }
        auto [ori_tokens, ori_fill_rate, ori_capacity] = bucket->getCurrentConfig();
        bucket->reConfig(ori_tokens + add_tokens, ori_fill_rate, ori_capacity);
    }

    // Tokens of GAC is not enough, enter trickling mode.
    void reConfigTokenBucketInTrickleMode(double add_tokens, double new_capacity, int64_t trickle_ms)
    {
        std::lock_guard lock(mu);
        if (new_capacity <= 0.0)
        {
            burstable = true;
            return;
        }

        bucket_mode = TokenBucketMode::trickle_mode;
        double new_tokens = bucket->peek() + add_tokens;
        double trickle_sec = static_cast<double>(trickle_ms) / 1000;
        double new_fill_rate = new_tokens / trickle_sec;
        bucket->reConfig(new_tokens, new_fill_rate, new_capacity);
    }

    // If we have network problem with GAC, enter degrade mode.
    void reConfigTokenBucketInDegradeMode()
    {
        std::lock_guard lock(mu);
        bucket_mode = TokenBucketMode::degrade_mode;
        double avg_speed = bucket->getAvgSpeedPerSec();
        auto [ori_tokens, _, ori_capacity] = bucket->getCurrentConfig();
        bucket->reConfig(ori_tokens, avg_speed, ori_capacity);
    }

    void stepIntoDegradeModeIfNecessary(uint64_t dura_seconds)
    {
        if (dura_seconds == 0)
            return;

        auto now = std::chrono::steady_clock::now();
        std::lock_guard lock(mu);
        if (burstable)
            return;

        if (now - last_fetch_tokens_from_gac_timepoint >= std::chrono::seconds(dura_seconds))
        {
            if (bucket_mode == TokenBucketMode::degrade_mode)
                return;

            reConfigTokenBucketInDegradeMode();
        }
    }

    KeyspaceID getKeyspaceID() const
    {
        return keyspace_id;
    }

    double getAndCleanConsumptionDelta()
    {
        std::lock_guard lock(mu);
        auto ori = ru_consumption_delta;
        ru_consumption_delta = 0.0;
        return ori;
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

// LocalAdmissionController is the local(tiflash) part of the distributed token bucket algorithm.
// 1. It manages all ResourceGroups for one tiflash node.
//   1. create: Fetch info from GAC if RG not found in LAC.
//   2. delete: Cleanup deleted RG from LAC periodically.
//   3. update: Update fill_rate/capacity of TokenBucket periodically.
// 2. Will fetch token/RU from GAC:
//   1: Periodically.
//   2: Low token threshold.
// 3. When GAC has no enough tokens for LAC, LAC will start trickling(a.k.a. using availableTokens/trickleTime as refill rate).
// 4. Degrade Mode:
//   1. If cannot get resp from GAC for a while, will enter degrade mode.
//   2. LAC runs as an independent token bucket whose refill rate is RU_PER_SEC in degrade mode.
class LocalAdmissionController final : private boost::noncopyable
{
public:
    explicit LocalAdmissionController(MPPTaskManagerPtr mpp_task_manager_, ::pingcap::kv::Cluster * cluster_)
        : cluster(cluster_)
        , last_cleanup_resource_group_timepoint(std::chrono::steady_clock::now())
        , mpp_task_manager(mpp_task_manager_)
        , thread_manager(newThreadManager())
    {
        thread_manager->scheduleThenDetach(true, "LocalAdmissionController", [this] { this->startBackgroudJob(); });
    }

    ~LocalAdmissionController()
    {
        {
            std::lock_guard lock(mu);
            stopped.store(true);
            cv.notify_all();
        }
        thread_manager->wait();
    }

    void consumeResource(const std::string & name, const KeyspaceID & keyspace_id, double ru, uint64_t cpu_time_in_ns)
    {
        ResourceGroupPtr group = getOrCreateResourceGroup(name, keyspace_id);
        group->consumeResource(ru, cpu_time_in_ns);
        if (group->lowToken())
            cv.notify_one();
    }

    uint64_t getPriority(const std::string & name, const KeyspaceID & keyspace_id)
    {
        ResourceGroupPtr group = getOrCreateResourceGroup(name, keyspace_id);
        return group->getPriority(max_ru_per_sec.load());
    }

    bool isResourceGroupThrottled(const std::string & name)
    {
        // todo: refine: ResourceControlQueue can record the statics of running pipeline task.
        // If the proportion of scheduled pipeline task is less than 10%, we say this resource group is throttled.
        ResourceGroupPtr group = findResourceGroup(name);
        if (group == nullptr)
            return false;
        return group->getRU() <= 0.0;
    }

#ifndef DBMS_PUBLIC_GTEST
    static std::unique_ptr<LocalAdmissionController> global_instance;
#else
    static std::unique_ptr<MockLocalAdmissionController> global_instance;
#endif

    // Interval of fetch from GAC periodically.
    static constexpr uint64_t DEFAULT_FETCH_GAC_INTERVAL = 5;
    // DEFAULT_TOKEN_FETCH_ESAPSED * token_avg_consumption_speed as token num to fetch from GAC.
    static constexpr uint64_t DEFAULT_TOKEN_FETCH_ESAPSED = 5;
    // Interval of cleanup resource group.
    static constexpr auto CLEANUP_RESOURCE_GROUP_INTERVAL = std::chrono::minutes(10);
    // If we cannot get GAC resp for DEGRADE_MODE_DURATION seconds, enter degrade mode.
    static constexpr auto DEGRADE_MODE_DURATION = 120;
    static constexpr auto TARGET_REQUEST_PERIOD_MS = 5000;
    static constexpr double ACQUIRE_RU_AMPLIFICATION = 1.1;

private:
    // Get ResourceGroup by name, if not exist, fetch from PD.
    // If you are sure this resource group exists in GAC, you can skip the check.
    ResourceGroupPtr getOrCreateResourceGroup(const std::string & name, const KeyspaceID & keyspace_id);
    ResourceGroupPtr findResourceGroup(const std::string & name)
    {
        std::lock_guard lock(mu);
        for (const auto & group : resource_groups)
        {
            if (group.first == name)
                return group.second;
        }
        return nullptr;
    }

    std::pair<ResourceGroupPtr, bool> addResourceGroup(const resource_manager::ResourceGroup & new_group_pb, const KeyspaceID & keyspace_id)
    {
        uint64_t user_ru_per_sec = new_group_pb.r_u_settings().r_u().settings().fill_rate();
        if (max_ru_per_sec.load() < user_ru_per_sec)
            max_ru_per_sec.store(user_ru_per_sec);

        std::lock_guard lock(mu);
        for (const auto & group : resource_groups)
        {
            if (group.first == new_group_pb.name())
                return std::make_pair(group.second, false);
        }

        auto new_group = std::make_shared<ResourceGroup>(new_group_pb, keyspace_id);
        resource_groups.insert({new_group_pb.name(), new_group});
        return std::make_pair(new_group, true);
    }

    void setupUniqueClientID()
    {
        // todo: need etcd client.
    }

    void handleTokenBucketsResp(const resource_manager::TokenBucketsResponse & resp);

    void handleBackgroundError(const std::string & err_msg);

    // Background jobs:
    // 1. Fetch tokens from GAC periodically.
    // 2. Fetch tokens when low threshold is triggered.
    // 3. Cleanup resource groups.
    // 4. Check if resource group need to goto degrade mode.
    void startBackgroudJob();
    void fetchTokensFromGAC();
    void cleanupResourceGroups();
    void checkDegradeMode();

    std::mutex mu;

    std::condition_variable cv;

    std::atomic<bool> stopped = false;

    std::unordered_map<std::string, ResourceGroupPtr> resource_groups;

    ::pingcap::kv::Cluster * cluster = nullptr;

    const LoggerPtr log = Logger::get("LocalAdmissionController");

    // No need to use lock, only be accessed by background thread.
    std::chrono::time_point<std::chrono::steady_clock> last_cleanup_resource_group_timepoint;

    // To cleanup MinTSOScheduler of resource group.
    MPPTaskManagerPtr mpp_task_manager;

    std::shared_ptr<ThreadManager> thread_manager;

    int64_t unique_client_id = -1;

    std::atomic<uint64_t> max_ru_per_sec = 0;
};
} // namespace DB
