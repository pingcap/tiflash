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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Flash/Executor/toRU.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Flash/ResourceControl/MockLocalAdmissionController.h>
#include <Flash/ResourceControl/TokenBucket.h>
#include <TiDB/Etcd/Client.h>
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
        , log(Logger::get("resource group:" + group_pb_.name()))
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

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    enum TokenBucketMode
    {
        normal_mode,
        degrade_mode,
        trickle_mode,
    };

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

    // Priority of resource group set by user.
    // This is specified by tidb: parser/model/model.go
    static constexpr int32_t LowPriorityValue = 1;
    static constexpr int32_t MediumPriorityValue = 8;
    static constexpr int32_t HighPriorityValue = 16;

    // Minus 1 because uint64 max is used as special flag.
    static constexpr uint64_t MAX_VIRTUAL_TIME = (std::numeric_limits<uint64_t>::max() >> 4) - 1;

    friend class LocalAdmissionController;
    friend class MockLocalAdmissionController;

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

    // Called when user change config of resource group.
    // Only update meta, will not touch runtime state(like bucket remaining tokens).
    void reConfig(const resource_manager::ResourceGroup & group_pb_)
    {
        group_pb = group_pb_;
        user_priority = group_pb_.priority();
        user_ru_per_sec = group_pb_.r_u_settings().r_u().settings().fill_rate();
    }

    bool lowToken() const
    {
        std::lock_guard lock(mu);
        return bucket->lowToken();
    }

    double getRU() const
    {
        std::lock_guard lock(mu);
        return bucket->peek();
    }

    // Return how many tokens should acquire from GAC for the next n seconds.
    double getAcquireRUNum(uint32_t n, double amplification) const
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
        double next_n_sec_need_num = avg_speed * n * amplification;

        // Prevent avg_speed from being 0 due to RU exhaustion.
        if (next_n_sec_need_num == 0.0)
            next_n_sec_need_num = base;

        auto acquire_num = next_n_sec_need_num - remaining_ru;
        LOG_TRACE(
            log,
            "acquire num for rg {}: avg_speed: {}, remaining_ru: {}, base: {}, amplification: {}, next n sec need: {}, "
            "acquire num: {}",
            name,
            avg_speed,
            remaining_ru,
            base,
            amplification,
            next_n_sec_need_num,
            acquire_num);
        return acquire_num;
    }

    // New tokens fetched from GAC, update remaining tokens.
    void toNormalMode(double add_tokens, double new_capacity)
    {
        assert(add_tokens >= 0);

        std::lock_guard lock(mu);
        bucket_mode = TokenBucketMode::normal_mode;
        if (new_capacity <= 0.0)
        {
            burstable = true;
            return;
        }
        auto [ori_tokens, ori_fill_rate, ori_capacity] = bucket->getCurrentConfig();
        std::string ori_bucket_info = bucket->toString();
        bucket->reConfig(ori_tokens + add_tokens, ori_fill_rate, ori_capacity);
        LOG_INFO(
            log,
            "token bucket of rg {} reconfig to normal mode. from: {}, to: {}",
            name,
            ori_bucket_info,
            bucket->toString());
    }

    void toTrickleMode(double add_tokens, double new_capacity, int64_t trickle_ms)
    {
        assert(add_tokens > 0.0);
        assert(trickle_ms > 0);

        std::lock_guard lock(mu);
        if (new_capacity <= 0.0)
        {
            burstable = true;
            return;
        }

        bucket_mode = TokenBucketMode::trickle_mode;

        const double trickle_sec = static_cast<double>(trickle_ms) / 1000;
        const double new_fill_rate = add_tokens / trickle_sec;
        RUNTIME_CHECK_MSG(
            new_fill_rate > 0.0,
            "token bucket of {} transform to trickle mode failed. trickle_ms: {}, trickle_sec: {}",
            name,
            trickle_ms,
            trickle_sec);

        std::string ori_bucket_info = bucket->toString();
        bucket->reConfig(bucket->peek(), new_fill_rate, new_capacity);
        LOG_INFO(
            log,
            "token bucket of rg {} reconfig to trickle mode: from: {}, to: {}",
            name,
            ori_bucket_info,
            bucket->toString());
    }

    // If we have network problem with GAC, enter degrade mode.
    void toDegrademode()
    {
        std::lock_guard lock(mu);
        if (burstable || bucket_mode == degrade_mode)
            return;

        bucket_mode = TokenBucketMode::degrade_mode;
        double avg_speed = bucket->getAvgSpeedPerSec();
        auto [ori_tokens, _, ori_capacity] = bucket->getCurrentConfig();
        std::string ori_bucket_info = bucket->toString();
        bucket->reConfig(ori_tokens, avg_speed, ori_capacity);
        LOG_INFO(
            log,
            "token bucket of rg {} reconfig to normal mode done: {}",
            name,
            ori_bucket_info,
            bucket->toString());
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

    bool burstable = false;

    resource_manager::ResourceGroup group_pb;

    mutable std::mutex mu;

    // Local token bucket.
    TokenBucketPtr bucket;

    // Total used cpu_time_in_ns of this ResourceGroup.
    uint64_t cpu_time_in_ns = 0;

    TokenBucketMode bucket_mode = TokenBucketMode::normal_mode;

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
    explicit LocalAdmissionController(
        MPPTaskManagerPtr mpp_task_manager_,
        ::pingcap::kv::Cluster * cluster_,
        Etcd::ClientPtr etcd_client_)
        : cluster(cluster_)
        , mpp_task_manager(mpp_task_manager_)
        , thread_manager(newThreadManager())
        , etcd_client(etcd_client_)
    {
        thread_manager->scheduleThenDetach(true, "LocalAdmissionController::fetchGAC", [this] {
            this->startBackgroudJob();
        });
        thread_manager->scheduleThenDetach(true, "LocalAdmissionController::watchGAC", [this] { this->watchGAC(); });
    }

    ~LocalAdmissionController() { stop(); }

    // NOTE: getOrFetchResourceGroup may throw if resource group has been deleted.
    void consumeResource(const std::string & name, double ru, uint64_t cpu_time_in_ns)
    {
        // When tidb_enable_resource_control is disabled, resource group name is empty.
        if (name.empty())
            return;

        ResourceGroupPtr group = getOrFetchResourceGroup(name);
        group->consumeResource(ru, cpu_time_in_ns);
        if (group->lowToken())
        {
            {
                std::lock_guard lock(mu);
                low_token_resource_groups.push_back(name);
            }
            cv.notify_one();
        }
    }

    uint64_t getPriority(const std::string & name)
    {
        if (name.empty())
            return EMPTY_RESOURCE_GROUP_DEF_PRIORITY;

        ResourceGroupPtr group = getOrFetchResourceGroup(name);
        return group->getPriority(max_ru_per_sec.load());
    }

    bool isResourceGroupThrottled(const std::string & name)
    {
        if (name.empty())
            return false;

        // todo: refine: ResourceControlQueue can record the statics of running pipeline task.
        // If the proportion of scheduled pipeline task is less than 10%, we say this resource group is throttled.
        ResourceGroupPtr group = findResourceGroup(name);
        if (group == nullptr)
            return false;
        return group->getRU() <= 0.0;
    }

    static bool isRUExhausted(uint64_t priority) { return priority == std::numeric_limits<uint64_t>::max(); }

#ifdef DBMS_PUBLIC_GTEST
    static std::unique_ptr<MockLocalAdmissionController> global_instance;
#else
    static std::unique_ptr<LocalAdmissionController> global_instance;
#endif

    // gjt todo private?
    // Interval of fetch from GAC periodically.
    static constexpr uint64_t DEFAULT_FETCH_GAC_INTERVAL = 5;
    // DEFAULT_TOKEN_FETCH_ESAPSED * token_avg_consumption_speed as token num to fetch from GAC.
    static constexpr uint64_t DEFAULT_TOKEN_FETCH_ESAPSED = 5;
    // If we cannot get GAC resp for DEGRADE_MODE_DURATION seconds, enter degrade mode.
    static constexpr auto DEGRADE_MODE_DURATION = 120;
    static constexpr auto TARGET_REQUEST_PERIOD_MS = 5000;
    static constexpr double ACQUIRE_RU_AMPLIFICATION = 1.1;
    // For tidb_enable_resource_control is disabled.
    static constexpr uint64_t EMPTY_RESOURCE_GROUP_DEF_PRIORITY = 1;
    static const std::string GAC_RESOURCE_GROUP_ETCD_PATH;

    // 1. This callback will be called everytime AcquireTokenBuckets GRPC is called.
    // 2. For now, only support one callback.
    //    Because only ResourceControlQueue will register this callback.
    void registerRefillTokenCallback(const std::function<void()> & cb)
    {
        std::lock_guard lock(mu);
        refill_token_callback = cb;
    }

    // This callback will be called when Etcd watcher find resource group is deleted by GAC.
    void registerDeleteResourceGroupCallback(const std::function<void(const std::string & del_rg_name)> & cb)
    {
        std::lock_guard lock(mu);
        delete_resource_group_callback = cb;
    }

    // This callback will be called every DEFAULT_FETCH_GAC_INTERVAL, to cleanup tombstone resource group.
    // Because sometime we cannot delete resource gorup info immediately, need to wait related tasks to finish.
    void registerCleanTombstoneResourceGroupCallback(const std::function<void()> & cb)
    {
        std::lock_guard lock(mu);
        clean_tombstone_resource_group_callback = cb;
    }

    void stop()
    {
        if (stopped)
            return;
        stopped.store(true);
        cv.notify_all();
        thread_manager->wait();
    }

private:
    // Get ResourceGroup by name, if not exist, fetch from PD.
    ResourceGroupPtr getOrFetchResourceGroup(const std::string & name);
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

    std::pair<ResourceGroupPtr, bool> addResourceGroup(const resource_manager::ResourceGroup & new_group_pb)
    {
        uint64_t user_ru_per_sec = new_group_pb.r_u_settings().r_u().settings().fill_rate();
        if (max_ru_per_sec.load() < user_ru_per_sec)
            max_ru_per_sec.store(user_ru_per_sec);

        std::lock_guard lock(mu);
        auto iter = resource_groups.find(new_group_pb.name());
        if (iter != resource_groups.end())
            return std::make_pair(iter->second, false);

        LOG_INFO(log, "add new resource group, info: {}", new_group_pb.DebugString());
        auto new_group = std::make_shared<ResourceGroup>(new_group_pb);
        resource_groups.insert({new_group_pb.name(), new_group});
        return std::make_pair(new_group, true);
    }

    std::vector<std::string> handleTokenBucketsResp(const resource_manager::TokenBucketsResponse & resp);

    void handleBackgroundError(const std::string & err_msg) const;

    static std::string isGACRespValid(const resource_manager::ResourceGroup & new_group_pb);

    struct AcquireTokenInfo
    {
        std::string resource_group_name;
        double acquire_tokens;
        double ru_consumption_delta;
    };

    // Background jobs:
    // 1. Fetch tokens from GAC periodically.
    // 2. Fetch tokens when low threshold is triggered.
    // 3. Check if resource group need to goto degrade mode.
    // 4. Watch GAC event to delete resource group.
    void startBackgroudJob();
    void fetchTokensFromGAC(const std::vector<AcquireTokenInfo> & acquire_tokens);
    void checkDegradeMode();
    void watchGAC();

    void fetchTokensForLowTokenResourceGroups();
    void fetchTokensForAllResourceGroups();

    bool parseResourceGroupNameFromWatchKey(const std::string & etcd_key, std::string & parsed_rg_name) const;

    std::mutex mu;

    std::condition_variable cv;

    std::atomic<bool> stopped = false;

    // gjt todo when to del
    std::unordered_map<std::string, ResourceGroupPtr> resource_groups;

    ::pingcap::kv::Cluster * cluster = nullptr;

    const LoggerPtr log = Logger::get("LocalAdmissionController");

    // To cleanup MinTSOScheduler of resource group.
    MPPTaskManagerPtr mpp_task_manager;

    std::shared_ptr<ThreadManager> thread_manager;

    uint64_t unique_client_id = 0;

    std::atomic<uint64_t> max_ru_per_sec = 0;

    std::function<void()> refill_token_callback;
    std::function<void(const std::string & del_rg_name)> delete_resource_group_callback;
    std::function<void()> clean_tombstone_resource_group_callback;

    // gjt todo update this
    std::chrono::time_point<std::chrono::steady_clock> last_fetch_tokens_from_gac_timepoint
        = std::chrono::steady_clock::now();

    Etcd::ClientPtr etcd_client;

    std::vector<std::string> low_token_resource_groups;
};
} // namespace DB
