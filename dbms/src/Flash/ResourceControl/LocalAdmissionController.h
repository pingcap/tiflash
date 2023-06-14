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
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Flash/Pipeline/Schedule/TaskQueues/MultiLevelFeedbackQueue.h>
#include <Flash/ResourceControl/TokenBucket.h>
#include <kvproto/resource_manager.pb.h>
#include <pingcap/kv/Cluster.h>

#include <atomic>
#include <memory>
#include <mutex>

namespace DB
{
class LocalAdmissionController;

class ResourceGroup final
{
public:
    explicit ResourceGroup(const ::resource_manager::ResourceGroup & group_pb_)
        : name(group_pb_.name())
        , user_priority(group_pb_.priority())
        , group_pb(group_pb_)
        , cpu_time(0)
        , last_fetch_tokens_from_gac_timepoint(std::chrono::steady_clock::now())
    {
        const auto & setting = group_pb.r_u_settings().r_u().settings();
        bucket = std::make_unique<TokenBucket>(setting.fill_rate(), setting.fill_rate(), setting.burst_limit());
    }

    ~ResourceGroup() = default;

private:
    friend class LocalAdmissionController;
    std::string getName() const { return name; }

    bool lowToken() const
    {
        std::lock_guard lock(mu);
        return bucket->lowToken();
    }

    bool consumeResource(double ru, uint64_t cpu_time_)
    { 
        cpu_time += cpu_time_;
        std::lock_guard lock(mu);
        return bucket->consume(ru);
    }

    // Get remaining RU of this resource group.
    double getRU() const
    { 
        std::lock_guard lock(mu);
        return bucket->peek();
    }

    double getCPUTime() const
    {
        return cpu_time.load();
    }

    double getAcquireRUNum(uint32_t avg_token_speed_duration) const
    {
        double avg_speed = 0.0;
        {
            std::lock_guard lock(mu);
            avg_speed = bucket->getAvgSpeedPerSec();
        }
        return avg_speed * avg_token_speed_duration;
    }

    // Positive: Less number means higher priority.
    // Negative means has no RU left, will not schedule this resource group.
    double getPriority() const
    {
        RUNTIME_ASSERT(user_priority == 1 || user_priority == 8 || user_priority == 16);
        {
            std::lock_guard lock(mu);
            if (bucket->peek() <= 0.0)
                return -1.0;
        }
        return ((user_priority - 1) << 60) | cpu_time;
    }

    // New tokens fetched from GAC, update remaining tokens.
    // gjt todo new_capacity < 0? ==0? >0?
    void reConfigTokenBucketInNormalMode(double add_tokens)
    {
        std::lock_guard lock(mu);
        auto [ori_tokens, ori_fill_rate, ori_capacity] = bucket->getCurrentConfig();
        bucket->reConfig(ori_tokens + add_tokens, ori_fill_rate, ori_capacity);
    }

    // Tokens of GAC is not enough, enter trickling mode.
    void reConfigTokenBucketInTrickleMode(double add_tokens, double new_capacity, int64_t trickle_ms)
    {
        std::lock_guard lock(mu);
        double new_tokens = bucket->peek() + add_tokens;
        double trickle_sec = static_cast<double>(trickle_ms) / 1000;
        double new_fill_rate = new_tokens / trickle_sec;
        bucket->reConfig(new_tokens, new_fill_rate, new_capacity);
    }

    // If we have network problem with GAC, enter degrade mode.
    void reConfigTokenBucketInDegradeMode()
    {
        std::lock_guard lock(mu);
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
        if (now - last_fetch_tokens_from_gac_timepoint >= std::chrono::seconds(dura_seconds))
        {
            if (in_degrade_mode)
                return;
            in_degrade_mode = true;

            reConfigTokenBucketInDegradeMode();
        }
    }

    const std::string name;

    // Priority of resource group set by user.
    // This is corresponding to tidb code:
    // 1. LowPriorityValue is 1.
    // 2. MediumPriorityValue is 8.
    // 3. HighPriorityValue is 16.
    uint32_t user_priority;
    
    // Definition of the RG, e.g. RG settings, priority etc.
    ::resource_manager::ResourceGroup group_pb;

    mutable std::mutex mu;

    // Local token bucket.
    TokenBucketPtr bucket;

    // Total used cpu_time of this ResourceGroup.
    std::atomic<uint64_t> cpu_time;

    std::chrono::time_point<std::chrono::steady_clock> last_fetch_tokens_from_gac_timepoint;

    bool in_degrade_mode = false;

    std::chrono::steady_clock::time_point last_gac_update_timepoint;
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
class LocalAdmissionController final
{
public:
    explicit LocalAdmissionController(MPPTaskManagerPtr mpp_task_manager_)
        : last_cleanup_resource_group_timepoint(std::chrono::steady_clock::now())
        , mpp_task_manager(mpp_task_manager_)
    {
        // gjt todo: what if error code?
        // gjt todo: how to stop?
        newThreadManager()->scheduleThenDetach(true, "LocalAdmissionController", [this] { this->startBackgroudJob(); });
    }
    ~LocalAdmissionController(); // thread_manager wait

    static std::unique_ptr<LocalAdmissionController> global_instance;

    bool consumeResource(const std::string & name, double ru, uint64_t cpu_time)
    {
        ResourceGroupPtr group = getOrCreateResourceGroup(name);
        bool consumed = group->consumeResource(ru, cpu_time);
        if (group->lowToken())
            cv.notify_one();
        return consumed;
    }

    double getPriority(const std::string & name)
    {
        ResourceGroupPtr group = getOrCreateResourceGroup(name);
        return group->getPriority();
    }
private:
    // Get ResourceGroup by name, if not exist, fetch from PD.
    // If you are sure this resource group exists in GAC, you can skip the check.
    ResourceGroupPtr getOrCreateResourceGroup(const std::string & name);
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

    std::pair<ResourceGroupPtr, bool> addResourceGroup(const ::resource_manager::ResourceGroup & new_group_pb)
    {
        std::lock_guard lock(mu);
        for (const auto & group : resource_groups)
        {
            if (group.first == new_group_pb.name())
                return std::make_pair(group.second, false);
        }
        
        auto new_group = std::make_shared<ResourceGroup>(new_group_pb);
        resource_groups.insert({new_group_pb.name(), new_group});
        return std::make_pair(new_group, true);
    }

    void handleTokenBucketsResp(const resource_manager::TokenBucketsResponse & resp);

    void handleBackgroundError(const std::string & err_msg);

    // Interval of fetch from GAC periodically.
    static constexpr uint64_t DEFAULT_FETCH_GAC_INTERVAL = 5;
    // DEFAULT_TOKEN_FETCH_ESAPSED * token_avg_consumption_speed as token num to fetch from GAC.
    static constexpr uint64_t DEFAULT_TOKEN_FETCH_ESAPSED = 5;
    // Interval of cleanup resource group.
    static constexpr auto CLEANUP_RESOURCE_GROUP_INTERVAL = std::chrono::minutes(10);
    // gjt todo sync with tidb.
    static constexpr auto DEGRADE_MODE_DURATION = 0;

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

    bool started = false;

    std::atomic<bool> stopped = false;

    std::unordered_map<std::string, ResourceGroupPtr> resource_groups;

    ::pingcap::kv::Cluster * cluster = nullptr;

    const LoggerPtr log;

    // No need to use lock, only be accessed by background thread.
    std::chrono::time_point<std::chrono::steady_clock> last_cleanup_resource_group_timepoint;

    // To cleanup MinTSOScheduler of resource group.
    MPPTaskManagerPtr mpp_task_manager;
};
} // namespace DB
