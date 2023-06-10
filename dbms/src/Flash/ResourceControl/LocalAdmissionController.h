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

#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Flash/Pipeline/Schedule/TaskQueues/MultiLevelFeedbackQueue.h>
#include <Flash/ResourceControl/TokenBucket.h>
#include <pingcap/kv/Cluster.h>
#include <Common/ThreadManager.h>

#include <kvproto/resource_manager.pb.h>

#include <atomic>
#include <memory>
#include <mutex>

namespace DB
{
class ResourceGroup final
{
public:
    explicit ResourceGroup(const ::resource_manager::ResourceGroup & group_pb_)
        : name(group_pb_.name())
        , group_pb(group_pb_)
        , cpu_time(0)
    {
        const auto & setting = group_pb.r_u_settings().r_u().settings();
        bucket = std::make_unique<TokenBucket>(setting.fill_rate(), setting.fill_rate(), setting.burst_limit());
    }

    ~ResourceGroup() = default;

    std::string getName() const { return name; }

    bool consumeResource(double ru, uint64_t cpu_time_)
    { 
        cpu_time += cpu_time_;
        std::lock_guard lock(mu);
        return bucket->consume(ru);
    }

    double getRU() const
    { 
        std::lock_guard lock(mu);
        return bucket->peek();
    }

    double getCPUTime() const
    {
        return cpu_time.load();
    }

    double getAcquireRUNum() const
    {
        // gjt todo
    }

    // gjt todo
    double getPriority() const
    {
    }

    // // Pipeline tasks are for computing layer, other tasks can add here later.
    // void addPipelineTask(TaskPtr && task);
    // void addPipelineTask(std::vector<TaskPtr> & tasks);
    // bool getPipelineTaskByMLFQ(TaskPtr & task);

    // 1. New tokens fetched from GAC, update remaining tokens.
    // 2. Tokens of GAC is not enough, enter trickling mode.
    // 3. If we have network problem with GAC, enter degrade mode.
    void reConfigTokenBucket(double refill_rate, double capacity)
    {
        std::lock_guard lock(mu);
        bucket->reConfig(refill_rate, capacity);
    }

private:
    mutable std::mutex mu;

    std::string name;
    
    // Definition of the RG, e.g. RG settings, priority etc.
    ::resource_manager::ResourceGroup group_pb;

    // Local token bucket.
    TokenBucketPtr bucket;

    // // All pipeline tasks of queries within this group.
    // std::vector<TaskPtr> pipeline_tasks;

    // // So pipeline tasks can run considering CPU time.
    // CPUMultiLevelFeedbackQueue pipeline_task_queue;

    // Total used cpu_time of this ResourceGroup.
    std::atomic<uint64_t> cpu_time;
};

using ResourceGroupPtr = std::shared_ptr<ResourceGroup>;

// LocalAdmissionController is the local(tiflash) part of the distributed token bucket algorithm.
// 1. It manages all ResourceGroups for one tiflash node.
//   1. create: Fetch info from GAC if RG not found in LAC.
//   2. delete: Cleanup deleted RG from LAC periodically.
//   3. update: gjt todo
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
    // gjt todo
    LocalAdmissionController()
    {
        // gjt todo: what if error code?
        newThreadManager()->scheduleThenDetach(true, "LocalAdmissionController", [this] { this->startBackgroudJob(); });
    }

    // Get ResourceGroup by name, if not exist, fetch from PD.
    ResourceGroupPtr getOrCreateResourceGroup(const std::string & name);

    // Get highest priority of ResourceGroup.
    ResourceGroupPtr getResourceGroupByPriority();

    static std::unique_ptr<LocalAdmissionController> global_instance;

private:
    ResourceGroupPtr findResourceGroup(const std::string & name)
    {
        std::lock_guard lock(mu);
        for (auto & group : resource_groups)
        {
            if (group->getName() == name)
                return group;
        }
        return nullptr;
    }

    std::pair<ResourceGroupPtr, bool> addResourceGroup(const ::resource_manager::ResourceGroup & new_group_pb)
    {
        std::lock_guard lock(mu);
        for (auto & group : resource_groups)
        {
            if (group->getName() == new_group_pb.name())
                return std::make_pair(group, false);
        }

        
        resource_groups.emplace_back(std::make_shared<ResourceGroup>(new_group_pb));
        return std::make_pair(resource_groups.back(), true);
    }

    double calcPriority(double ru, double cpu_time) const
    {
        // gjt todo
    }

    void handleBackgroundError(const std::string & err_msg);

    // Interval of fetch from GAC periodically.
    static const uint64_t DEFAULT_FETCH_GAC_INTERVAL = 5;

    // DEFAULT_TOKEN_FETCH_ESAPSED * token_avg_consumption_speed as token num to fetch from GAC.
    static const uint64_t DEFAULT_TOKEN_FETCH_ESAPSED = 5;

    void startBackgroudJob();

    void fetchFromGAC();

    std::mutex mu;

    std::condition_variable cv;

    bool started = false;
    std::atomic<bool> stopped = false;

    std::vector<ResourceGroupPtr> resource_groups;

    ::pingcap::kv::Cluster * cluster = nullptr;

    const LoggerPtr log;
};
} // namespace DB
