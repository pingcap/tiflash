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
#include <Common/Logger.h>
#include <Common/TiFlashMetrics.h>
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
#include <magic_enum.hpp>
#include <memory>
#include <mutex>

namespace DB
{
class LocalAdmissionController;
using SteadyClock = std::chrono::steady_clock;

struct GACRequestInfo
{
    std::string resource_group_name;
    double acquire_tokens;
    double ru_consumption_delta;

    std::string toString() const
    {
        return fmt::format(
            "rg: {}, acquire_tokens: {}, ru_consumption_delta: {}",
            resource_group_name,
            acquire_tokens,
            ru_consumption_delta);
    }
};

struct LACRUConsumptionDeltaInfo
{
    double speed = 0.0;
    double delta = 0.0;
};

// TODO need also support burst_limit == -2
// gac_resp.burst_limit < 0: resource group is burstable, and will not use bucket at all.
// gac_resp.burst_limit >= 0: resource group is not burstable, will use bucket to limit the speed of the resource group.
//     1. normal_mode: bucket is static(a.k.a. bucket.fill_rate is zero), LAC will fetch tokens from GAC to fill bucket.
//     2. degrade_mode: when lost connection with GAC for 120s, bucket will enter degrade_mode.
//     3. trickle_mode: when tokens is running out of tokens, bucket will enter trickle_mode.
//                      GAC will assign X tokens and Y trickle_ms. And the bucket fill rate should be X/Y.
//                      bucket is dynamic(a.k.a. bucket.fill_rate is greater than zero) in degrade_mode and trickle_mode.
// NOTE: Member function of ResourceGroup should only be called by LocalAdmissionController,
// so we can make sure the lock order of LocalAdmissionController::mu is always before ResourceGroup::mu,
// which helps to avoid dead lock.
class ResourceGroup final : private boost::noncopyable
{
public:
    explicit ResourceGroup(const resource_manager::ResourceGroup & group_pb_, const SteadyClock::time_point & tp)
        : name(group_pb_.name())
        , group_pb(group_pb_)
        , log(Logger::get("resource group:" + group_pb_.name()))
    {
        resetResourceGroup(group_pb_);
        const auto & setting = group_pb.r_u_settings().r_u().settings();
        initStaticTokenBucket(setting.burst_limit());

        last_compute_ru_consumption_speed = tp;
        last_request_gac_timepoint = tp;

        degrade_deadline = SteadyClock::time_point::max();
        trickle_expire_timepoint = SteadyClock::time_point::min();
        trickle_deadline = SteadyClock::time_point::min();
    }

#ifdef DBMS_PUBLIC_GTEST
    ResourceGroup(const std::string & group_name_, uint32_t user_priority_, uint64_t user_ru_per_sec_, bool burstable_)
        : name(group_name_)
        , user_priority_val(getUserPriorityVal(user_priority_))
        , user_ru_per_sec(user_ru_per_sec_)
        , burstable(burstable_)
        , log(Logger::get("resource group:" + group_name_))
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

    void initStaticTokenBucket(int64_t capacity);

    static constexpr auto USER_PRIORITY_BITS = 4;
    // UserXXXPriority is specified by tidb: parser/model/model.go
    static constexpr int32_t UserLowPriority = 1;
    static constexpr int32_t UserMediumPriority = 8;
    static constexpr int32_t UserHighPriority = 16;
    // XXXPriorityValue is used to calculate priority for pipeline engine scheduling.
    static constexpr int32_t LowPriorityValue = 15;
    static constexpr int32_t MediumPriorityValue = 7;
    static constexpr int32_t HighPriorityValue = 0;

    // Minus 1 because uint64 max is used as special flag.
    static constexpr uint64_t MAX_VIRTUAL_TIME = (std::numeric_limits<uint64_t>::max() >> USER_PRIORITY_BITS) - 1;
    static constexpr double MOVING_RU_CONSUMPTION_SPEED_FACTOR = 0.5;
    static constexpr auto COMPUTE_RU_CONSUMPTION_SPEED_INTERVAL = std::chrono::seconds(1);
    static constexpr auto REPORT_RU_CONSUMPTION_DELTA_THRESHOLD = 100;
    static constexpr auto EXTENDING_REPORT_RU_CONSUMPTION_FACTOR = 4;
    static constexpr auto DEFAULT_BUFFER_TOKENS = 5000;

    // Indicate the round trip time of gac request.
    static constexpr auto GAC_RTT_ANTICIPATION = std::chrono::seconds(1);

    friend class LocalAdmissionController;
    friend class MockLocalAdmissionController;

    std::string getName() const { return name; }

    void consumeResource(double ru, uint64_t cpu_time_in_ns_)
    {
        const auto now = SteadyClock::now();
        std::lock_guard lock(mu);
        cpu_time_in_ns += cpu_time_in_ns_;
        ru_consumption_delta += ru;
        ru_consumption_delta_for_compute_speed += ru;
        if (!burstable)
        {
            bucket->consume(ru, now);
            GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_remaining_tokens, name).Set(bucket->peek());
        }
    }

    uint64_t estWaitDuraMS(uint64_t max_wait_dura_ms) const
    {
        std::lock_guard lock(mu);
        return bucket->estWaitDuraMS(max_wait_dura_ms);
    }

    // Priority greater than zero: Less number means higher priority.
    // Zero priority means has no RU left, should not schedule this resource group at all.
    uint64_t getPriority(uint64_t max_ru_per_sec) const;
    bool lowToken() const
    {
        std::lock_guard lock(mu);
        return !burstable && bucket->lowToken();
    }

    // Related to sending GAC request.
    bool beginRequestWithoutLock(const SteadyClock::time_point & tp);
    void endRequestWithoutLock();
    void endRequest()
    {
        std::lock_guard lock(mu);
        endRequestWithoutLock();
    }
    bool shouldReportRUConsumption(const SteadyClock::time_point & now) const;
    std::optional<GACRequestInfo> buildRequestInfoIfNecessary(const SteadyClock::time_point & now);
    LACRUConsumptionDeltaInfo updateRUConsumptionDeltaInfoWithoutLock();
    double getAcquireRUNumWithoutLock(double speed, uint32_t n_sec, double amplification) const;
    void updateRUConsumptionSpeedIfNecessary(const SteadyClock::time_point & now);

    // Called when user change config of resource group.
    // Only update meta, will not touch runtime state(like bucket remaining tokens).
    void resetResourceGroup(const resource_manager::ResourceGroup & group_pb_)
    {
        std::lock_guard lock(mu);
        group_pb = group_pb_;
        user_priority_val = getUserPriorityVal(group_pb_.priority());
        const auto & setting = group_pb.r_u_settings().r_u().settings();
        user_ru_per_sec = setting.fill_rate();
        burstable = (setting.burst_limit() <= 0);
    }

    // Change bucket status according to the gac response.
    void updateNormalMode(double add_tokens, double new_capacity, const SteadyClock::time_point & now);
    void updateTrickleMode(
        double add_tokens,
        double new_capacity,
        int64_t trickle_ms,
        const SteadyClock::time_point & now);
    void updateDegradeMode(const SteadyClock::time_point & now);

    // Trickle mode related.
    bool okToAcquireTokenWithoutLock(const SteadyClock::time_point & tp) const
    {
        return !burstable && (bucket_mode != trickle_mode || trickleModeLeaseExpireWithoutLock(tp));
    }
    bool trickleModeLeaseExpire(const SteadyClock::time_point & tp) const
    {
        std::lock_guard lock(mu);
        return trickleModeLeaseExpireWithoutLock(tp);
    }
    bool trickleModeLeaseExpireWithoutLock(const SteadyClock::time_point & tp) const
    {
        return bucket_mode == trickle_mode && tp >= trickle_expire_timepoint;
    }
    double getTrickleLeftTokens(const SteadyClock::time_point & tp)
    {
        std::lock_guard lock(mu);
        if (bucket_mode == TokenBucketMode::trickle_mode && trickle_deadline > tp)
        {
            return static_cast<double>(
                       std::chrono::duration_cast<std::chrono::milliseconds>(trickle_deadline - tp).count()
                       * bucket->getConfig().fill_rate)
                / 1000.0;
        }
        return 0.0;
    }

    void updateBucketMetrics(const TokenBucket::TokenBucketConfig & config) const
    {
        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_bucket_fill_rate, name).Set(config.fill_rate);
        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_bucket_capacity, name).Set(config.capacity);
        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_remaining_tokens, name).Set(config.tokens);
        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_low_token_threshold, name)
            .Set(config.low_token_threshold);
    }

    void clearCPUTime()
    {
        std::lock_guard lock(mu);
        cpu_time_in_ns = 0;
    }

    static uint32_t getUserPriorityVal(uint32_t user_priority_from_pb)
    {
        switch (user_priority_from_pb)
        {
        case UserLowPriority:
            return LowPriorityValue;
        case UserMediumPriority:
            return MediumPriorityValue;
        case UserHighPriority:
            return HighPriorityValue;
        default:
            throw Exception(fmt::format("unexpected user priority: {}", user_priority_from_pb));
        }
    }

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    mutable std::mutex mu;

    // Meta info.
    const std::string name;
    uint32_t user_priority_val = 0;
    uint64_t user_ru_per_sec = 0;
    bool burstable = false;
    resource_manager::ResourceGroup group_pb;

    LoggerPtr log;

    // Local token bucket.
    TokenBucketPtr bucket;
    TokenBucketMode bucket_mode = TokenBucketMode::normal_mode;

    // For compute priority.
    uint64_t cpu_time_in_ns = 0;

    // For report to GAC.
    double ru_consumption_delta = 0.0;

    // For compute avg ru consumption speed.
    double ru_consumption_delta_for_compute_speed = 0.0;
    SteadyClock::time_point last_compute_ru_consumption_speed;
    double smooth_ru_consumption_speed = 1000;

    // To avoid too many request sent to GAC at the same time.
    bool request_in_progress = false;

    // Fro degrade mode.
    SteadyClock::time_point degrade_deadline;

    // To decide when to report ru consumption.
    SteadyClock::time_point last_request_gac_timepoint;
    // For trickle mode.
    SteadyClock::time_point trickle_expire_timepoint;
    SteadyClock::time_point trickle_deadline;
};

using ResourceGroupPtr = std::shared_ptr<ResourceGroup>;

// LocalAdmissionController is the local(tiflash) part of the distributed token bucket algorithm.
// It manages all resource groups:
// 1. Creation, deletion and config updates of resource group.
// 2. Fetching tokens from GAC periodically or when tokens are low.
// 3. Record/report resource consumption and the priority of each resource group.
class LocalAdmissionController final : private boost::noncopyable
{
public:
    LocalAdmissionController(::pingcap::kv::Cluster * cluster_, Etcd::ClientPtr etcd_client_)
        : cluster(cluster_)
        , etcd_client(etcd_client_)
        , watch_gac_grpc_context(std::make_unique<grpc::ClientContext>())
    {
        background_threads.emplace_back([this] { this->mainLoop(); });
        background_threads.emplace_back([this] { this->watchGACLoop(); });
        background_threads.emplace_back([this] { this->requestGACLoop(); });

        current_tick = SteadyClock::now();
        last_clear_cpu_time = current_tick;
    }

    ~LocalAdmissionController() { safeStop(); }

    void safeStop()
    {
        try
        {
            stop();
        }
        catch (...)
        {
            LOG_ERROR(log, "stop server id({}) failed: {}", unique_client_id, getCurrentExceptionMessage(false));
        }
    }

    void consumeCPUResource(const std::string & name, double ru, uint64_t cpu_time_in_ns)
    {
        consumeResource(name, ru, cpu_time_in_ns);
        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_compute_ru_consumption, name).Increment(ru);
    }

    void consumeBytesResource(const std::string & name, double ru)
    {
        consumeResource(name, ru, 0);
        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_storage_ru_consumption, name).Increment(ru);
    }

    uint64_t estWaitDuraMS(const std::string & name) const
    {
        if (unlikely(stopped))
            return 0;

        if (name.empty())
            return 0;

        ResourceGroupPtr group = findResourceGroup(name);
        if unlikely (!group)
        {
            LOG_DEBUG(log, "cannot get priority for {}, maybe it has been deleted", name);
            return 0;
        }
        return group->estWaitDuraMS(DEFAULT_MAX_EST_WAIT_DURATION.count());
    }

    std::optional<uint64_t> getPriority(const std::string & name)
    {
        if (unlikely(stopped))
            return {HIGHEST_RESOURCE_GROUP_PRIORITY};

        if (name.empty())
            return {HIGHEST_RESOURCE_GROUP_PRIORITY};

        auto [group, tmp_max_ru_per_sec] = findResourceGroupAndMaxRUPerSec(name);
        if unlikely (!group)
        {
            LOG_DEBUG(log, "cannot get priority for {}, maybe it has been deleted", name);
            return std::nullopt;
        }

        return {group->getPriority(tmp_max_ru_per_sec)};
    }

    // Fetch resource group info from GAC if necessary and store in local cache.
    // Throw exception if got error when fetching from GAC.
    void warmupResourceGroupInfoCache(const std::string & name);

    static bool isRUExhausted(uint64_t priority) { return priority == std::numeric_limits<uint64_t>::max(); }

    void registerRefillTokenCallback(const std::function<void()> & cb)
    {
        if unlikely (stopped.load())
            return;

        // NOTE: Better not use lock inside refill_token_callback,
        // because LAC needs to lock when calling refill_token_callback,
        // which may introduce dead lock.
        std::lock_guard lock(mu);
        RUNTIME_CHECK_MSG(refill_token_callback == nullptr, "callback cannot be registered multiple times");
        refill_token_callback = cb;
    }

    void unregisterRefillTokenCallback()
    {
        if (unlikely(stopped))
            return;

        std::lock_guard lock(mu);
        RUNTIME_CHECK_MSG(refill_token_callback != nullptr, "callback cannot be nullptr before unregistering");
        refill_token_callback = nullptr;
    }

#ifdef DBMS_PUBLIC_GTEST
    static std::unique_ptr<MockLocalAdmissionController> global_instance;
#else
    static std::unique_ptr<LocalAdmissionController> global_instance;
#endif

    // Interval of fetch from GAC periodically.
    static constexpr auto DEFAULT_TARGET_PERIOD = std::chrono::seconds(5);
    static constexpr auto DEFAULT_TARGET_PERIOD_MS
        = std::chrono::duration_cast<std::chrono::milliseconds>(DEFAULT_TARGET_PERIOD);
    static constexpr auto DEGRADE_MODE_DURATION = std::chrono::seconds(120);
    static constexpr double ACQUIRE_RU_AMPLIFICATION = 1.1;
    static constexpr auto DEFAULT_MAX_EST_WAIT_DURATION = std::chrono::milliseconds(1000);

private:
    static const std::string GAC_RESOURCE_GROUP_ETCD_PATH;
    static const std::string WATCH_GAC_ERR_PREFIX;
    static constexpr auto NETWORK_EXCEPTION_RETRY_DURATION_SEC = 3;

    // For tidb_enable_resource_control is disabled.
    static constexpr uint64_t HIGHEST_RESOURCE_GROUP_PRIORITY = 0;

    static constexpr auto CLEAR_CPU_TIME_DURATION = std::chrono::seconds(30);

    void consumeResource(const std::string & name, double ru, uint64_t cpu_time_in_ns)
    {
        if (unlikely(stopped))
            return;

        // When tidb_enable_resource_control is disabled, resource group name is empty.
        if (name.empty())
            return;

        ResourceGroupPtr group = findResourceGroup(name);
        if unlikely (!group)
        {
            LOG_DEBUG(log, "cannot consume ru for {}, maybe it has been deleted", name);
            return;
        }

        group->consumeResource(ru, cpu_time_in_ns);
        if (group->lowToken() || group->trickleModeLeaseExpire(SteadyClock::now()))
        {
            {
                std::lock_guard lock(mu);
                low_token_resource_groups.insert(name);
            }
            cv.notify_all();
        }
    }

    // findResourceGroup() should be private,
    // this is to avoid user call member function of ResourceGroup directly.
    // So we can avoid dead lock.
    ResourceGroupPtr findResourceGroup(const std::string & name) const
    {
        std::lock_guard lock(mu);
        auto iter = resource_groups.find(name);
        return iter == resource_groups.end() ? nullptr : iter->second;
    }
    std::pair<ResourceGroupPtr, uint64_t> findResourceGroupAndMaxRUPerSec(const std::string & name) const
    {
        std::lock_guard lock(mu);
        auto iter = resource_groups.find(name);
        auto rg = (iter == resource_groups.end() ? nullptr : iter->second);
        return {rg, max_ru_per_sec};
    }

    void addResourceGroup(const resource_manager::ResourceGroup & new_group_pb)
    {
        uint64_t user_ru_per_sec = new_group_pb.r_u_settings().r_u().settings().fill_rate();
        std::lock_guard lock(mu);

        if (max_ru_per_sec < user_ru_per_sec)
            max_ru_per_sec = user_ru_per_sec;

        auto iter = resource_groups.find(new_group_pb.name());
        if (iter != resource_groups.end())
            return;

        LOG_INFO(log, "add new resource group, info: {}", new_group_pb.ShortDebugString());
        auto new_group = std::make_shared<ResourceGroup>(new_group_pb, current_tick);
        resource_groups.insert({new_group_pb.name(), new_group});

        if (refill_token_callback)
            refill_token_callback();
    }

    std::vector<std::string> handleTokenBucketsResp(const resource_manager::TokenBucketsResponse & resp);

    static void checkGACRespValid(const resource_manager::ResourceGroup & new_group_pb);

    // 1. Fetch tokens from GAC when low token triggers.
    // 2. Report RU consumption.
    // 3. Check if resource group need to goto degrade mode.
    void mainLoop();
    // Watch GAC event to delete resource group.
    void watchGACLoop();
    // Send request to gac, separate from mainLoop() to avoid affect the ru consumption speed computation.
    void requestGACLoop();

    // mainLoop related methods.
    void updateRUConsumptionSpeed();
    std::optional<resource_manager::TokenBucketsRequest> buildGACRequest(bool is_final_report);
    void checkDegradeMode();

    // requestGACLoop related methods.
    void doRequestGAC();

    // watchGACLoop related methods.
    void doWatch();
    static etcdserverpb::WatchRequest setupWatchReq();
    bool handleDeleteEvent(const mvccpb::KeyValue & kv, std::string & err_msg);
    bool handlePutEvent(const mvccpb::KeyValue & kv, std::string & err_msg);
    static bool parseResourceGroupNameFromWatchKey(
        const std::string & etcd_key,
        std::string & parsed_rg_name,
        std::string & err_msg);
    void updateMaxRUPerSecAfterDeleteWithoutLock(uint64_t deleted_user_ru_per_sec);

    void clearCPUTime(const SteadyClock::time_point & now)
    {
        static_assert(CLEAR_CPU_TIME_DURATION > ResourceGroup::COMPUTE_RU_CONSUMPTION_SPEED_INTERVAL);
        std::lock_guard lock(mu);
        if (now - last_clear_cpu_time >= CLEAR_CPU_TIME_DURATION)
        {
            for (auto & resource_group : resource_groups)
                resource_group.second->clearCPUTime();
            last_clear_cpu_time = now;
        }
    }

    void stop();

private:
    mutable std::mutex mu;
    std::condition_variable cv;

    mutable std::mutex gac_requests_mu;
    std::condition_variable gac_requests_cv;
    std::vector<resource_manager::TokenBucketsRequest> gac_requests{};

    std::atomic<bool> stopped = false;

    std::unordered_map<std::string, ResourceGroupPtr> resource_groups{};
    std::unordered_set<std::string> low_token_resource_groups{};

    uint64_t max_ru_per_sec = 0;

    ::pingcap::kv::Cluster * cluster = nullptr;
    std::atomic<bool> need_reset_unique_client_id{false};
    uint64_t unique_client_id = 0;
    Etcd::ClientPtr etcd_client = nullptr;
    std::unique_ptr<grpc::ClientContext> watch_gac_grpc_context = nullptr;
    std::vector<std::thread> background_threads;

    SteadyClock::time_point current_tick = SteadyClock::time_point::min();
    SteadyClock::time_point last_clear_cpu_time = SteadyClock::time_point::min();

    std::function<void()> refill_token_callback = nullptr;

    const LoggerPtr log = Logger::get("LocalAdmissionController");
};

// This is to reduce the calling frequency of LAC::consumeResource() to avoid lock contention.
// TODO: Need to optimize LAC::consumeResource().
// Because the lock contention still increase when the thread num of storage layer or the data to be read is very large.
class LACBytesCollector
{
public:
    explicit LACBytesCollector(const std::string & name)
        : resource_group_name(name)
        , delta_bytes(0)
    {}

    ~LACBytesCollector()
    {
        if (delta_bytes != 0)
            consume();
    }

    void collect(uint64_t bytes)
    {
        delta_bytes += bytes;
        // Call LAC::consumeResource() when accumulated to `bytes_of_one_hundred_ru` to avoid lock contention.
        if (delta_bytes >= bytes_of_one_hundred_ru)
        {
            consume();
            delta_bytes = 0;
        }
    }

private:
    void consume()
    {
        assert(delta_bytes != 0);
        if (!resource_group_name.empty())
            LocalAdmissionController::global_instance->consumeBytesResource(
                resource_group_name,
                bytesToRU(delta_bytes));
    }

    const std::string resource_group_name;
    uint64_t delta_bytes;
};
using LACBytesCollectorPtr = std::unique_ptr<LACBytesCollector>;
} // namespace DB
