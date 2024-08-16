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
    explicit ResourceGroup(const resource_manager::ResourceGroup & group_pb_)
        : name(group_pb_.name())
        , group_pb(group_pb_)
        , log(Logger::get("resource group:" + group_pb_.name()))
    {
        resetResourceGroup(group_pb_);
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

    void initStaticTokenBucket(int64_t capacity = std::numeric_limits<int64_t>::max())
    {
        std::lock_guard lock(mu);
        // If token bucket is normal mode, it's static, so fill_rate is zero.
        const double init_fill_rate = 0.0;
        const double init_tokens = user_ru_per_sec;
        int64_t init_cap = capacity;
        if (capacity < 0)
            init_cap = std::numeric_limits<int64_t>::max();
        bucket = std::make_unique<TokenBucket>(init_fill_rate, init_tokens, log->identifier(), init_cap);
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
        ru_consumption_delta += ru;
        if (!burstable)
        {
            bucket->consume(ru);
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
    uint64_t getPriority(uint64_t max_ru_per_sec) const
    {
        std::lock_guard lock(mu);

        const auto remaining_token = bucket->peek();
        if (!burstable && remaining_token <= 0.0)
        {
            GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_compute_ru_exhausted, name).Increment();
            return std::numeric_limits<uint64_t>::max();
        }

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
            "getPriority detailed info: resource group name: {}, weight: {}, virtual_time: {}, "
            "user_priority: {}, "
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
    void resetResourceGroup(const resource_manager::ResourceGroup & group_pb_)
    {
        std::lock_guard lock(mu);
        group_pb = group_pb_;
        user_priority = group_pb_.priority();
        const auto & setting = group_pb.r_u_settings().r_u().settings();
        user_ru_per_sec = setting.fill_rate();
        burstable = (setting.burst_limit() < 0);
        assert(
            user_priority == LowPriorityValue || user_priority == MediumPriorityValue
            || user_priority == HighPriorityValue);
    }

    bool lowToken() const
    {
        std::lock_guard lock(mu);
        return !burstable && bucket->lowToken();
    }

    // Return how many tokens should acquire from GAC for the next n seconds.
    double getAcquireRUNum(double speed, uint32_t n_sec, double amplification) const
    {
        assert(amplification >= 1.0);

        double remaining_ru = 0.0;
        double base = 0.0;
        {
            std::lock_guard lock(mu);
            remaining_ru = bucket->peek();
            base = static_cast<double>(user_ru_per_sec);
        }

        // Appropriate amplification is necessary to prevent situation that GAC has sufficient RU,
        // but user query speed is limited due to LAC requests too few RU.
        double acquire_num = speed * n_sec * amplification;

        // Prevent avg_speed from being 0 due to RU exhaustion.
        if (acquire_num == 0.0 && remaining_ru <= 0.0)
            acquire_num = base;

        acquire_num -= remaining_ru;
        acquire_num = (acquire_num > 0.0 ? acquire_num : 0.0);

        LOG_TRACE(
            log,
            "acquire num for rg {}: avg_speed: {}, remaining_ru: {}, base: {}, amplification: {}, "
            "acquire num: {}",
            name,
            speed,
            remaining_ru,
            base,
            amplification,
            acquire_num);
        return acquire_num;
    }

    void updateNormalMode(double add_tokens, double new_capacity)
    {
        assert(add_tokens >= 0);

        std::lock_guard lock(mu);
        bucket_mode = TokenBucketMode::normal_mode;
        if (new_capacity <= 0.0)
        {
            burstable = true;
            return;
        }
        auto config = bucket->getConfig();
        std::string ori_bucket_info = bucket->toString();

        config.tokens += add_tokens;
        config.fill_rate = 0;
        config.capacity = new_capacity;
        bucket->reConfig(config);
        LOG_DEBUG(
            log,
            "token bucket of rg {} reconfig to normal mode. from: {}, to: {}",
            name,
            ori_bucket_info,
            bucket->toString());

        updateBucketMetrics(config);
    }

    void updateTrickleMode(double add_tokens, double new_capacity, int64_t trickle_ms)
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
            "token bucket of {} reconfig to trickle mode failed. add_tokens: {} trickle_ms: {}, trickle_sec: {}",
            name,
            add_tokens,
            trickle_ms,
            trickle_sec);

        std::string ori_bucket_info = bucket->toString();
        const auto ori_tokens = bucket->peek();
        bucket->reConfig(TokenBucket::TokenBucketConfig(ori_tokens, new_fill_rate, new_capacity));
        stop_trickle_timepoint = SteadyClock::now() + std::chrono::milliseconds(trickle_ms);
        LOG_DEBUG(
            log,
            "token bucket of rg {} reconfig to trickle mode: from: {}, to: {}",
            name,
            ori_bucket_info,
            bucket->toString());

        updateBucketMetrics(bucket->getConfig());
    }

    // If we have network problem with GAC, enter degrade mode.
    void toDegrademode()
    {
        std::lock_guard lock(mu);
        if (burstable || bucket_mode == degrade_mode)
            return;

        bucket_mode = TokenBucketMode::degrade_mode;
        auto config = bucket->getConfig();
        std::string ori_bucket_info = bucket->toString();

        config.fill_rate = ru_consumption_speed;
        bucket->reConfig(config);
        LOG_INFO(
            log,
            "token bucket of rg {} reconfig to degrade mode done: {}",
            name,
            ori_bucket_info,
            bucket->toString());

        updateBucketMetrics(config);
    }

    struct ConsumptionUpdateInfo
    {
        // Avg speed of RU consumption of time range [last_update_ru_consumption_timepoint, now].
        double speed = 0.0;
        // RU consumption since last_update_ru_consumption_timepoint.
        double delta = 0.0;
        // Total RU consumption of all time.
        double total = 0.0;
        // If speed or delta is updated or not.
        bool updated = false;
    };

    ConsumptionUpdateInfo updateConsumptionSpeedInfoIfNecessary(
        const SteadyClock::time_point & now,
        const std::chrono::seconds & dura)
    {
        ConsumptionUpdateInfo info;
        {
            std::lock_guard lock(mu);
            const auto elapsed
                = std::chrono::duration_cast<std::chrono::seconds>(now - last_update_ru_consumption_timepoint);

            if (elapsed < dura)
                return {.speed = ru_consumption_speed, .delta = ru_consumption_delta, .updated = false};

            ru_consumption_speed = ru_consumption_delta / elapsed.count();
            total_ru_consumption += ru_consumption_delta;

            info.speed = ru_consumption_speed;
            info.total = total_ru_consumption;
            info.delta = ru_consumption_delta;
            info.updated = true;

            ru_consumption_delta = 0;
            last_update_ru_consumption_timepoint = now;
        }

        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_avg_speed, name).Set(info.speed);
        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_total_consumption, name).Set(info.total);
        return info;
    }

    bool needFetchToken(const SteadyClock::time_point & now, const std::chrono::seconds & dura) const
    {
        std::lock_guard lock(mu);
        return std::chrono::duration_cast<std::chrono::seconds>(now - last_fetch_tokens_from_gac_timepoint) > dura;
    }

    void updateFetchTokenTimepoint(const SteadyClock::time_point & tp)
    {
        std::lock_guard lock(mu);
        assert(last_fetch_tokens_from_gac_timepoint <= tp);
        last_fetch_tokens_from_gac_timepoint = tp;
        ++fetch_tokens_from_gac_count;
    }

    bool inTrickleModeLease(const SteadyClock::time_point & tp)
    {
        std::lock_guard lock(mu);
        return bucket_mode == trickle_mode && tp < stop_trickle_timepoint;
    }

    bool trickleModeLeaseExpire(const SteadyClock::time_point & tp)
    {
        std::lock_guard lock(mu);
        return bucket_mode == trickle_mode && tp >= stop_trickle_timepoint;
    }

    void updateBucketMetrics(const TokenBucket::TokenBucketConfig & config) const
    {
        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_bucket_fill_rate, name).Set(config.fill_rate);
        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_bucket_capacity, name).Set(config.capacity);
        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_remaining_tokens, name).Set(config.tokens);
    }

    const std::string name;

    uint32_t user_priority = 0;
    uint64_t user_ru_per_sec = 0;

    bool burstable = false;

    resource_manager::ResourceGroup group_pb;

    mutable std::mutex mu;

    // Local token bucket.
    TokenBucketPtr bucket;

    // Total used cpu_time_in_ns of this ResourceGroup.
    uint64_t cpu_time_in_ns = 0;

    TokenBucketMode bucket_mode = TokenBucketMode::normal_mode;

    LoggerPtr log;

    SteadyClock::time_point last_fetch_tokens_from_gac_timepoint = SteadyClock::now();
    SteadyClock::time_point stop_trickle_timepoint = SteadyClock::now();
    uint64_t fetch_tokens_from_gac_count = 0;
    double total_ru_consumption = 0.0;

    double ru_consumption_delta = 0.0;
    double ru_consumption_speed = 0.0;
    SteadyClock::time_point last_update_ru_consumption_timepoint = SteadyClock::now();
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
    // For tidb_enable_resource_control is disabled.
    static constexpr uint64_t HIGHEST_RESOURCE_GROUP_PRIORITY = 0;

    LocalAdmissionController(::pingcap::kv::Cluster * cluster_, Etcd::ClientPtr etcd_client_)
        : cluster(cluster_)
        , etcd_client(etcd_client_)
        , watch_gac_grpc_context(std::make_unique<grpc::ClientContext>())
    {
        background_threads.emplace_back([this] { this->startBackgroudJob(); });
        background_threads.emplace_back([this] { this->watchGAC(); });
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
        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_compute_ru_consumption, name).Set(ru);
    }

    void consumeBytesResource(const std::string & name, double ru)
    {
        consumeResource(name, ru, 0);
        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_storage_ru_consumption, name).Set(ru);
    }

    uint64_t estWaitDuraMS(const std::string & name) const
    {
        if (name.empty())
            return 0;

        ResourceGroupPtr group = findResourceGroup(name);
        if unlikely (!group)
        {
            LOG_DEBUG(log, "cannot get priority for {}, maybe it has been deleted", name);
            return 0;
        }
        return group->estWaitDuraMS(DEFAULT_FETCH_GAC_INTERVAL_MS);
    }

    std::optional<uint64_t> getPriority(const std::string & name)
    {
        assert(!stopped);

        if (name.empty())
            return {HIGHEST_RESOURCE_GROUP_PRIORITY};

        ResourceGroupPtr group = findResourceGroup(name);
        if unlikely (!group)
        {
            LOG_DEBUG(log, "cannot get priority for {}, maybe it has been deleted", name);
            return std::nullopt;
        }

        return {group->getPriority(max_ru_per_sec.load())};
    }

    // Fetch resource group info from GAC if necessary and store in local cache.
    // Throw exception if got error when fetching from GAC.
    void warmupResourceGroupInfoCache(const std::string & name);

    static bool isRUExhausted(uint64_t priority) { return priority == std::numeric_limits<uint64_t>::max(); }

    void registerRefillTokenCallback(const std::function<void()> & cb)
    {
        assert(!stopped);
        // NOTE: Better not use lock inside refill_token_callback,
        // because LAC needs to lock when calling refill_token_callback,
        // which may introduce dead lock.
        std::lock_guard lock(mu);
        RUNTIME_CHECK_MSG(refill_token_callback == nullptr, "callback cannot be registered multiple times");
        refill_token_callback = cb;
    }

    void unregisterRefillTokenCallback()
    {
        assert(!stopped);
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
    static constexpr auto DEFAULT_FETCH_GAC_INTERVAL = std::chrono::seconds(5);
    static constexpr auto DEFAULT_FETCH_GAC_INTERVAL_MS = 5000;

private:
    void consumeResource(const std::string & name, double ru, uint64_t cpu_time_in_ns)
    {
        assert(!stopped);

        // When tidb_enable_resource_control is disabled, resource group name is empty.
        if (name.empty())
            return;

        ResourceGroupPtr group = findResourceGroup(name);
        if unlikely (!group)
        {
            LOG_INFO(log, "cannot consume ru for {}, maybe it has been deleted", name);
            return;
        }

        group->consumeResource(ru, cpu_time_in_ns);
        if (group->lowToken() || group->trickleModeLeaseExpire(SteadyClock::now()))
        {
            {
                std::lock_guard lock(mu);
                low_token_resource_groups.insert(name);
            }
            cv.notify_one();
        }
    }

    void stop()
    {
        if (stopped)
            return;
        stopped.store(true);

        // TryCancel() is thread safe(https://github.com/grpc/grpc/pull/30416).
        // But we need to create a new grpc_context for each new grpc reader/writer(https://github.com/grpc/grpc/issues/18348#issuecomment-477402608). So need to lock.
        {
            std::lock_guard lock(mu);
            watch_gac_grpc_context->TryCancel();
        }
        cv.notify_all();
        for (auto & thread : background_threads)
        {
            if (thread.joinable())
                thread.join();
        }

        // Report final RU consumption before stop:
        // 1. to avoid RU consumption omission.
        // 2. clear GAC's unique_client_id to avoid affecting burst limit calculation.
        // This can happend when disagg CN is scaled-in/out frequently.
        std::vector<AcquireTokenInfo> acquire_infos;
        for (const auto & resource_group : resource_groups)
        {
            const auto consumption_update_info = resource_group.second->updateConsumptionSpeedInfoIfNecessary(
                SteadyClock::time_point::max(),
                std::chrono::seconds(0));
            assert(consumption_update_info.updated);
            acquire_infos.push_back(
                {.resource_group_name = resource_group.first,
                 .acquire_tokens = 0,
                 .ru_consumption_delta = consumption_update_info.delta});
        }
        fetchTokensFromGAC(acquire_infos, "before stop", true);

        if (need_reset_unique_client_id.load())
        {
            try
            {
                etcd_client->deleteServerIDFromGAC(unique_client_id);
                LOG_DEBUG(log, "delete server id({}) from GAC succeed", unique_client_id);
            }
            catch (...)
            {
                LOG_ERROR(
                    log,
                    "delete server id({}) from GAC failed: {}",
                    unique_client_id,
                    getCurrentExceptionMessage(false));
            }
        }
    }

    // If we cannot get GAC resp for DEGRADE_MODE_DURATION seconds, enter degrade mode.
    static constexpr auto DEGRADE_MODE_DURATION = std::chrono::seconds(120);
    static constexpr auto TARGET_REQUEST_PERIOD_MS = std::chrono::milliseconds(5000);
    static constexpr double ACQUIRE_RU_AMPLIFICATION = 1.5;

    static const std::string GAC_RESOURCE_GROUP_ETCD_PATH;
    static const std::string WATCH_GAC_ERR_PREFIX;

    // findResourceGroup() should be private,
    // this is to avoid user call member function of ResourceGroup directly.
    // So we can avoid dead lock.
    ResourceGroupPtr findResourceGroup(const std::string & name) const
    {
        std::lock_guard lock(mu);
        auto iter = resource_groups.find(name);
        return iter == resource_groups.end() ? nullptr : iter->second;
    }

    void addResourceGroup(const resource_manager::ResourceGroup & new_group_pb)
    {
        uint64_t user_ru_per_sec = new_group_pb.r_u_settings().r_u().settings().fill_rate();
        if (max_ru_per_sec.load() < user_ru_per_sec)
            max_ru_per_sec.store(user_ru_per_sec);

        std::lock_guard lock(mu);
        auto iter = resource_groups.find(new_group_pb.name());
        if (iter != resource_groups.end())
            return;

        LOG_INFO(log, "add new resource group, info: {}", new_group_pb.DebugString());
        auto new_group = std::make_shared<ResourceGroup>(new_group_pb);
        resource_groups.insert({new_group_pb.name(), new_group});

        if (refill_token_callback)
            refill_token_callback();
    }

    std::vector<std::string> handleTokenBucketsResp(const resource_manager::TokenBucketsResponse & resp);

    static void checkGACRespValid(const resource_manager::ResourceGroup & new_group_pb);

    struct AcquireTokenInfo
    {
        std::string resource_group_name;
        double acquire_tokens;
        double ru_consumption_delta;

        std::string toString() const
        {
            FmtBuffer fmt_buf;
            fmt_buf.fmtAppend(
                "rg: {}, acquire_tokens: {}, ru_consumption_delta: {}",
                resource_group_name,
                acquire_tokens,
                ru_consumption_delta);
            return fmt_buf.toString();
        }
    };

    // Background jobs:
    // 1. Fetch tokens from GAC periodically.
    // 2. Fetch tokens when low threshold is triggered.
    // 3. Check if resource group need to goto degrade mode.
    // 4. Watch GAC event to delete resource group.
    void startBackgroudJob();
    void fetchTokensFromGAC(
        const std::vector<AcquireTokenInfo> & acquire_infos,
        const std::string & desc_str,
        bool is_final_report = false);
    void checkDegradeMode();
    void watchGAC();

    // Utilities for fetch token from GAC.
    void fetchTokensForLowTokenResourceGroups();
    void fetchTokensForAllResourceGroups();
    static std::optional<AcquireTokenInfo> buildAcquireInfo(
        const ResourceGroupPtr & resource_group,
        bool is_periodically_fetch);

    // Watch GAC utilities.
    void doWatch();
    static etcdserverpb::WatchRequest setupWatchReq();
    bool handleDeleteEvent(const mvccpb::KeyValue & kv, std::string & err_msg);
    bool handlePutEvent(const mvccpb::KeyValue & kv, std::string & err_msg);
    static bool parseResourceGroupNameFromWatchKey(
        const std::string & etcd_key,
        std::string & parsed_rg_name,
        std::string & err_msg);

    mutable std::mutex mu;
    std::condition_variable cv;

    std::atomic<bool> stopped = false;

    std::unordered_map<std::string, ResourceGroupPtr> resource_groups;
    std::unordered_set<std::string> low_token_resource_groups;

    std::atomic<uint64_t> max_ru_per_sec = 0;
    std::chrono::time_point<SteadyClock> last_fetch_tokens_from_gac_timepoint = SteadyClock::now();

    ::pingcap::kv::Cluster * cluster = nullptr;
    std::atomic<bool> need_reset_unique_client_id{false};
    uint64_t unique_client_id = 0;
    Etcd::ClientPtr etcd_client = nullptr;
    std::unique_ptr<grpc::ClientContext> watch_gac_grpc_context = nullptr;
    std::vector<std::thread> background_threads;

    std::function<void()> refill_token_callback;

    const LoggerPtr log = Logger::get("LocalAdmissionController");
};

// This is to reduce the calling frequence of LAC::consumeResource() to avoid lock contension.
// TODO: Need to optimize LAC::consumeResource().
// Because the lock contension still increase when the thread num of storage layer or the data to be read is very large.
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
        // Call LAC::consumeResource() when accumulated to `bytes_of_one_hundred_ru` to avoid lock contension.
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
