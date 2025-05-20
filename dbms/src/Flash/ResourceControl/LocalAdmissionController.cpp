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

#include <Flash/ResourceControl/LocalAdmissionController.h>
#include <etcd/rpc.pb.h>

#include <magic_enum.hpp>

namespace DB
{
void ResourceGroup::initStaticTokenBucket(int64_t capacity)
{
    std::lock_guard lock(mu);
    const double init_fill_rate = 0.0;
    // TODO maybe a better init value like user_ru_per_sec/instance_num,
    // for now just use user_ru_per_sec like tidb.
    const double init_tokens = user_ru_per_sec;
    int64_t init_cap = capacity;
    if (capacity < 0)
        init_cap = std::numeric_limits<int64_t>::max();
    bucket = std::make_unique<TokenBucket>(init_fill_rate, init_tokens, log->identifier(), init_cap);
}

uint64_t ResourceGroup::getPriority(uint64_t max_ru_per_sec) const
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

    return (((static_cast<uint64_t>(user_priority_val)) << 60) | virtual_time);
}

std::optional<GACRequestInfo> ResourceGroup::buildRequestInfoIfNecessary(const SteadyClock::time_point & now)
{
    std::lock_guard lock(mu);
    if (!beginRequestWithoutLock(now))
    {
        // A request has already been sent to GAC, skip sending.
        return {};
    }

    const auto consumption_delta_info = updateRUConsumptionDeltaInfoWithoutLock();
    double report_token_consumption = consumption_delta_info.delta;

    double acquire_tokens = 0.0;
    if (okToAcquireTokenWithoutLock(now))
    {
        acquire_tokens = getAcquireRUNumWithoutLock(
            consumption_delta_info.speed,
            LocalAdmissionController::DEFAULT_TARGET_PERIOD.count(),
            LocalAdmissionController::ACQUIRE_RU_AMPLIFICATION);

        assert(acquire_tokens >= 0.0);
    }

    if (report_token_consumption == 0.0 && acquire_tokens == 0.0)
    {
        endRequestWithoutLock();
        return std::nullopt;
    }
    else
    {
        return GACRequestInfo{
            .resource_group_name = name,
            .acquire_tokens = acquire_tokens,
            .ru_consumption_delta = report_token_consumption,
        };
    }
}

bool ResourceGroup::beginRequestWithoutLock(const SteadyClock::time_point & tp)
{
    if (request_in_progress)
        return false;
    request_in_progress = true;
    last_request_gac_timepoint = tp;
    degrade_deadline = tp + LocalAdmissionController::DEGRADE_MODE_DURATION;
    return true;
}

void ResourceGroup::endRequestWithoutLock()
{
    request_in_progress = false;
    degrade_deadline = SteadyClock::time_point::max();
}

bool ResourceGroup::shouldReportRUConsumption(const SteadyClock::time_point & now) const
{
    std::lock_guard lock(mu);
    const auto elapsed = now - last_request_gac_timepoint;
    RUNTIME_CHECK(elapsed.count() >= 0, elapsed.count());
    if (elapsed >= LocalAdmissionController::DEFAULT_TARGET_PERIOD)
    {
        if (ru_consumption_delta >= REPORT_RU_CONSUMPTION_DELTA_THRESHOLD)
            return true;
        if (elapsed >= LocalAdmissionController::DEFAULT_TARGET_PERIOD * EXTENDING_REPORT_RU_CONSUMPTION_FACTOR)
            return true;
    }
    return false;
}

double ResourceGroup::getAcquireRUNumWithoutLock(double speed, uint32_t n_sec, double amplification) const
{
    assert(amplification > 1.0);

    double remaining_ru = 0.0;
    remaining_ru = bucket->peek();

    // Appropriate amplification is necessary to prevent situation that GAC has sufficient RU,
    // but user query speed is limited due to LAC requests too few RU.
    double acquire_num = speed * n_sec * amplification;

    // This should not happen, but still add this to avoid stuck.
    if unlikely (acquire_num == 0.0 && remaining_ru == 0.0)
        acquire_num = DEFAULT_BUFFER_TOKENS;

    acquire_num -= remaining_ru;
    acquire_num = (acquire_num > 0.0 ? acquire_num : 0.0);
    return acquire_num;
}

void ResourceGroup::updateNormalMode(double add_tokens, double new_capacity, const SteadyClock::time_point & now)
{
    RUNTIME_CHECK(add_tokens >= 0, add_tokens);

    std::lock_guard lock(mu);
    endRequestWithoutLock();

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
    config.low_token_threshold = config.tokens * TokenBucket::LOW_TOKEN_THRESHOLD_RATE;
    bucket->reConfig(config, now);
    LOG_DEBUG(
        log,
        "token bucket of rg {} reconfig to normal mode. from: {}, to: {}",
        name,
        ori_bucket_info,
        bucket->toString());

    updateBucketMetrics(config);
}

void ResourceGroup::updateTrickleMode(
    double add_tokens,
    double new_capacity,
    int64_t trickle_ms,
    const SteadyClock::time_point & now)
{
    RUNTIME_CHECK(add_tokens >= 0.0, add_tokens);
    RUNTIME_CHECK(trickle_ms > 0, trickle_ms);

    std::lock_guard lock(mu);
    endRequestWithoutLock();

    if (new_capacity <= 0.0)
    {
        burstable = true;
        return;
    }

    bucket_mode = TokenBucketMode::trickle_mode;
    double new_fill_rate = add_tokens / (static_cast<double>(trickle_ms) / 1000);
    if unlikely (new_fill_rate <= 1.0)
        new_fill_rate = 1.0;

    std::string ori_bucket_info = bucket->toString();
    const auto ori_tokens = bucket->peek();
    bucket->reConfig(
        TokenBucket::TokenBucketConfig(ori_tokens, new_fill_rate, new_capacity, /*low_token_threshold_=*/0.0),
        now);

    const auto trickle_dura = std::chrono::milliseconds(trickle_ms);
    trickle_deadline = now + trickle_dura;
    trickle_expire_timepoint = trickle_deadline;
    if (trickle_dura >= 2 * GAC_RTT_ANTICIPATION)
        trickle_expire_timepoint = now + trickle_dura - GAC_RTT_ANTICIPATION;
    LOG_DEBUG(
        log,
        "token bucket of rg {} reconfig to trickle mode: from: {}, to: {}, trickle_dura: {}ms",
        name,
        ori_bucket_info,
        bucket->toString(),
        trickle_dura.count());

    updateBucketMetrics(bucket->getConfig());
}

void ResourceGroup::updateDegradeMode(const SteadyClock::time_point & now)
{
    // Disable degrade mode like tidb, just print log.
    // https://github.com/tikv/pd/blob/7c3b9a35139dc404f0782f8300d8d3f04c65aa17/client/resource_group/controller/config.go#L82
    // In normal mode, the LAC will exhaust all tokens, causing all queries to hang.
    // In trickle mode, the LAC will continue working using the original trickle refill rate.
    // And it will try to send a request to the GAC every DEGRADE_MODE_DURATION.
    std::lock_guard lock(mu);
    if (now >= degrade_deadline)
    {
        endRequestWithoutLock();
        LOG_INFO(
            log,
            "resource group({}) cannot receive gac response(bucket: {}, mode: {}) for {} seconds",
            name,
            bucket->toString(),
            magic_enum::enum_name(bucket_mode),
            std::chrono::duration_cast<std::chrono::seconds>(LocalAdmissionController::DEGRADE_MODE_DURATION).count());
        GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_enter_degrade_mode, name).Increment();
    }
}

void ResourceGroup::updateRUConsumptionSpeedIfNecessary(const SteadyClock::time_point & now)
{
    std::lock_guard lock(mu);

    const auto elapsed = now - last_compute_ru_consumption_speed;
    RUNTIME_CHECK(elapsed.count() >= 0, elapsed.count());
    if (elapsed < COMPUTE_RU_CONSUMPTION_SPEED_INTERVAL)
        return;

    // static_assert here because the computation assume time unit is seconds.
    static_assert(COMPUTE_RU_CONSUMPTION_SPEED_INTERVAL >= std::chrono::seconds(1));
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    auto current_ru_consumption_speed_per_sec = (ru_consumption_delta_for_compute_speed * 1000) / elapsed_ms;

    static_assert(MOVING_RU_CONSUMPTION_SPEED_FACTOR < 1);
    smooth_ru_consumption_speed = current_ru_consumption_speed_per_sec * MOVING_RU_CONSUMPTION_SPEED_FACTOR
        + (1 - MOVING_RU_CONSUMPTION_SPEED_FACTOR) * smooth_ru_consumption_speed;

    ru_consumption_delta_for_compute_speed = 0.0;
    last_compute_ru_consumption_speed = now;
    GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_avg_speed, name).Set(smooth_ru_consumption_speed);
}

LACRUConsumptionDeltaInfo ResourceGroup::updateRUConsumptionDeltaInfoWithoutLock()
{
    // Will be called:
    // 1. got low token OR
    // 2. report ru consumption
    LACRUConsumptionDeltaInfo info;

    info.speed = smooth_ru_consumption_speed;
    info.delta = ru_consumption_delta;

    GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_total_consumption, name).Increment(ru_consumption_delta);

    ru_consumption_delta = 0;
    return info;
}

void LocalAdmissionController::warmupResourceGroupInfoCache(const std::string & name)
{
    if (unlikely(stopped))
        return;

    if (name.empty())
        return;

    ResourceGroupPtr group = findResourceGroup(name);
    if (group != nullptr)
        return;

    resource_manager::GetResourceGroupRequest req;
    req.set_resource_group_name(name);
    resource_manager::GetResourceGroupResponse resp;

    try
    {
        resp = cluster->pd_client->getResourceGroup(req);
    }
    catch (...)
    {
        throw ::DB::Exception(
            ErrorCodes::LOGICAL_ERROR,
            "warmupResourceGroupInfoCache({}) failed: {}",
            name,
            getCurrentExceptionMessage(false));
    }

    RUNTIME_CHECK_MSG(!resp.has_error(), "warmupResourceGroupInfoCache({}) failed: {}", name, resp.error().message());

    checkGACRespValid(resp.group());

    addResourceGroup(resp.group());
}

void LocalAdmissionController::mainLoop()
{
    while (!stopped.load())
    {
        try
        {
            // If the unique_client_id cannot be successfully obtained from GAC for a long time, then the behavior of resource control is:
            // when the resource group has consumed RU, all queries cannot be scheduled anymore.
            unique_client_id = etcd_client->acquireServerIDFromGAC();
            need_reset_unique_client_id.store(true);
        }
        catch (...)
        {
            // Needs to catch in case we got context timeout error.
            LOG_ERROR(
                log,
                "get unique_client_id from PD error: {}, resource control may not work properly, try again later",
                getCurrentExceptionMessage(false));
            std::this_thread::sleep_for(std::chrono::seconds(5));
            continue;
        }
        break;
    }
    LOG_INFO(log, "get unique_client_id succeed: {}", unique_client_id);

    // Wakeup every n seconds to:
    // 1. compute RU consumption speed(COMPUTE_RU_CONSUMPTION_SPEED_INTERVAL, default 1s)
    // 2. report RU consumption to GAC(DEFAULT_TARGET_PERIOD, default 5s)
    // 3. check if need to step into degrade mode(DEGRADE_MODE_DURATION, default 120s)
    constexpr auto tick_interval = ResourceGroup::COMPUTE_RU_CONSUMPTION_SPEED_INTERVAL;
    static_assert(
        tick_interval <= ResourceGroup::COMPUTE_RU_CONSUMPTION_SPEED_INTERVAL && tick_interval <= DEGRADE_MODE_DURATION
        && tick_interval <= DEFAULT_TARGET_PERIOD);
    auto cur_tick_beg = current_tick;
    auto cur_tick_end = cur_tick_beg + tick_interval;
    while (!stopped.load())
    {
        if (current_tick < cur_tick_end)
        {
            std::unique_lock<std::mutex> lock(mu);
            if (low_token_resource_groups.empty())
            {
                if (cv.wait_until(lock, cur_tick_end, [this]() {
                        return stopped.load() || !low_token_resource_groups.empty();
                    }))
                {
                    if (stopped.load())
                        return;
                }
            }

            current_tick = SteadyClock::now();
        }

        try
        {
            while (current_tick >= cur_tick_end)
            {
                updateRUConsumptionSpeed();
                cur_tick_beg = cur_tick_end;
                cur_tick_end += tick_interval;
            }

            if (const auto gac_req_opt = buildGACRequest(/*is_final_report=*/false); gac_req_opt.has_value())
            {
                std::lock_guard lock(gac_requests_mu);
                gac_requests.push_back(gac_req_opt.value());
                gac_requests_cv.notify_all();
            }
            clearCPUTimeWithoutLock(current_tick);
            checkDegradeMode();
        }
        catch (...)
        {
            LOG_ERROR(log, getCurrentExceptionMessage(true));
        }
    }
}

void LocalAdmissionController::updateRUConsumptionSpeed()
{
    std::lock_guard lock(mu);
    for (const auto & resource_group : resource_groups)
        resource_group.second->updateRUConsumptionSpeedIfNecessary(current_tick);
}

std::optional<resource_manager::TokenBucketsRequest> LocalAdmissionController::buildGACRequest(bool is_final_report)
{
    std::vector<GACRequestInfo> request_infos;
    if unlikely (is_final_report)
    {
        // Doesn't need to lock for resource_groups because all threads should already been joined!
        for (const auto & resource_group : resource_groups)
        {
            const auto consumption_delta_info = resource_group.second->updateRUConsumptionDeltaInfoWithoutLock();
            request_infos.push_back({
                .resource_group_name = resource_group.first,
                .acquire_tokens = 0,
                .ru_consumption_delta = consumption_delta_info.delta,
            });
        }
    }
    else
    {
        std::unordered_set<std::string> local_low_token_resource_groups;
        {
            std::lock_guard lock(mu);
            local_low_token_resource_groups = low_token_resource_groups;
            low_token_resource_groups.clear();
        }

        for (const auto & iter : resource_groups)
        {
            const auto rg_name = iter.first;
            const bool need_fetch_token = local_low_token_resource_groups.contains(rg_name);
            const bool need_report = iter.second->shouldReportRUConsumption(current_tick);

            if (need_fetch_token || need_report)
            {
                auto req_info_opt = iter.second->buildRequestInfoIfNecessary(current_tick);
                if (req_info_opt.has_value())
                    request_infos.push_back(req_info_opt.value());
            }
        }
    }

    if (request_infos.empty())
        return {};

    resource_manager::TokenBucketsRequest gac_req;
    gac_req.set_client_unique_id(unique_client_id);
    gac_req.set_target_request_period_ms(DEFAULT_TARGET_PERIOD_MS.count());

    for (const auto & info : request_infos)
    {
        auto * group_request = gac_req.add_requests();
        group_request->set_resource_group_name(info.resource_group_name);
        assert(info.acquire_tokens > 0.0 || info.ru_consumption_delta > 0.0 || is_final_report);
        if (info.acquire_tokens > 0.0 || is_final_report)
        {
            auto * ru_items = group_request->mutable_ru_items();
            auto * req_ru = ru_items->add_request_r_u();
            req_ru->set_type(resource_manager::RequestUnitType::RU);
            req_ru->set_value(info.acquire_tokens);
            GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_gac_req_acquire_tokens, info.resource_group_name)
                .Set(info.acquire_tokens);
        }
        if (info.ru_consumption_delta > 0.0 || is_final_report)
        {
            group_request->set_is_tiflash(true);
            auto * tiflash_consumption = group_request->mutable_consumption_since_last_request();
            tiflash_consumption->set_r_r_u(info.ru_consumption_delta);
            GET_RESOURCE_GROUP_METRIC(
                tiflash_resource_group,
                type_gac_req_ru_consumption_delta,
                info.resource_group_name)
                .Set(info.ru_consumption_delta);
        }
    }

    return {gac_req};
}

void LocalAdmissionController::requestGACLoop()
{
    while (!stopped.load())
    {
        try
        {
            doRequestGAC();
        }
        catch (...)
        {
            LOG_ERROR(
                log,
                "doRequestGAC got error: {}, retry {} sec later",
                getCurrentExceptionMessage(false),
                NETWORK_EXCEPTION_RETRY_DURATION_SEC);
        }

        // Got here when network exception happens or stopped is true.
        {
            std::unique_lock lock(mu);
            if (cv.wait_for(lock, std::chrono::seconds(NETWORK_EXCEPTION_RETRY_DURATION_SEC), [this]() {
                    return stopped.load();
                }))
                return;
        }
    }
}

static std::vector<std::string> extractGACReqNames(const resource_manager::TokenBucketsRequest & gac_req)
{
    std::vector<std::string> res;
    res.reserve(gac_req.requests_size());
    for (const auto & req : gac_req.requests())
    {
        res.push_back(req.resource_group_name());
    }
    return res;
}

void LocalAdmissionController::doRequestGAC()
{
    while (!stopped.load())
    {
        std::vector<resource_manager::TokenBucketsRequest> local_gac_requests;
        {
            std::unique_lock<std::mutex> lock(gac_requests_mu);
            gac_requests_cv.wait(lock, [this]() { return stopped.load() || !gac_requests.empty(); });
            if unlikely (stopped.load())
                return;
            local_gac_requests = gac_requests;
            gac_requests.clear();
        }

        assert(!local_gac_requests.empty());
        for (const auto & req : local_gac_requests)
        {
            const auto req_rg_names = extractGACReqNames(req);
            for (const auto & req_rg_name : req_rg_names)
                GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_request_gac_count, req_rg_name).Increment();

            const auto resp = cluster->pd_client->acquireTokenBuckets(req);
            LOG_DEBUG(log, "request to GAC done, req: {}. resp: {}", req.ShortDebugString(), resp.ShortDebugString());

            auto handled = handleTokenBucketsResp(resp);

            std::vector<std::string> not_found;
            // not_found includes resource group names that appears in gac_req but not found in resp.
            // This can happen when the resource group has been deleted.
            if unlikely (handled.size() != req_rg_names.size())
            {
                for (const auto & req_rg_name : req_rg_names)
                {
                    if (std::find(handled.begin(), handled.end(), req_rg_name) == std::end(handled))
                        not_found.emplace_back(req_rg_name);
                }

                std::lock_guard lock(mu);
                for (const auto & name : not_found)
                {
                    auto erase_num = resource_groups.erase(name);
                    LOG_INFO(
                        log,
                        "delete resource group {} because acquireTokenBuckets didn't handle it, GAC may have already "
                        "delete it. erase_num: {}",
                        name,
                        erase_num);
                }
            }
        }
    }
}

void LocalAdmissionController::checkDegradeMode()
{
    std::lock_guard lock(mu);
    for (const auto & ele : resource_groups)
    {
        auto group = ele.second;
        group->updateDegradeMode(current_tick);
    }
}

std::vector<std::string> LocalAdmissionController::handleTokenBucketsResp(
    const resource_manager::TokenBucketsResponse & resp)
{
    if unlikely (resp.has_error())
    {
        LOG_ERROR(log, "got error when request to GAC: {}", resp.error().message());
        return {};
    }

    std::vector<std::string> handled_resource_group_names;
    handled_resource_group_names.reserve(resp.responses_size());
    if (resp.responses().empty())
    {
        LOG_ERROR(log, "got empty TokenBuckets resp from GAC, {}", resp.ShortDebugString());
        return {};
    }

    for (const resource_manager::TokenBucketResponse & one_resp : resp.responses())
    {
        // Only expect RU tokens instead of resource tokens.
        if unlikely (!one_resp.granted_resource_tokens().empty())
        {
            LOG_ERROR(log, "GAC return RAW granted tokens, but LAC expect RU tokens");
            continue;
        }

        const auto & name = one_resp.resource_group_name();
        auto resource_group = findResourceGroup(name);
        if (resource_group == nullptr)
        {
            LOG_ERROR(log, "cannot find resource group: {}", name);
            continue;
        }

        handled_resource_group_names.push_back(name);
        const String err_msg = fmt::format("handle acquire token resp failed: rg: {}", name);

        // It's possible for one_resp.granted_r_u_tokens() to be empty
        // when the acquire_token_req is only for report RU consumption.
        if (one_resp.granted_r_u_tokens().empty())
        {
            resource_group->endRequest();
            continue;
        }

        if unlikely (one_resp.granted_r_u_tokens().size() != 1)
        {
            LOG_ERROR(
                log,
                "expect resp.granted_r_u_tokens().size() is 1 or 0, but got {} for rg {}",
                one_resp.granted_r_u_tokens().size(),
                one_resp.resource_group_name());
            continue;
        }

        const resource_manager::GrantedRUTokenBucket & granted_token_bucket = one_resp.granted_r_u_tokens()[0];
        if unlikely (granted_token_bucket.type() != resource_manager::RequestUnitType::RU)
        {
            LOG_ERROR(log, "unexpected request type");
            continue;
        }

        const auto trickle_ms = granted_token_bucket.trickle_time_ms();
        if unlikely (trickle_ms < 0)
        {
            LOG_ERROR(log, "{}, expect trickle_ms >= 0, but got: {}", err_msg, trickle_ms);
            continue;
        }

        const auto now = SteadyClock::now();

        double added_tokens = granted_token_bucket.granted_tokens().tokens();
        if unlikely (!std::isfinite(added_tokens) || added_tokens < 0.0)
        {
            LOG_ERROR(log, "{}, invalid added_tokens: {}", err_msg, added_tokens);
            continue;
        }
        auto trickle_left_tokens = resource_group->getTrickleLeftTokens(now);
        if unlikely (!std::isfinite(trickle_left_tokens) || trickle_left_tokens < 0.0)
        {
            LOG_ERROR(log, "{}, invalid trickel_left_tokens {}, reset to zero", err_msg, trickle_left_tokens);
            trickle_left_tokens = 0;
        }
        added_tokens += trickle_left_tokens;

        // capacity can be zero
        // Check GAC code to see burst limit meaning:
        // https://github.com/tikv/pd/blob/e9757fbe03260775262763c67f62296fcb26b3c2/pkg/mcs/resourcemanager/server/token_buckets.go#L47
        int64_t capacity = granted_token_bucket.granted_tokens().settings().burst_limit();

        if (added_tokens > 0)
            GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_gac_resp_tokens, name).Set(added_tokens);
        if (capacity > 0)
            GET_RESOURCE_GROUP_METRIC(tiflash_resource_group, type_gac_resp_capacity, name).Set(capacity);

        // fill_rate should never be setted.
        // This is not critical error, just ignore and handle rest resource groups.
        const auto fill_rate = granted_token_bucket.granted_tokens().settings().fill_rate();
        if unlikely (fill_rate != 0)
            LOG_ERROR(log, "{}, expect fill_rate is 0, but got: {}", err_msg, fill_rate);

        if (trickle_ms == 0)
        {
            // GAC has enough tokens for LAC.
            resource_group->updateNormalMode(added_tokens, capacity, now);
        }
        else
        {
            // GAC doesn't have enough tokens for LAC, start to trickle.
            resource_group->updateTrickleMode(added_tokens, capacity, trickle_ms, now);
        }
    }

    {
        // Need lock here to avoid RCQ has already been destroyed.
        std::lock_guard lock(mu);
        if (refill_token_callback)
            refill_token_callback();
    }

    return handled_resource_group_names;
}

void LocalAdmissionController::watchGACLoop()
{
    while (!stopped.load())
    {
        try
        {
            doWatch();
        }
        catch (...)
        {
            LOG_ERROR(
                log,
                "watchGACLoop failed: {}, retry {} sec later",
                getCurrentExceptionMessage(false),
                NETWORK_EXCEPTION_RETRY_DURATION_SEC);
        }

        // Got here when:
        // 1. grpc stream read/write error.
        // 2. watch is cancel or stopped is true.
        {
            std::unique_lock lock(mu);
            if (cv.wait_for(lock, std::chrono::seconds(NETWORK_EXCEPTION_RETRY_DURATION_SEC), [this]() {
                    return stopped.load();
                }))
                return;

            // Create new grpc_context for each reader/writer.
            watch_gac_grpc_context = std::make_unique<grpc::ClientContext>();
        }
    }
}

void LocalAdmissionController::doWatch()
{
    auto stream = etcd_client->watch(watch_gac_grpc_context.get());
    auto watch_req = setupWatchReq();
    LOG_DEBUG(log, "watchGAC req: {}", watch_req.ShortDebugString());
    const bool write_ok = stream->Write(watch_req);
    if (!write_ok)
    {
        auto status = stream->Finish();
        LOG_ERROR(log, WATCH_GAC_ERR_PREFIX + status.error_message());
        return;
    }

    while (!stopped.load())
    {
        etcdserverpb::WatchResponse resp;
        auto read_ok = stream->Read(&resp);
        if (!read_ok)
        {
            auto status = stream->Finish();
            LOG_ERROR(log, WATCH_GAC_ERR_PREFIX + "read watch stream failed, " + status.error_message());
            break;
        }
        LOG_DEBUG(log, "watchGAC got resp: {}", resp.ShortDebugString());
        if (resp.canceled())
        {
            LOG_ERROR(log, WATCH_GAC_ERR_PREFIX + "watch is canceled");
            break;
        }
        for (const auto & event : resp.events())
        {
            std::string err_msg;
            const mvccpb::KeyValue & kv = event.kv();
            switch (event.type())
            {
            case mvccpb::Event_EventType_DELETE:
                if (!handleDeleteEvent(kv, err_msg))
                    LOG_ERROR(log, WATCH_GAC_ERR_PREFIX + err_msg);
                break;
            case mvccpb::Event_EventType_PUT:
                if (!handlePutEvent(kv, err_msg))
                    LOG_ERROR(log, WATCH_GAC_ERR_PREFIX + err_msg);
                break;
            default:
                RUNTIME_ASSERT(false, log, "unexpect event type {}", magic_enum::enum_name(event.type()));
            }
        }
    }
}

etcdserverpb::WatchRequest LocalAdmissionController::setupWatchReq()
{
    etcdserverpb::WatchRequest watch_req;
    auto * watch_create_req = watch_req.mutable_create_request();
    watch_create_req->set_key(GAC_RESOURCE_GROUP_ETCD_PATH);
    auto end_key = GAC_RESOURCE_GROUP_ETCD_PATH;
    end_key[end_key.length() - 1] += 1;
    watch_create_req->set_range_end(end_key);
    return watch_req;
}

bool LocalAdmissionController::handleDeleteEvent(const mvccpb::KeyValue & kv, std::string & err_msg)
{
    std::string name;
    if (!parseResourceGroupNameFromWatchKey(kv.key(), name, err_msg))
        return false;

    size_t erase_num = 0;
    {
        std::lock_guard lock(mu);
        if (auto delete_iter = resource_groups.find(name); delete_iter != resource_groups.end())
        {
            erase_num = 1;
            const auto deleted_user_ru_per_sec = delete_iter->second->user_ru_per_sec;
            resource_groups.erase(delete_iter);
            updateMaxRUPerSecAfterDeleteWithoutLock(deleted_user_ru_per_sec);
        }
    }
    LOG_DEBUG(log, "delete resource group {}, erase_num: {}", name, erase_num);
    return true;
}

bool LocalAdmissionController::handlePutEvent(const mvccpb::KeyValue & kv, std::string & err_msg)
{
    std::string name;
    if (!parseResourceGroupNameFromWatchKey(kv.key(), name, err_msg))
        return false;

    resource_manager::ResourceGroup group_pb;
    if (!group_pb.ParseFromString(kv.value()))
    {
        err_msg = "parse pb from etcd value failed";
        return false;
    }
    {
        std::lock_guard lock(mu);
        auto iter = resource_groups.find(name);
        if (iter == resource_groups.end())
        {
            // It happens when query of this resource group has not came.
            LOG_DEBUG(
                log,
                "trying to modify resource group config({}), but cannot find its info",
                group_pb.ShortDebugString());
            return true;
        }
        else
        {
            const auto deleted_user_ru_per_sec = iter->second->user_ru_per_sec;
            iter->second->resetResourceGroup(group_pb);
            updateMaxRUPerSecAfterDeleteWithoutLock(deleted_user_ru_per_sec);
        }
    }
    LOG_DEBUG(log, "modify resource group to: {}", group_pb.ShortDebugString());
    return true;
}

bool LocalAdmissionController::parseResourceGroupNameFromWatchKey(
    const std::string & etcd_key,
    std::string & parsed_rg_name,
    std::string & err_msg)
{
    const std::string & key_prefix = GAC_RESOURCE_GROUP_ETCD_PATH;
    // Expect etcd_key: resource_group/settings/rg_name
    // key_prefix is resource_group/settings
    if (etcd_key.length() <= key_prefix.length() + 1)
    {
        err_msg = fmt::format(
            "expect etcd key: {}/resource_group_name, but got {}",
            GAC_RESOURCE_GROUP_ETCD_PATH,
            etcd_key);
        return false;
    }
    parsed_rg_name = std::string(etcd_key.begin() + key_prefix.length() + 1, etcd_key.end());
    return true;
}

void LocalAdmissionController::checkGACRespValid(const resource_manager::ResourceGroup & new_group_pb)
{
    RUNTIME_CHECK_MSG(!new_group_pb.name().empty(), "resource group name from GAC is empty");
    RUNTIME_CHECK_MSG(new_group_pb.mode() == resource_manager::GroupMode::RUMode, "resource group is not RUMode");
}

void LocalAdmissionController::updateMaxRUPerSecAfterDeleteWithoutLock(uint64_t deleted_user_ru_per_sec)
{
    if (max_ru_per_sec == deleted_user_ru_per_sec)
    {
        max_ru_per_sec = 0;
        for (const auto & resource_group : resource_groups)
        {
            if (max_ru_per_sec < resource_group.second->user_ru_per_sec)
                max_ru_per_sec = resource_group.second->user_ru_per_sec;
        }
    }
}

void LocalAdmissionController::stop()
{
    if (stopped)
    {
        LOG_DEBUG(log, "LAC already stopped");
        return;
    }

    stopped.store(true);

    // TryCancel() is thread safe(https://github.com/grpc/grpc/pull/30416).
    // But we will to create a new grpc_context for each new grpc reader/writer(https://github.com/grpc/grpc/issues/18348#issuecomment-477402608).
    // So still need to lock.
    {
        std::lock_guard lock(mu);
        watch_gac_grpc_context->TryCancel();
        cv.notify_all();
    }
    {
        std::lock_guard lock(gac_requests_mu);
        gac_requests_cv.notify_all();
    }
    for (auto & thread : background_threads)
    {
        if (thread.joinable())
            thread.join();
    }

    // Report final RU consumption before stop:
    // 1. to avoid RU consumption missed.
    // 2. clear GAC's unique_client_id by setting acquire_tokens as zero to avoid affecting burst limit calculation.
    // This can happen when disagg CN is scaled-in/out frequently.
    // NOTE: Make sure all threads have been joined before call buildGACRequest().
    const auto gac_req = buildGACRequest(/*is_final_report=*/true);
    RUNTIME_CHECK(resource_groups.empty() || gac_req.has_value());
    auto resp = cluster->pd_client->acquireTokenBuckets(gac_req.value());

    if (resp.has_error())
        LOG_ERROR(log, "LAC stop got error: {}", resp.error().message());

    if (need_reset_unique_client_id.load())
    {
        try
        {
            etcd_client->deleteServerIDFromGAC(unique_client_id);
        }
        catch (...)
        {
            LOG_ERROR(
                log,
                "LAC stop got error: delete server id({}) from GAC failed: {}",
                unique_client_id,
                getCurrentExceptionMessage(false));
        }
    }
    LOG_INFO(log, "LAC({}) stop finish", unique_client_id);
}

#ifdef DBMS_PUBLIC_GTEST
std::unique_ptr<MockLocalAdmissionController> LocalAdmissionController::global_instance;
#else
std::unique_ptr<LocalAdmissionController> LocalAdmissionController::global_instance;
#endif

// Defined in PD resource_manager_client.go.
const std::string LocalAdmissionController::GAC_RESOURCE_GROUP_ETCD_PATH = "resource_group/settings";
const std::string LocalAdmissionController::WATCH_GAC_ERR_PREFIX = "watch resource group event failed: ";

} // namespace DB
