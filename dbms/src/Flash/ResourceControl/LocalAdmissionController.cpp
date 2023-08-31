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
#include <pingcap/kv/Cluster.h>

namespace DB
{
void LocalAdmissionController::warmupResourceGroupInfoCache(const std::string & name)
{
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
            fmt::format("warmupResourceGroupInfoCache({}) failed: {}", name, getCurrentExceptionMessage(false)));
    }

    RUNTIME_CHECK_MSG(!resp.has_error(), "warmupResourceGroupInfoCache({}) failed: {}", name, resp.error().message());

    checkGACRespValid(resp.group());

    addResourceGroup(resp.group());
}

void LocalAdmissionController::startBackgroudJob()
{
    try
    {
        unique_client_id = etcd_client->acquireServerIDFromPD();
    }
    catch (...)
    {
        LOG_FATAL(log, getCurrentExceptionMessage(false));
        std::terminate();
    }

    while (!stopped.load())
    {
        std::function<void()> local_refill_token_callback = nullptr;
        bool fetch_token_periodically = false;

        {
            std::unique_lock<std::mutex> lock(mu);
            local_refill_token_callback = refill_token_callback;

            if (low_token_resource_groups.empty())
            {
                fetch_token_periodically = true;
                if (cv.wait_for(lock, std::chrono::seconds(DEFAULT_FETCH_GAC_INTERVAL), [this]() {
                        return stopped.load();
                    }))
                    return;
            }
        }

        try
        {
            if (fetch_token_periodically)
            {
                fetchTokensForAllResourceGroups();
                checkDegradeMode();
            }
            else
            {
                fetchTokensForLowTokenResourceGroups();
            }

            if (local_refill_token_callback)
                local_refill_token_callback();
        }
        catch (...)
        {
            handleBackgroundError(getCurrentExceptionMessage(true));
        }
    }
}

void LocalAdmissionController::fetchTokensForAllResourceGroups()
{
    std::vector<AcquireTokenInfo> acquire_infos;
    {
        std::lock_guard lock(mu);
        for (const auto & resource_group : resource_groups)
        {
            auto acquire_info = buildAcquireInfo(resource_group.second, /*is_periodically_fetch=*/true);
            if (acquire_info.has_value())
                acquire_infos.push_back(acquire_info.value());
        }
    }

    static const std::string log_desc_str = fmt::format("periodically({}sec)", DEFAULT_TOKEN_FETCH_ESAPSED);
    fetchTokensFromGAC(acquire_infos, log_desc_str);
}

void LocalAdmissionController::fetchTokensForLowTokenResourceGroups()
{
    std::vector<AcquireTokenInfo> acquire_infos;
    {
        std::lock_guard lock(mu);
        for (const auto & name : low_token_resource_groups)
        {
            auto iter = resource_groups.find(name);
            if (iter != resource_groups.end())
            {
                auto acquire_info = buildAcquireInfo(iter->second, /*is_periodically_fetch=*/false);
                if (acquire_info.has_value())
                    acquire_infos.push_back(acquire_info.value());
            }
        }
        low_token_resource_groups.clear();
    }

    static const std::string log_desc_str = "because of low token";
    fetchTokensFromGAC(acquire_infos, log_desc_str);
}

std::optional<LocalAdmissionController::AcquireTokenInfo> LocalAdmissionController::buildAcquireInfo(
    const ResourceGroupPtr & resource_group,
    bool is_periodically_fetch)
{
    double token_consumption = 0.0;
    double acquire_tokens = 0.0;

    auto get_token_consumption = [&]() {
        if (is_periodically_fetch)
            token_consumption = resource_group->getAndCleanConsumptionDelta();
    };
    auto get_acquire_tokens = [&]() {
        if (resource_group->burstable)
            return;

        // To avoid periodically_token_fetch after low_token_fetch immediately
        const auto now = std::chrono::steady_clock::now();
        if (is_periodically_fetch
            && !resource_group->needFetchTokenPeridically(now, std::chrono::seconds(DEFAULT_FETCH_GAC_INTERVAL)))
            return;

        // During trickle mode, no need to fetch tokens from GAC.
        if (resource_group->inTrickleModeLease(now))
            return;

        acquire_tokens = resource_group->getAcquireRUNum(DEFAULT_TOKEN_FETCH_ESAPSED, ACQUIRE_RU_AMPLIFICATION);
        if (acquire_tokens < 0.0)
            acquire_tokens = 0.0;
    };

    get_token_consumption();
    get_acquire_tokens();

    if (token_consumption == 0.0 && acquire_tokens == 0.0)
        return std::nullopt;
    else
        return {AcquireTokenInfo{
            .resource_group_name = resource_group->name,
            .acquire_tokens = acquire_tokens,
            .ru_consumption_delta = token_consumption}};
}

void LocalAdmissionController::fetchTokensFromGAC(
    const std::vector<AcquireTokenInfo> & acquire_infos,
    const std::string & desc_str)
{
    if (acquire_infos.empty())
    {
        // In theory last_fetch_tokens_from_gac_timepoint should only be updated when network to GAC is ok,
        // but we still update here to avoid resource groups that has enough RU goto degrade mode.
        last_fetch_tokens_from_gac_timepoint = std::chrono::steady_clock::now();
        return;
    }

    resource_manager::TokenBucketsRequest gac_req;
    gac_req.set_client_unique_id(unique_client_id);
    gac_req.set_target_request_period_ms(TARGET_REQUEST_PERIOD_MS);

    FmtBuffer fmt_buf;
    for (const auto & info : acquire_infos)
    {
        fmt_buf.fmtAppend("{};", info.toString());

        auto * single_group_req = gac_req.add_requests();
        single_group_req->set_resource_group_name(info.resource_group_name);
        assert(info.acquire_tokens > 0.0 || info.ru_consumption_delta > 0.0);
        if (info.acquire_tokens > 0.0)
        {
            auto * ru_items = single_group_req->mutable_ru_items();
            auto * req_ru = ru_items->add_request_r_u();
            req_ru->set_type(resource_manager::RequestUnitType::RU);
            req_ru->set_value(info.acquire_tokens);
        }
        if (info.ru_consumption_delta > 0.0)
        {
            single_group_req->set_is_tiflash(true);
            auto * tiflash_consumption = single_group_req->mutable_consumption_since_last_request();
            tiflash_consumption->set_r_r_u(info.ru_consumption_delta);
        }
    }

    auto resp = cluster->pd_client->acquireTokenBuckets(gac_req);
    LOG_DEBUG(
        log,
        "fetch token from GAC {}: acquire_infos: {}, req: {}. resp: {}",
        desc_str,
        fmt_buf.toString(),
        gac_req.DebugString(),
        resp.DebugString());

    auto handled = handleTokenBucketsResp(resp);
    // not_found includes resource group names that appears in gac_req but not found in resp.
    // This can happen when the resource group is deleted.
    if unlikely (handled.size() != acquire_infos.size())
    {
        std::vector<std::string> not_found;
        for (const auto & info : acquire_infos)
        {
            if (std::find(handled.begin(), handled.end(), info.resource_group_name) == std::end(handled))
                not_found.emplace_back(info.resource_group_name);
        }

        std::lock_guard lock(mu);
        for (const auto & name : not_found)
        {
            auto erase_num = resource_groups.erase(name);
            LOG_INFO(
                log,
                "delete resource group {} because acquireTokenBuckets didn't handle it, GAC may have already delete "
                "it. erase_num: {}",
                name,
                erase_num);
        }
    }
}

void LocalAdmissionController::checkDegradeMode()
{
    auto now = std::chrono::steady_clock::now();
    std::lock_guard lock(mu);
    if (DEGRADE_MODE_DURATION != 0
        && (now - last_fetch_tokens_from_gac_timepoint) >= std::chrono::seconds(DEGRADE_MODE_DURATION))
    {
        for (const auto & ele : resource_groups)
        {
            auto group = ele.second;
            group->toDegrademode();
        }
    }
}

std::vector<std::string> LocalAdmissionController::handleTokenBucketsResp(
    const resource_manager::TokenBucketsResponse & resp)
{
    if unlikely (resp.has_error())
    {
        handleBackgroundError(resp.error().message());
        return {};
    }

    std::vector<std::string> handled_resource_group_names;
    handled_resource_group_names.reserve(resp.responses_size());
    // Network to GAC is ok, update timepoint.
    last_fetch_tokens_from_gac_timepoint = std::chrono::steady_clock::now();

    if (resp.responses().empty())
    {
        handleBackgroundError("got empty TokenBuckets resp from GAC");
        return {};
    }

    const auto now = std::chrono::steady_clock::now();
    for (const resource_manager::TokenBucketResponse & one_resp : resp.responses())
    {
        // For each resource group.
        if unlikely (!one_resp.granted_resource_tokens().empty())
        {
            handleBackgroundError("GAC return RAW granted tokens, but LAC expect RU tokens");
            continue;
        }

        handled_resource_group_names.emplace_back(one_resp.resource_group_name());
        for (const resource_manager::GrantedRUTokenBucket & granted_token_bucket : one_resp.granted_r_u_tokens())
        {
            // For each granted token bucket.
            if unlikely (granted_token_bucket.type() != resource_manager::RequestUnitType::RU)
            {
                handleBackgroundError("unexpected request type");
                continue;
            }

            int64_t trickle_ms = granted_token_bucket.trickle_time_ms();
            RUNTIME_CHECK(trickle_ms >= 0);

            double added_tokens = granted_token_bucket.granted_tokens().tokens();
            RUNTIME_CHECK(added_tokens >= 0);

            int64_t capacity = granted_token_bucket.granted_tokens().settings().burst_limit();

            // Check GAC code, fill_rate is never setted.
            RUNTIME_CHECK(granted_token_bucket.granted_tokens().settings().fill_rate() == 0);

            auto resource_group = findResourceGroup(one_resp.resource_group_name());

            if (resource_group == nullptr)
                continue;

            if (trickle_ms == 0)
            {
                // GAC has enough tokens for LAC.
                resource_group->updateNormalMode(added_tokens, capacity);
            }
            else
            {
                // GAC doesn't have enough tokens for LAC, start to trickle.
                resource_group->updateTrickleMode(added_tokens, capacity, trickle_ms);
            }
            resource_group->updateFetchTokenTimepoint(now);
        }
    }
    return handled_resource_group_names;
}

void LocalAdmissionController::watchGAC()
{
    while (true)
    {
        doWatch();

        // Got here when:
        // 1. grpc stream write error.
        // 2. grpc stream read error.
        // 3. watch is cancel.
        // Will sleep and try again.
        std::unique_lock lock(mu);
        if (cv.wait_for(lock, std::chrono::seconds(30), [this]() { return stopped.load(); }))
            return;
    }
}

void LocalAdmissionController::doWatch()
{
    auto stream = etcd_client->watch(&watch_gac_grpc_context);
    auto watch_req = setupWatchReq();
    LOG_DEBUG(log, "watch req: {}", watch_req.DebugString());
    const bool write_ok = stream->Write(watch_req);
    if (!write_ok)
    {
        auto status = stream->Finish();
        handleBackgroundError(WATCH_GAC_ERR_PREFIX + status.error_message());
        return;
    }

    while (!stopped.load())
    {
        etcdserverpb::WatchResponse resp;
        auto read_ok = stream->Read(&resp);
        if (!read_ok)
        {
            auto status = stream->Finish();
            handleBackgroundError(WATCH_GAC_ERR_PREFIX + "read watch stream failed, " + status.error_message());
            break;
        }
        LOG_DEBUG(log, "watchGAC got resp: {}", resp.DebugString());
        if (resp.canceled())
        {
            handleBackgroundError(WATCH_GAC_ERR_PREFIX + "watch is canceled");
            break;
        }
        for (const auto & event : resp.events())
        {
            std::string err_msg;
            const mvccpb::KeyValue & kv = event.kv();
            if (event.type() == mvccpb::Event_EventType_DELETE)
            {
                if (!handleDeleteEvent(kv, err_msg))
                    handleBackgroundError(WATCH_GAC_ERR_PREFIX + err_msg);
            }
            else if (event.type() == mvccpb::Event_EventType_PUT)
            {
                if (!handlePutEvent(kv, err_msg))
                    handleBackgroundError(WATCH_GAC_ERR_PREFIX + err_msg);
            }
            else
            {
                __builtin_unreachable();
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
        erase_num = resource_groups.erase(name);
    }
    LOG_INFO(log, "delete resource group {}, erase_num: {}", name, erase_num);
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
            err_msg = fmt::format(
                "trying to modify resource group config({}), but cannot find its info",
                group_pb.DebugString());
            return false;
        }
        else
        {
            iter->second->resetResourceGroup(group_pb);
        }
    }
    LOG_INFO(log, "modify resource group to: {}", group_pb.DebugString());
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

void LocalAdmissionController::handleBackgroundError(const std::string & err_msg) const
{
    // Basically, errors are all from GAC, cannot handle in tiflash.
    // So only print log.
    LOG_ERROR(log, err_msg);
}

void LocalAdmissionController::checkGACRespValid(const resource_manager::ResourceGroup & new_group_pb)
{
    if unlikely (new_group_pb.name().empty())
        throw Exception("resource group name from GAC is empty");

    if unlikely (new_group_pb.mode() != resource_manager::GroupMode::RUMode)
        throw Exception("resource group is not RUMode");
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
