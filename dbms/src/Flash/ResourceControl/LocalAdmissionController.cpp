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

#include <Flash/ResourceControl/LocalAdmissionController.h>
#include <etcd/rpc.pb.h>
#include <pingcap/kv/Cluster.h>

namespace DB
{
ResourceGroupPtr LocalAdmissionController::getOrCreateResourceGroup(const std::string & name)
{
    ResourceGroupPtr group = findResourceGroup(name);
    if (group != nullptr)
        return group;

    resource_manager::GetResourceGroupRequest req;
    req.set_resource_group_name(name);
    auto resp = cluster->pd_client->getResourceGroup(req);
    RUNTIME_CHECK_MSG(
        !resp.has_error(),
        "fetch resource group({}) info from GAC failed: {}",
        name,
        resp.error().message());

    std::string err_msg = isGACRespValid(resp.group());
    RUNTIME_CHECK_MSG(err_msg.empty(), "fetch resource group({}) info from GAC failed: {}", name, err_msg);

    return addResourceGroup(resp.group()).first;
}

void LocalAdmissionController::startBackgroudJob()
{
    while (!stopped.load())
    {
        try
        {
            unique_client_id = etcd_client->acquireServerIDFromPD();
            break;
        }
        catch (...)
        {
            auto err_msg = getCurrentExceptionMessage(true);
            handleBackgroundError(err_msg);
        }
    }

    while (!stopped.load())
    {
        {
            std::unique_lock<std::mutex> lock(mu);
            if (cv.wait_for(lock, std::chrono::seconds(DEFAULT_FETCH_GAC_INTERVAL), [this]() {
                    return stopped.load();
                }))
                return;

            if (refill_token_callback)
                refill_token_callback();

            // gjt todo
            // if (delete_resource_group_callback)
            //     delete_resource_group_callback();
        }

        try
        {
            fetchTokensFromGAC();
            checkDegradeMode();
        }
        catch (...)
        {
            auto err_msg = getCurrentExceptionMessage(true);
            handleBackgroundError(err_msg);
        }
    }
}

void LocalAdmissionController::watchResourceGroupDelete()
{
    grpc::ClientContext grpc_context;
    grpc_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
    auto stream = etcd_client->watch(&grpc_context);

    etcdserverpb::WatchRequest watch_req;
    // gjt todo add test to ensure not change.
    auto * watch_create_req = watch_req.mutable_create_request();
    watch_create_req->set_key(GAC_RESOURCE_GROUP_ETCD_PATH);
    // Only watch delete event.
    watch_create_req->add_filters(etcdserverpb::WatchCreateRequest_FilterType::WatchCreateRequest_FilterType_NOPUT);
    bool ok = stream->Write(watch_req);
    static const std::string err_msg_prefix = "watch resource group delete event failed: ";
    if (!ok)
    {
        auto status = stream->Finish();
        handleBackgroundError(err_msg_prefix + status.error_message());
        return;
    }

    while (!stopped.load())
    {
        etcdserverpb::WatchResponse resp;
        ok = stream->Read(&resp);
        if (!ok)
        {
            handleBackgroundError(err_msg_prefix + "read watch stream failed, watcher is closed");
            break;
        }
        if (resp.canceled())
        {
            handleBackgroundError(err_msg_prefix + "watch is canceled");
            break;
        }
        for (const auto & event : resp.events())
        {
            if (event.type() == mvccpb::Event_EventType_DELETE)
            {
                const mvccpb::KeyValue & kv = event.kv();
                const std::string & name = kv.value();
                std::lock_guard lock(mu);
                auto erase_num = resource_groups.erase(name);
                LOG_INFO(log, "delete resource group {}, erase_num: {}", name, erase_num);
            }
        }
    }
    // No need to cancel watch when exit, check: https://github.com/etcd-io/etcd/issues/7742#issuecomment-294184158
}

void LocalAdmissionController::fetchTokensFromGAC()
{
    std::vector<AcquireTokenInfo> acquire_infos;
    {
        std::lock_guard lock(mu);
        for (const auto & resource_group : resource_groups)
        {
            double token_need_from_gac
                = resource_group.second->getAcquireRUNum(DEFAULT_TOKEN_FETCH_ESAPSED, ACQUIRE_RU_AMPLIFICATION);
            if (token_need_from_gac <= 0.0)
                continue;

            acquire_infos.emplace_back(AcquireTokenInfo{
                .resource_group_name = resource_group.first,
                .acquire_tokens = token_need_from_gac,
                .ru_consumption_delta = resource_group.second->getAndCleanConsumptionDelta()});
        }
    }

    if (acquire_infos.empty())
    {
        last_fetch_tokens_from_gac_timepoint = std::chrono::steady_clock::now();
        return;
    }

    resource_manager::TokenBucketsRequest gac_req;
    // gjt todo:
    gac_req.set_client_unique_id(10010);
    gac_req.set_target_request_period_ms(TARGET_REQUEST_PERIOD_MS);

    for (const auto & info : acquire_infos)
    {
        auto * single_group_req = gac_req.add_requests();
        single_group_req->set_resource_group_name(info.resource_group_name);
        auto * ru_items = single_group_req->mutable_ru_items();
        auto * req_ru = ru_items->add_request_r_u();
        req_ru->set_type(resource_manager::RequestUnitType::RU);
        req_ru->set_value(info.acquire_tokens);

        single_group_req->set_is_tiflash(true);
        auto * tiflash_consumption = single_group_req->mutable_consumption_since_last_request();
        tiflash_consumption->set_r_r_u(info.ru_consumption_delta);
    }
    LOG_DEBUG(log, "trying to fetch token from GAC: {}", gac_req.DebugString());

    auto resp = cluster->pd_client->acquireTokenBuckets(gac_req);
    auto handled = handleTokenBucketsResp(resp);
    std::vector<std::string> not_found;
    not_found.reserve(handled.size());
    for (const auto & info : acquire_infos)
    {
        bool found = false;
        for (const auto & name : handled)
        {
            if (info.resource_group_name == name)
                found = true;
        }
        if unlikely (!found)
            not_found.emplace_back(info.resource_group_name);
    }
    {
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
    std::lock_guard lock(mu);
    auto now = std::chrono::steady_clock::now();
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
    LOG_DEBUG(log, "got TokenBucketsResponse: {}", resp.DebugString());
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
                resource_group->toNormalMode(added_tokens, capacity);
            }
            else
            {
                // GAC doesn't have enough tokens for LAC, start to trickle.
                resource_group->toTrickleMode(added_tokens, capacity, trickle_ms);
            }
        }
    }
    return handled_resource_group_names;
}

void LocalAdmissionController::handleBackgroundError(const std::string & err_msg)
{
    // Basically, errors are all from GAC, cannot handle in tiflash.
    // So only print log.
    LOG_ERROR(log, err_msg);
}

std::string LocalAdmissionController::isGACRespValid(const resource_manager::ResourceGroup & new_group_pb)
{
    String err_msg;
    if unlikely (new_group_pb.name().empty())
        err_msg += "resource group name from GAC pb is empty.";

    if unlikely (new_group_pb.mode() != resource_manager::GroupMode::RUMode)
        err_msg += fmt::format(" expect RUMode, got {}", new_group_pb.mode());

    return err_msg;
}

#ifdef DBMS_PUBLIC_GTEST
auto LocalAdmissionController::global_instance = std::make_unique<MockLocalAdmissionController>();
#else
std::unique_ptr<LocalAdmissionController> LocalAdmissionController::global_instance;
#endif

// Defined in PD resource_manager_client.go.
const std::string LocalAdmissionController::GAC_RESOURCE_GROUP_ETCD_PATH = "resource_group/settings";
} // namespace DB
