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
    if (resp.has_error())
    {
        LOG_ERROR(log, "got error when fetch resource group from GAC: {}", resp.error().message());
        return nullptr;
    }
    return addResourceGroup(resp.group()).first;
}

void LocalAdmissionController::startBackgroudJob()
{
    setupUniqueClientID();
    while (!stopped.load())
    {
        {
            auto now = std::chrono::steady_clock::now();
            std::unique_lock<std::mutex> lock(mu);
            if (cv.wait_until(lock, now + std::chrono::seconds(DEFAULT_FETCH_GAC_INTERVAL), [this]() { return stopped.load(); }))
                return;
        }

        fetchTokensFromGAC();
        checkDegradeMode();

        auto now = std::chrono::steady_clock::now();
        if (now - last_cleanup_resource_group_timepoint >= CLEANUP_RESOURCE_GROUP_INTERVAL)
            cleanupResourceGroups();
    }
}

void LocalAdmissionController::fetchTokensFromGAC()
{
    // Prepare req.
    // tuple<resource_group_name, token_num_that_will_acquire_from_gac, ru_consumption_delta>.
    std::vector<std::tuple<std::string, double, double>> need_tokens;
    {
        std::lock_guard lock(mu);
        for (const auto & resource_group : resource_groups)
        {
            double token_need_from_gac = resource_group.second->getAcquireRUNum(DEFAULT_TOKEN_FETCH_ESAPSED, ACQUIRE_RU_AMPLIFICATION);
            if (token_need_from_gac <= 0.0)
                continue;
            need_tokens.emplace_back(std::make_tuple(resource_group.first, token_need_from_gac, resource_group.second->getAndCleanConsumptionDelta()));
        }
    }

    if (need_tokens.empty())
        return;

    resource_manager::TokenBucketsRequest gac_req;
    gac_req.set_client_unique_id(unique_client_id);
    gac_req.set_target_request_period_ms(TARGET_REQUEST_PERIOD_MS);

    for (const auto & ele : need_tokens)
    {
        auto * single_group_req = gac_req.add_requests();
        single_group_req->set_resource_group_name(std::get<0>(ele));
        auto * ru_items = single_group_req->mutable_ru_items();
        auto * req_ru = ru_items->add_request_r_u();
        req_ru->set_type(resource_manager::RequestUnitType::RU);
        req_ru->set_value(std::get<1>(ele));

        single_group_req->set_is_tiflash(true);
        auto * tiflash_consumption = single_group_req->mutable_consumption_since_last_request();
        tiflash_consumption->set_r_r_u(std::get<2>(ele));
    }

    auto resps = cluster->pd_client->acquireTokenBuckets(gac_req);
    for (const auto & resp : resps)
        handleTokenBucketsResp(resp);
}

void LocalAdmissionController::checkDegradeMode()
{
    std::lock_guard lock(mu);
    for (const auto & ele : resource_groups)
    {
        auto group = ele.second;
        group->stepIntoDegradeModeIfNecessary(DEGRADE_MODE_DURATION);
    }
}

void LocalAdmissionController::handleTokenBucketsResp(const resource_manager::TokenBucketsResponse & resp)
{
    if unlikely (resp.has_error())
    {
        handleBackgroundError(resp.error().message());
    }
    else if (resp.responses().empty())
    {
        handleBackgroundError("got empty TokenBuckets resp from GAC");
    }
    else
    {
        for (const resource_manager::TokenBucketResponse & one_resp : resp.responses())
        {
            // For each resource group.
            if unlikely (!one_resp.granted_resource_tokens().empty())
                handleBackgroundError("GAC return RAW granted tokens, but LAC expect RU tokens");

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

                RUNTIME_CHECK(granted_token_bucket.granted_tokens().settings().fill_rate() == 0);

                auto resource_group = findResourceGroup(one_resp.resource_group_name());

                if (resource_group == nullptr)
                    continue;

                if (trickle_ms == 0)
                {
                    // GAC has enough tokens for LAC.
                    resource_group->reConfigTokenBucketInNormalMode(added_tokens, capacity);
                }
                else
                {
                    // GAC doesn't have enough tokens for LAC, start to trickle.
                    resource_group->reConfigTokenBucketInTrickleMode(added_tokens, capacity, trickle_ms);
                }
            }
        }
    }
}

void LocalAdmissionController::cleanupResourceGroups()
{
    resource_manager::ListResourceGroupsRequest req;
    auto resp = cluster->pd_client->listResourceGroups(req);
    if (resp.has_error())
    {
        handleBackgroundError(resp.error().message());
        return;
    }
    std::unordered_set<std::string> gac_resource_groups;
    for (const auto & resource_group_pb : resp.groups())
    {
        auto insert_result = gac_resource_groups.insert(resource_group_pb.name());
        assert(insert_result.second);
    }

    // Record all resource group name to remove instead of call removeResourceGroupMinTSOScheduler inside the follow lock scope.
    // This is to avoid potential dead lock between LocalAdmissionController::mu and MPPTaskManager::mu.
    std::unordered_set<std::string> remove_names;
    {
        std::lock_guard lock(mu);
        for (auto iter = resource_groups.begin(); iter != resource_groups.end();)
        {
            if (!gac_resource_groups.contains(iter->first))
            {
                iter = resource_groups.erase(iter);
                remove_names.insert(iter->first);
            }
            else
            {
                ++iter;
            }
        }
    }

    for (const auto & remove_name : remove_names)
        mpp_task_manager->tagResourceGroupSchedulerReadyToDelete(remove_name);

    mpp_task_manager->cleanResourceGroupScheduler();

    last_cleanup_resource_group_timepoint = std::chrono::steady_clock::now();
}

void LocalAdmissionController::handleBackgroundError(const std::string & err_msg)
{
    // Basically, errors are all from GAC, cannot handle in tiflash.
    // So only print log.
    LOG_ERROR(log, err_msg);
}

#ifndef DBMS_PUBLIC_GTEST
std::unique_ptr<LocalAdmissionController> LocalAdmissionController::global_instance;
#else
auto LocalAdmissionController::global_instance = std::make_unique<MockLocalAdmissionController>();
#endif

const std::string ResourceGroup::DEFAULT_RESOURCE_GROUP_NAME = "default";
} // namespace DB
