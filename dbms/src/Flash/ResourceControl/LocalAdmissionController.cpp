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

#include <common/logger_useful.h>
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

// gjt todo: in trickle mode, stop low token notify
void LocalAdmissionController::startBackgroudJob()
{
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
    // key: ResourceGroup name
    // gjt todo: keyspace id and ru_consumption_delta
    // val: tuple<token_num_that_will_acquire_from_gac, ru_consumption_delta, keyspace_id
    std::unordered_map<std::string, double> need_tokens;
    {
        std::lock_guard lock(mu);
        for (const auto & resource_group : resource_groups)
        {
            double token_need_from_gac = resource_group.second->getAcquireRUNum(DEFAULT_TOKEN_FETCH_ESAPSED);
            // gjt todo maybe always <= 0.0
            if (token_need_from_gac <= 0.0)
                return;
            bool ok = need_tokens.insert(std::make_pair(resource_group.first, token_need_from_gac)).second;
            assert(ok);
        }
    }
    
    resource_manager::TokenBucketsRequest gac_req;
    // gjt todo: client_unique_id target_request_period_ms
    gac_req.set_client_unique_id(100);
    gac_req.set_target_request_period_ms(100);

    for (const auto & ele : need_tokens)
    {
        auto * single_group_req = gac_req.add_requests();
        single_group_req->set_resource_group_name(ele.first);
        auto * ru_items = single_group_req->mutable_ru_items();
        auto * req_ru = ru_items->add_request_r_u();
        req_ru->set_type(resource_manager::RequestUnitType::RU);
        req_ru->set_value(ele.second);
    }

    auto grpc_reader_writer = cluster->pd_client->acquireTokenBuckets();
    bool succ = grpc_reader_writer->Write(gac_req);
    if (!succ)
    {
        handleBackgroundError("write grpc stream failed when send TokenBucketsRequest to GAC");
        return;
    }
    grpc_reader_writer->WritesDone();

    resource_manager::TokenBucketsResponse resp;
    while (grpc_reader_writer->Read(&resp))
        handleTokenBucketsResp(resp);
    grpc_reader_writer->Finish();
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

                // gjt todo watch out unit?
                int64_t trickle_ms = granted_token_bucket.trickle_time_ms();
                RUNTIME_CHECK(trickle_ms >= 0);

                double added_tokens = granted_token_bucket.granted_tokens().tokens();
                RUNTIME_CHECK(added_tokens >= 0);

                int64_t capacity = granted_token_bucket.granted_tokens().settings().burst_limit();
                RUNTIME_CHECK(capacity >= 0);
                
                RUNTIME_CHECK(granted_token_bucket.granted_tokens().settings().fill_rate() == 0);

                auto resource_group = getOrCreateResourceGroup(one_resp.resource_group_name());

                if (trickle_ms == 0)
                {
                    // GAC has enough tokens for LAC.
                    resource_group->reConfigTokenBucketInNormalMode(added_tokens);
                }
                else
                {
                    // GAC doesn't have enough tokens for LAC, start to trickle.
                    // gjt todo maybe new group?
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
        mpp_task_manager->removeResourceGroupMinTSOScheduler(remove_name);

    last_cleanup_resource_group_timepoint = std::chrono::steady_clock::now();
}

void LocalAdmissionController::handleBackgroundError(const std::string & err_msg)
{
    // Basically error is from GAC, cannot handle in tiflash.
    LOG_ERROR(log, err_msg);
}

// gjt todo init
std::unique_ptr<LocalAdmissionController> LocalAdmissionController::global_instance;
} // namespace DB
