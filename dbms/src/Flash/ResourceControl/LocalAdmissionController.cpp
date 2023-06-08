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

    ::resource_manager::GetResourceGroupRequest req;
    req.set_resource_group_name(name);
    auto resp = cluster->pd_client->getResourceGroup(req);
    if (resp.has_error())
    {
        // gjt todo: if not found group is error or not?
    }
    return addResourceGroup(resp.group()).first;
}

ResourceGroupPtr LocalAdmissionController::getResourceGroupByPriority()
{
    ResourceGroupPtr highest_resource_group;
    double max_priority = 0.0;
    std::lock_guard lock(mu);
    for (const auto & resource_group : resource_groups)
    {
        double ru = resource_group->getRU();
        double cpu_time = resource_group->getCPUTime();
        double priority = calcPriority(ru, cpu_time);
        if (max_priority < priority)
        {
            max_priority = priority;
            highest_resource_group = resource_group;
        }
    }
    return highest_resource_group;
}

void LocalAdmissionController::startBackgroudJob()
{
    while (!stopped.load())
    {
        {
            auto now = std::chrono::steady_clock::now();
            std::unique_lock<std::mutex> lock(mu);
            cv.wait_until(lock, now + std::chrono::seconds(DEFAULT_FETCH_GAC_INTERVAL));
        }

        // Prepare req.
        // key: ResourceGroup name
        // gjt todo: keyspace id and ru_consumption_delta
        // val: tuple<token_num_that_will_acquire_from_gac, ru_consumption_delta, keyspace_id
        std::unordered_map<std::string, double> need_tokens;
        {
            std::lock_guard lock(mu);
            for (const auto & resource_group : resource_groups)
            {
                double token_need_from_gac = resource_group->getAcquireRUNum(DEFAULT_TOKEN_FETCH_ESAPSED);
                if (token_need_from_gac <= 0.0)
                    continue;
                bool ok = need_tokens.insert(std::make_pair(resource_group->getName(), token_need_from_gac)).second;
                assert(ok);
            }
        }
        
        // gjt todo: call pd_client API to
        ::resource_manager::TokenBucketsRequest gac_req;
        // gjt todo: client_unique_id target_request_period_ms
        gac_req.set_client_unique_id(100);
        gac_req.set_target_request_period_ms(100);

        for (const auto & ele : need_tokens)
        {
            auto * single_group_req = gac_req.add_requests();
            single_group_req->set_resource_group_name(ele.first);
            auto * ru_items = single_group_req->mutable_ru_items();
            auto * req_ru = ru_items->add_request_r_u();
            req_ru->set_type(::resource_manager::RequestUnitType::RU);
            req_ru->set_value(ele.second);
        }
        ::resource_manager::TokenBucketsResponse resp = cluster->pd_client->acquireTokenBuckets(gac_req);

        // Handle resp.
        if unlikely (resp.has_error())
        {
            // gjt todo handle error
            handleBackgroundError(resp.error().message());
        }

        if (resp.responses().empty())
        {
            LOG_DEBUG(log, "got empty TokenBuckets resp from GAC");
            continue;
        }

        for (const ::resource_manager::TokenBucketResponse & one_resp : resp.responses())
        {
            // For each resource group.
            if unlikely (!one_resp.granted_resource_tokens().empty())
                handleBackgroundError("GAC return RAW granted tokens, but LAC expect RU tokens");

            for (const ::resource_manager::GrantedRUTokenBucket & granted_token_bucket : one_resp.granted_r_u_tokens())
            {
                // For each granted token bucket.
                if unlikely (granted_token_bucket.type() != ::resource_manager::RequestUnitType::RU)
                    handleBackgroundError("unexpected request type");
                // gjt todo
                // handleResp();
            }
        }
    }
}

void LocalAdmissionController::handleBackgroundError(const std::string & err_msg)
{
    // gjt todo
}

// gjt todo init
std::unique_ptr<LocalAdmissionController> LocalAdmissionController::global_instance;
} // namespace DB
