// Copyright 2025 PingCAP, Inc.
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

#include <Flash/Mpp/CTEManager.h>
#include <fmt/core.h>
#include <tipb/select.pb.h>

#include <mutex>
#include <utility>

namespace DB
{
void CTEManager::releaseCTEBySource(const String & query_id_and_cte_id, const String & partition_id)
{
    std::lock_guard<std::mutex> lock(this->mu);
    auto iter = this->ctes.find(query_id_and_cte_id);
    if unlikely (iter == this->ctes.end())
        // Maybe the task is cancelled and all ctes have been released
        return;

    auto iter_for_cte = iter->second.find(partition_id);
    RUNTIME_CHECK_MSG(
        iter_for_cte != iter->second.end(),
        "Can't find cte: {}, partition: {}",
        query_id_and_cte_id,
        partition_id);

    if (iter_for_cte->second.getTotalExitNum() == iter_for_cte->second.getExpectedTotalNum())
        iter->second.erase(iter_for_cte);

    if (iter->second.empty())
        this->ctes.erase(iter);
}

// TODO refine codes here, do not directly use map
void CTEManager::releaseCTEBySink(const tipb::SelectResponse & resp, const String & query_id_and_cte_id)
{
    std::unique_lock<std::mutex> lock(this->mu);
    auto iter = this->ctes.find(query_id_and_cte_id);
    if unlikely (iter == this->ctes.end())
        // Maybe the task is cancelled and all ctes have been released
        return;

    auto iter_for_cte = iter->second.begin();
    auto iter_for_cte_end = iter->second.end();
    std::vector<String> ctes_need_erase;
    while (iter_for_cte != iter_for_cte_end)
    {
        CTEWithCounter & cte_with_counter = iter_for_cte->second;
        cte_with_counter.getCTE()->addResp(resp);
        cte_with_counter.sinkExit();
        if (cte_with_counter.getSinkExitNum() == cte_with_counter.getExpectedSinkNum())
            cte_with_counter.getCTE()->notifyEOF();
        if (cte_with_counter.getTotalExitNum() == cte_with_counter.getExpectedTotalNum())
            ctes_need_erase.push_back(iter_for_cte->first);
        iter_for_cte++;
    }

    if (ctes_need_erase.size() == iter->second.size())
    {
        this->ctes.erase(iter);
        return;
    }

    for (const auto & key : ctes_need_erase)
    {
        auto iter_for_cte = iter->second.find(key);
        iter->second.erase(iter_for_cte);
    }
}

void CTEManager::releaseCTEs(const String & query_id_and_cte_id)
{
    std::lock_guard<std::mutex> lock(this->mu);
    auto iter = this->ctes.find(query_id_and_cte_id);
    if (iter != this->ctes.end())
        this->ctes.erase(iter);
}

std::shared_ptr<CTE> CTEManager::getCTEImpl(
    const String & query_id_and_cte_id,
    const String & partition_id,
    Int32 expected_sink_num,
    Int32 expected_source_num)
{
    std::lock_guard<std::mutex> lock(this->mu);
    auto iter = this->ctes.find(query_id_and_cte_id);
    if (iter == this->ctes.end())
        this->ctes[query_id_and_cte_id] = std::unordered_map<String, CTEWithCounter>{};

    auto & cte_map = this->ctes[query_id_and_cte_id];
    auto [iter_for_cte, _] = cte_map.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(partition_id),
        std::forward_as_tuple(std::make_shared<CTE>(), expected_sink_num, expected_source_num));

    return iter_for_cte->second.getCTE();
}
} // namespace DB
