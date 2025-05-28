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

#include <chrono>
#include <mutex>
#include <utility>

namespace DB
{
void CTEManager::releaseCTEBySource(const String & query_id_and_cte_id, const String & partition_id)
{
    std::lock_guard<std::mutex> lock(this->mu);
    auto iter = this->ctes.find(query_id_and_cte_id);
    if unlikely (iter == this->ctes.end())
        throw Exception(fmt::format("Can't find cte: {}", query_id_and_cte_id));
    auto iter_for_cte = iter->second.find(partition_id);
    if unlikely (iter_for_cte == iter->second.end())
        throw Exception(fmt::format("Can't find cte: {}, partition: {}", query_id_and_cte_id, partition_id));
    
    iter_for_cte->second.sourceExit();
    if (iter_for_cte->second.getTotalExitNum() == iter_for_cte->second.getExpectedTotalNum())
        iter->second.erase(iter_for_cte);

    if (iter->second.size() == 0)
    {
        auto * log = &Poco::Logger::get("LRUCache");
        LOG_INFO(log, "xzxdebug releaseCTEBySource, erase {}", query_id_and_cte_id);
        this->ctes.erase(iter);
    }
}

// TODO refine codes here, do not directly use map
void CTEManager::releaseCTEBySink(const tipb::SelectResponse & resp, const String & query_id_and_cte_id)
{
    std::unique_lock<std::mutex> lock(this->mu);
    auto iter = this->ctes.find(query_id_and_cte_id);
    if unlikely (iter == this->ctes.end())
        throw Exception(fmt::format("Can't find cte: {}", query_id_and_cte_id));

    auto * log = &Poco::Logger::get("LRUCache");
    LOG_INFO(log, "xzxdebug releaseCTEBySink, counter");

    auto iter_for_cte = iter->second.begin();
    auto iter_for_cte_end = iter->second.end();
    std::vector<String> ctes_need_erase;
    while (iter_for_cte != iter_for_cte_end)
    {
        CTEWithCounter & cte_with_counter = iter_for_cte->second;
        cte_with_counter.getCTE()->addRespAndNotifyEOF(resp);
        cte_with_counter.sinkExit();
        if (cte_with_counter.getSinkExitNum() == cte_with_counter.getExpectedSinkNum())
            cte_with_counter.getCTE()->notifyEOF();
        if (cte_with_counter.getTotalExitNum() == cte_with_counter.getExpectedTotalNum())
            ctes_need_erase.push_back(iter_for_cte->first);
        iter_for_cte++;
    }

    if (ctes_need_erase.size() == iter->second.size())
        this->ctes.erase(iter);

    for (const auto & key : ctes_need_erase)
    {
        auto iter_for_cte = iter->second.find(key);
        iter->second.erase(iter_for_cte);
    }
}

std::shared_ptr<CTE> CTEManager::getCTEimpl(const String & query_id_and_cte_id, const String & partition_id, Int32 expected_sink_num, Int32 expected_source_num)
{
    std::lock_guard<std::mutex> lock(this->mu);
    auto iter = this->ctes.find(query_id_and_cte_id);
    if (iter == this->ctes.end())
        this->ctes[query_id_and_cte_id] = std::map<String, CTEWithCounter>{};

    auto iter_for_cte = this->ctes[query_id_and_cte_id].find(partition_id);
    if (iter_for_cte == this->ctes[query_id_and_cte_id].end())
    {
        // It's the first time we request for the specific cte
        // Create it because no one created it before.
        auto cte = std::make_shared<CTE>();
        CTEWithCounter cte_with_counter(cte, expected_sink_num, expected_source_num);
        this->ctes[query_id_and_cte_id].insert(std::make_pair(partition_id, cte_with_counter));
        return cte;
    }

    return iter_for_cte->second.getCTE();
}
} // namespace DB
