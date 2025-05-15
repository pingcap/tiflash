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
std::shared_ptr<CTE> CTEManager::getCTE(const String & query_id_and_cte_id, const String & partition_id)
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
        this->ctes[query_id_and_cte_id].insert(std::make_pair(partition_id, CTEWithCounter(cte, 1)));
        return cte;
    }

    ++(iter_for_cte->second.counter);
    return iter_for_cte->second.cte;
}

void CTEManager::releaseCTE(const String & query_id_and_cte_id, const String & partition_id)
{
    std::lock_guard<std::mutex> lock(this->mu);
    auto iter = this->ctes.find(query_id_and_cte_id);
    if unlikely (iter == this->ctes.end())
        throw Exception(fmt::format("Can't find cte: {}", query_id_and_cte_id));
    auto iter_for_cte = iter->second.find(partition_id);
    if unlikely (iter_for_cte == iter->second.end())
        throw Exception(fmt::format("Can't find cte: {}, partition: {}", query_id_and_cte_id, partition_id));
    --(iter_for_cte->second.counter);
    if (iter_for_cte->second.counter == 0)
        iter->second.erase(iter_for_cte);

    if (iter->second.size() == 0)
        this->ctes.erase(iter);
}

void CTEManager::setRespAndNotifyEOF(const tipb::SelectResponse & resp, const String & query_id_and_cte_id)
{
    this->executeOnManyCTEs<true>(resp, query_id_and_cte_id);
}

void CTEManager::notifyEOF(const String & query_id_and_cte_id)
{
    this->executeOnManyCTEs<false>(tipb::SelectResponse(), query_id_and_cte_id);
}

template <bool set_resp>
void CTEManager::executeOnManyCTEs(const tipb::SelectResponse & resp, const String & query_id_and_cte_id)
{
    std::lock_guard<std::mutex> lock(this->mu);
    auto iter = this->ctes.find(query_id_and_cte_id);
    if unlikely (iter == this->ctes.end())
        throw Exception(fmt::format("Can't find cte: {}", query_id_and_cte_id));
    auto iter_for_cte = iter->second.begin();
    while (iter_for_cte != iter->second.end())
    {
        if constexpr (set_resp)
            iter_for_cte->second.cte->setRespAndNotifyEOF(resp);
        else
            iter_for_cte->second.cte->notifyEOF();
        iter_for_cte++;
    }
}
} // namespace DB
