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

namespace DB
{
std::shared_ptr<CTE> CTEManager::getCTE(const String & query_id_and_cte_id)
{
    std::lock_guard<std::mutex> lock(this->mu);
    auto iter = this->ctes.find(query_id_and_cte_id);
    if (iter == this->ctes.end())
    {
        // It's the first time we request for the specific cte
        // Create it because no one created it before.
        auto cte = std::make_shared<CTE>();
        this->ctes[query_id_and_cte_id] = std::make_pair(1, cte);
        return cte;
    }

    ++(iter->second.first);
    return iter->second.second;
}

void CTEManager::releaseCTE(const String & query_id_and_cte_id)
{
    std::lock_guard<std::mutex> lock(this->mu);
    auto iter = this->ctes.find(query_id_and_cte_id);
    if unlikely (iter == this->ctes.end())
        throw Exception(fmt::format("Can't find cte: {}", query_id_and_cte_id));
    --(iter->second.first);
    if (iter->second.first == 0)
        this->ctes.erase(iter);
}
} // namespace DB