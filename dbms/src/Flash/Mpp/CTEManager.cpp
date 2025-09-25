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
void CTEManager::releaseCTEBySource(const String & query_id_and_cte_id)
{
    std::lock_guard<std::mutex> lock(this->mu);
    auto iter = this->ctes.find(query_id_and_cte_id);
    if unlikely (iter == this->ctes.end())
        // Maybe the task is cancelled and the cte has been released
        return;

    iter->second->sourceExit();
    if (iter->second->allExit())
        this->ctes.erase(iter);
}

void CTEManager::releaseCTEBySink(const tipb::SelectResponse & resp, const String & query_id_and_cte_id)
{
    std::lock_guard<std::mutex> lock(this->mu);
    auto iter = this->ctes.find(query_id_and_cte_id);
    if unlikely (iter == this->ctes.end())
        // Maybe the task is cancelled and the cte has been released
        return;

    std::shared_ptr<CTE> cte = iter->second;
    cte->addResp(resp);
    cte->sinkExit<false>();
    if (cte->allExit())
        this->ctes.erase(iter);
}

void CTEManager::releaseCTE(const String & query_id_and_cte_id)
{
    std::lock_guard<std::mutex> lock(this->mu);
    this->ctes.erase(query_id_and_cte_id);
}

std::shared_ptr<CTE> CTEManager::getOrCreateCTE(
    const String & query_id_and_cte_id,
    Int32 concurrency,
    Int32 expected_sink_num,
    Int32 expected_source_num)
{
    std::lock_guard<std::mutex> lock(this->mu);
    auto iter = this->ctes.find(query_id_and_cte_id);
    std::shared_ptr<CTE> cte;
    if (iter == this->ctes.end())
    {
        cte = std::make_shared<CTE>(concurrency, expected_sink_num, expected_source_num);
        this->ctes.insert(std::make_pair(query_id_and_cte_id, cte));
    }
    else
    {
        cte = iter->second;
    }

    return cte;
}

bool CTEManager::hasCTEForTest(const String & query_id_and_cte_id)
{
    std::lock_guard<std::mutex> lock(this->mu);
    return !(this->ctes.find(query_id_and_cte_id) == this->ctes.end());
}
} // namespace DB
