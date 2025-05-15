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

#pragma once

#include <Flash/Mpp/CTEManager.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Operators/CTE.h>
#include <tipb/select.pb.h>

#include <deque>
#include <mutex>

namespace DB
{
class CTEReader
{
public:
    CTEReader(const String & query_id_and_cte_id_, const String & partition_id_, CTEManager * cte_manager_)
        : query_id_and_cte_id(query_id_and_cte_id_)
        , partition_id(partition_id_)
        , cte_manager(cte_manager_)
        , cte(cte_manager_->getCTE(query_id_and_cte_id_, partition_id))
    {}

    ~CTEReader()
    {
        this->cte.reset();
        this->cte_manager->releaseCTE(this->query_id_and_cte_id, this->partition_id);
    }

    std::pair<FetchStatus, Block> fetchNextBlock();
    FetchStatus checkAvailableBlock();

    void getResp(tipb::SelectResponse & resp)
    {
        std::lock_guard<std::mutex> lock(this->mu);
        if (this->resp_fetched)
            return;
        resp.CopyFrom(this->resp);
    }

    bool isBlockGenerated()
    {
        std::lock_guard<std::mutex> lock(this->mu);

        // `block_fetch_idx == 0` means that CTE hasn't received block yet, maybe it is waiting
        // for the finish of join executor
        return this->block_fetch_idx != 0;
    }

    void setNotifyFuture() { ::DB::setNotifyFuture(cte.get()); }

private:
    String query_id_and_cte_id;
    String partition_id;
    CTEManager * cte_manager;
    std::shared_ptr<CTE> cte;

    std::mutex mu;
    std::deque<Block> blocks;
    size_t block_fetch_idx = 0;

    bool resp_fetched = false;
    tipb::SelectResponse resp;
};
} // namespace DB
