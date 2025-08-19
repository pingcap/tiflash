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

#include <Common/Exception.h>
#include <Flash/Mpp/CTEManager.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Interpreters/Context.h>
#include <Operators/CTE.h>
#include <tipb/select.pb.h>

#include <memory>
#include <mutex>

namespace DB
{
class CTEReader
{
public:
    CTEReader(Context & context, const String & query_id_and_cte_id_, std::shared_ptr<CTE> cte_)
        : query_id_and_cte_id(query_id_and_cte_id_)
        , cte_manager(context.getCTEManager())
        , cte(cte_)
        , cte_reader_id(this->cte->getCTEReaderID())
    {
        RUNTIME_CHECK(cte);
    }

    // For Test
    CTEReader(const String & query_id_and_cte_id_, CTEManager * cte_manager_, std::shared_ptr<CTE> cte_)
        : query_id_and_cte_id(query_id_and_cte_id_)
        , cte_manager(cte_manager_)
        , cte(cte_)
        , cte_reader_id(this->cte->getCTEReaderID())
    {}

    ~CTEReader()
    {
        this->cte.reset();
        this->cte_manager->releaseCTEBySource(this->query_id_and_cte_id);
    }

    CTEOpStatus fetchNextBlock(size_t source_id, Block & block);

    void getResp(tipb::SelectResponse & resp)
    {
        std::lock_guard<std::mutex> lock(this->mu);
        if (this->resp_fetched)
            return;
        this->resp_fetched = true;
        resp.CopyFrom(this->resp);
    }

    std::shared_ptr<CTE> getCTE() const { return this->cte; }
    size_t getID() const { return this->cte_reader_id; }

    bool areAllSinksRegistered() { return this->cte->areAllSinksRegistered<true>(); }

    CTEOpStatus waitForBlockAvailableForTest(size_t partition_idx);

private:
    String query_id_and_cte_id;
    CTEManager * cte_manager;
    std::shared_ptr<CTE> cte;
    size_t cte_reader_id;

    std::mutex mu;
    bool resp_fetched = false;
    tipb::SelectResponse resp;
};
} // namespace DB
