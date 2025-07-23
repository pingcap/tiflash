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

#include <atomic>
#include <memory>
#include <mutex>

namespace DB
{
class CTEReader
{
public:
    explicit CTEReader(Context & context)
        : query_id_and_cte_id(context.getDAGContext()->getQueryIDAndCTEIDForSource())
        , cte_manager(context.getCTEManager())
        , cte(context.getDAGContext()->getCTESource())
        , cte_reader_id(this->cte->getCTEReaderID())
    {
        RUNTIME_CHECK(cte);
    }

    std::atomic_size_t total_fetch_blocks = 0;
    std::atomic_size_t total_fetch_rows = 0;

    ~CTEReader()
    {
        this->cte.reset();
        this->cte_manager->releaseCTEBySource(this->query_id_and_cte_id);

        auto * log = &Poco::Logger::get("LRUCache");
        LOG_INFO(
            log,
            fmt::format(
                "xzxdebug CTEReader fb: {} fr: {}",
                this->total_fetch_blocks.load(),
                this->total_fetch_rows.load()));
    }

    CTEOpStatus fetchNextBlock(size_t partition_id, Block & block);
    CTEOpStatus fetchBlockFromDisk(size_t partition_id, Block & block);

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

private:
    String query_id_and_cte_id;
    CTEManager * cte_manager;
    std::shared_ptr<CTE> cte;
    size_t cte_reader_id;

    std::mutex mu;
    bool resp_fetched = false;
    tipb::SelectResponse resp;

    std::atomic_bool is_first_log = false;
};
} // namespace DB
