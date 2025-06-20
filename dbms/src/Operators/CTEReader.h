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
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Operators/CTE.h>
#include <tipb/select.pb.h>

#include <memory>
#include <mutex>

namespace DB
{
class CTEReaderNotifyFuture : public NotifyFuture
{
public:
    CTEReaderNotifyFuture(std::shared_ptr<CTE> cte_, size_t cte_reader_id_)
        : cte(cte_)
        , cte_reader_id(cte_reader_id_)
    {}

    void registerTask(TaskPtr && task) override
    {
        this->cte->checkBlockAvailableAndRegisterTask(std::move(task), this->cte_reader_id);
    }

private:
    std::shared_ptr<CTE> cte;
    size_t cte_reader_id;
};

class CTEReader
{
public:
    CTEReader(
        const String & query_id_and_cte_id_,
        const String & partition_id_,
        CTEManager * cte_manager_,
        Int32 expected_sink_num_,
        Int32 expected_source_num_)
        : query_id_and_cte_id(query_id_and_cte_id_)
        , partition_id(partition_id_)
        , cte_manager(cte_manager_)
        , cte(cte_manager_
                  ->getCTEBySource(query_id_and_cte_id_, partition_id, expected_sink_num_, expected_source_num_))
        , cte_reader_id(this->cte->getCTEReaderID())
        , notifier(cte, this->cte_reader_id)
    {}

    ~CTEReader()
    {
        this->cte.reset();
        this->cte_manager->releaseCTEBySource(this->query_id_and_cte_id, this->partition_id);
    }

    CTEOpStatus fetchNextBlock(Block & block);
    CTEOpStatus checkAvailableBlock();

    void getResp(tipb::SelectResponse & resp)
    {
        std::lock_guard<std::mutex> lock(this->mu);
        if (this->resp_fetched)
            return;
        this->resp_fetched = true;
        resp.CopyFrom(this->resp);
    }

    void setNotifyFuture() { ::DB::setNotifyFuture(&(this->notifier)); }

    std::shared_ptr<CTE> getCTE() const { return this->cte; }

private:
    String query_id_and_cte_id;
    String partition_id;
    CTEManager * cte_manager;
    std::shared_ptr<CTE> cte;
    size_t cte_reader_id;
    CTEReaderNotifyFuture notifier;

    std::mutex mu;
    bool resp_fetched = false;
    tipb::SelectResponse resp;
};
} // namespace DB
