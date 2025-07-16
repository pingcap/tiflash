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

#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Mpp/CTEManager.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Operators/CTE.h>
#include <Operators/CTEReader.h>
#include <Operators/Operator.h>
#include <Common/Stopwatch.h>

#include <memory>

namespace DB
{
class CTESourceNotifyFuture : public NotifyFuture
{
public:
    CTESourceNotifyFuture(std::shared_ptr<CTE> cte_, size_t cte_reader_id_, size_t source_id_)
        : cte(cte_)
        , cte_reader_id(cte_reader_id_)
        , source_id(source_id_)
    {}

    void registerTask(TaskPtr && task) override
    {
        this->cte->checkBlockAvailableAndRegisterTask(std::move(task), this->cte_reader_id, this->source_id);
    }

private:
    std::shared_ptr<CTE> cte;
    size_t cte_reader_id;
    size_t source_id;
};

class CTESourceOp : public SourceOp
{
public:
    CTESourceOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        std::shared_ptr<CTEReader> cte_reader_,
        size_t id_,
        const NamesAndTypes & schema,
        const String & query_id_and_cte_id_)
        : SourceOp(exec_context_, req_id)
        , query_id_and_cte_id(query_id_and_cte_id_)
        , cte_reader(cte_reader_)
        , io_profile_info(IOProfileInfo::createForRemote(profile_info_ptr, 1))
        , id(id_)
        , notifier(this->cte_reader->getCTE(), this->cte_reader->getID(), this->id)
        , io_notifier(this->cte_reader->getCTE(), id)
    {
        setHeader(Block(getColumnWithTypeAndName(schema)));
    }

    String getName() const override { return "CTESourceOp"; }
    IOProfileInfoPtr getIOProfileInfo() const override { return io_profile_info; }

protected:
    void operateSuffixImpl() override;

    OperatorStatus readImpl(Block & block) override;
    OperatorStatus executeIOImpl() override;

    OperatorStatus awaitImpl() override
    {
        if (this->cte_reader->areAllSinksRegistered())
            return OperatorStatus::HAS_OUTPUT;

        if (this->sw.elapsedSeconds() >= 10)
            throw Exception(fmt::format(
                "cte sink can't be registered for 10s, query_id_and_cte_id: {}",
                this->query_id_and_cte_id));
        return OperatorStatus::WAITING;
    }

private:
    String query_id_and_cte_id;
    Block block_from_disk;

    uint64_t total_rows{};
    std::shared_ptr<CTEReader> cte_reader;

    IOProfileInfoPtr io_profile_info;
    tipb::SelectResponse resp;
    size_t id;
    CTESourceNotifyFuture notifier;
    CTEIONotifier io_notifier;
    Stopwatch sw;
};
} // namespace DB
