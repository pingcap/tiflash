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
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Mpp/CTEManager.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Operators/CTE.h>
#include <Operators/Operator.h>

namespace DB
{
class CTESourceOp : public SourceOp
{
public:
    CTESourceOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const String & query_id_and_cte_id_,
        CTEManager * cte_manager_)
        : SourceOp(exec_context_, req_id)
        , query_id_and_cte_id(query_id_and_cte_id_)
        , cte_manager(cte_manager_)
        , cte(cte_manager_->getCTE(query_id_and_cte_id_))
    {}

    ~CTESourceOp() override { assert(!this->cte); }

    String getName() const override { return "CTESourceOp"; }

protected:
    void operateSuffixImpl() override;

    OperatorStatus readImpl(Block & block) override;

    OperatorStatus awaitImpl() override; // TODO implement awaitImpl

private:
    String query_id_and_cte_id;
    CTEManager * cte_manager;
    std::shared_ptr<CTE> cte;
    size_t block_fetch_idx = 0;

    uint64_t total_rows{};
    std::queue<Block> block_queue;
};
} // namespace DB
