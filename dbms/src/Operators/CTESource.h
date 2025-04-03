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
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Operators/CTE.h>
#include <Operators/Operator.h>

namespace DB
{
// TODO consider fine graine at compiling stage
class CTESourceOp : public SourceOp
{
public:
    CTESourceOp(PipelineExecutorContext & exec_context_, const String & req_id, std::shared_ptr<CTE> cte_)
        : SourceOp(exec_context_, req_id)
        , cte(cte_)
    {}

    String getName() const override { return "CTESourceOp"; }

protected:
    void operateSuffixImpl() override { LOG_DEBUG(log, "finish read {} rows from cte source", total_rows); }

    OperatorStatus readImpl(Block & block) override;

    OperatorStatus awaitImpl() override;

private:
    std::shared_ptr<CTE> cte;
    size_t block_fetch_idx = 0;

    uint64_t total_rows{};
    std::queue<Block> block_queue;
};
} // namespace DB
