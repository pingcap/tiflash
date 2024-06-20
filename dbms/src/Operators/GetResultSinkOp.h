// Copyright 2023 PingCAP, Inc.
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

#include <Flash/Executor/ResultQueue_fwd.h>
#include <Operators/Operator.h>

namespace DB
{
// The sink operator for getting the execution results.
class GetResultSinkOp : public SinkOp
{
public:
    GetResultSinkOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const ResultQueuePtr & result_queue_)
        : SinkOp(exec_context_, req_id)
        , result_queue(result_queue_)
    {
        assert(result_queue);
    }

    String getName() const override { return "GetResultSinkOp"; }

protected:
    OperatorStatus writeImpl(Block && block) override;

    OperatorStatus prepareImpl() override;

private:
    OperatorStatus tryFlush();

private:
    ResultQueuePtr result_queue;
    std::optional<Block> t_block;
};
} // namespace DB
