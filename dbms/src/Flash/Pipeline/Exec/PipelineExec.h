// Copyright 2023 PingCAP, Ltd.
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

#include <Operators/Operator.h>

#include <memory>

namespace DB
{
class PipelineExecutorStatus;

// The executor of push model operator.
// A pipeline will generate multiple pipeline_execs.
// data flow: source --> transform --> .. --> transform --> sink
class PipelineExec
{
public:
    PipelineExec(
        SourcePtr && source_,
        Transforms && transforms_,
        SinkPtr && sink_)
        : source(std::move(source_))
        , transforms(std::move(transforms_))
        , sink(std::move(sink_))
    {}

    OperatorStatus execute(PipelineExecutorStatus & exec_status);

    OperatorStatus await(PipelineExecutorStatus & exec_status);

    OperatorStatus spill(PipelineExecutorStatus & exec_status);

private:
    OperatorStatus fetchBlock(Block & block, size_t & transform_index, PipelineExecutorStatus & exec_status);

    template <typename Op>
    OperatorStatus prepareSpillOp(OperatorStatus op_status, const Op & op)
    {
        assert(op);
        if (op_status == OperatorStatus::SPILLING)
        {
            assert(!ready_spill_op);
            ready_spill_op.emplace(op.get());
        }
        return op_status;
    }

private:
    SourcePtr source;
    Transforms transforms;
    SinkPtr sink;

    // hold the operator which is ready for spilling.
    std::optional<Operator *> ready_spill_op;
};
using PipelineExecPtr = std::unique_ptr<PipelineExec>;
using PipelineExecGroup = std::vector<PipelineExecPtr>;
} // namespace DB
