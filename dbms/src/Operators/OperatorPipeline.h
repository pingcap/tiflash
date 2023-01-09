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
class PipelineExecStatus;

// push model operator pipeline.
// data flow: source --> transform --> .. --> transform --> sink
class OperatorPipeline
{
public:
    OperatorPipeline(
        SourcePtr && source_,
        std::vector<TransformPtr> && transforms_,
        SinkPtr && sink_)
        : source(std::move(source_))
        , transforms(std::move(transforms_))
        , sink(std::move(sink_))
    {}

    OperatorStatus execute(PipelineExecStatus & exec_status);

    OperatorStatus await(PipelineExecStatus & exec_status);

    OperatorStatus spill(PipelineExecStatus & exec_status);

private:
    OperatorStatus fetchBlock(Block & block, size_t & transform_index, PipelineExecStatus & exec_status);

    template <typename Op>
    OperatorStatus pushSpillOp(OperatorStatus status, const Op & op)
    {
        assert(op);
        if (status == OperatorStatus::SPILLING)
        {
            assert(!spill_op);
            spill_op.emplace(op.get());
        }
        return status;
    }

private:
    SourcePtr source;
    std::vector<TransformPtr> transforms;
    SinkPtr sink;

    // hold the operator which is ready for spilling.
    std::optional<SpillOp *> spill_op;
};
using OperatorPipelinePtr = std::unique_ptr<OperatorPipeline>;
using OperatorPipelineGroup = std::vector<OperatorPipelinePtr>;
} // namespace DB
