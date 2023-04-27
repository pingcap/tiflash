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

#include <Operators/AggregateContext.h>
#include <Operators/Operator.h>

namespace DB
{
/// Only do build and convert at the current operator, no sharing of objects with other operators.
class LocalAggregateTransform : public TransformOp
{
public:
    LocalAggregateTransform(
        PipelineExecutorStatus & exec_status_,
        const String & req_id,
        const Aggregator::Params & params_);

    String getName() const override
    {
        return "LocalAggregateTransform";
    }

protected:
    OperatorStatus transformImpl(Block & block) override;

    OperatorStatus tryOutputImpl(Block & block) override;

    OperatorStatus executeIOImpl() override;

    void transformHeaderImpl(Block & header_) override;

private:
    OperatorStatus tryFromBuildToSpill();

    OperatorStatus fromBuildToConvergent(Block & block);

    OperatorStatus fromBuildToFinalSpillOrRestore();

private:
    Aggregator::Params params;
    AggregateContext agg_context;

    /**
     * spill◄────►build────┬─────────────►restore
     *              │      │                 ▲
     *              │      └───►final_spill──┘
     *              ▼
     *           convergent
     */
    enum class LocalAggStatus
    {
        // Accept the block and build aggregate data.
        build,
        // spill the aggregate data into disk.
        spill,
        // convert the aggregate data to block and then output it.
        convergent,
        // spill the rest remaining memory aggregate data.
        final_spill,
        // load the disk aggregate data to memory and then convert to block and output it.
        restore,
    };
    LocalAggStatus status{LocalAggStatus::build};

    LocalAggregateRestorerPtr restorer;
};
} // namespace DB
