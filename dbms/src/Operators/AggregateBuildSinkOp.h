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
class AggregateBuildSinkOp : public SinkOp
{
public:
    AggregateBuildSinkOp(
        PipelineExecutorStatus & exec_status_,
        size_t index_,
        AggregateContextPtr agg_context_,
        const String & req_id)
        : SinkOp(exec_status_, req_id)
        , index(index_)
        , agg_context(agg_context_)
    {
    }

    String getName() const override
    {
        return "AggregateBuildSinkOp";
    }

    void operateSuffix() override;

protected:
    OperatorStatus writeImpl(Block && block) override;

private:
    size_t index{};
    uint64_t total_rows{};
    AggregateContextPtr agg_context;
};
} // namespace DB
