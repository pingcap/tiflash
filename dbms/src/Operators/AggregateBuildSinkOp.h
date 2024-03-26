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

#include <Operators/Operator.h>

namespace DB
{
class AggregateContext;
using AggregateContextPtr = std::shared_ptr<AggregateContext>;

class AggregateBuildSinkOp : public SinkOp
{
public:
    AggregateBuildSinkOp(
        PipelineExecutorContext & exec_context_,
        size_t index_,
        AggregateContextPtr agg_context_,
        const String & req_id)
        : SinkOp(exec_context_, req_id)
        , index(index_)
        , agg_context(agg_context_)
    {}

    String getName() const override { return "AggregateBuildSinkOp"; }

protected:
    void operateSuffixImpl() override;

    OperatorStatus prepareImpl() override;

    OperatorStatus writeImpl(Block && block) override;

    OperatorStatus executeIOImpl() override;

private:
    size_t index{};
    AggregateContextPtr agg_context;

    bool is_final_spill = false;
};
} // namespace DB
