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
class AggregateMergeSourceOp : public SourceOp
{
public:
    AggregateMergeSourceOp(
        PipelineExecutorStatus & exec_status_,
        const AggregateContextPtr & agg_context_,
        const String & req_id)
        : SourceOp(exec_status_)
        , agg_context(agg_context_)
        , log(Logger::get(req_id))
    {
        setHeader(agg_context->getHeader());
    }

    String getName() const override
    {
        return "AggregateMergeSourceOp";
    }

protected:
    OperatorStatus readImpl(Block & block) override
    {
        if (!inited)
        {
            agg_context->initMerge();
            inited = true;
        }

        agg_context->read(block);
        return OperatorStatus::HAS_OUTPUT;
    }

private:
    AggregateContextPtr agg_context;
    const LoggerPtr log;
    bool inited = false;
};
} // namespace DB
