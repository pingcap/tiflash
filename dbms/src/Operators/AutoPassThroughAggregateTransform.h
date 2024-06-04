// Copyright 2024 PingCAP, Inc.
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
#include <Operators/AutoPassThroughHashAggContext.h>

namespace DB
{
class AutoPassThroughAggregateTransform : public TransformOp
{
public:
    AutoPassThroughAggregateTransform(
            PipelineExecutorContext & exec_context_,
            const String & req_id_,
            const Aggregator::Params & params_);
    
    String getName() const override { return "AutoPassThroughAggregateTransform"; }
protected:
    OperatorStatus transformImpl(Block & block) override;

    OperatorStatus tryOutputImpl(Block & block) override;

    OperatorStatus executeIOImpl() override
    {
        // todo: when call here
        throw Exception("shouldn't handle io for AutoPassThroughAggregateTransform");
    }

    void transformHeaderImpl(Block & header_) override
    {
        header_ = auto_pass_through_context->getHeader();
    }

private:
    enum class Status
    {
        building_hash_map,
        hash_map_done,
    };
    Status status;
    AutoPassThroughHashAggContextPtr auto_pass_through_context;
};
}
