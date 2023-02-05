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

#include <Common/Logger.h>
#include <DataStreams/FilterTransformAction.h>
#include <Operators/Operator.h>

namespace DB
{
class FilterTransformOp : public TransformOp
{
public:
    FilterTransformOp(
        PipelineExecutorStatus & exec_status_,
        const Block & input_header,
        const ExpressionActionsPtr & expression,
        const String & filter_column_name,
        const String & req_id)
        : TransformOp(exec_status_)
        , filter_transform_action(input_header, expression, filter_column_name)
        , log(Logger::get(req_id))
    {}

    String getName() const override
    {
        return "FilterTransformOp";
    }

protected:
    OperatorStatus transformImpl(Block & block) override;

    void transformHeaderImpl(Block & header_) override;

private:
    FilterTransformAction filter_transform_action;
    const LoggerPtr log;
};
} // namespace DB
