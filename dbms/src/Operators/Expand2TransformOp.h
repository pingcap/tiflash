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

#include <Common/Logger.h>
#include <DataStreams/ExpandTransformAction.h>
#include <Operators/Operator.h>

namespace DB
{
class Expand2TransformOp : public TransformOp
{
public:
    Expand2TransformOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const Block & input_header,
        const Expand2Ptr & expand)
        : TransformOp(exec_context_, req_id)
        , expand_transform_action(input_header, expand)
    {}

    String getName() const override { return "Expand2TransformOp"; }

protected:
    ReturnOpStatus transformImpl(Block & block) override;

    ReturnOpStatus tryOutputImpl(Block & block) override;

    void transformHeaderImpl(Block & header_) override;

private:
    ExpandTransformAction expand_transform_action;
};
} // namespace DB
