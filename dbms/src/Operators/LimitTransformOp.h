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
#include <Operators/Operator.h>

namespace DB
{
template <typename LimitActionPtr>
class LimitTransformOp : public TransformOp
{
public:
    LimitTransformOp(PipelineExecutorContext & exec_context_, const String & req_id, const LimitActionPtr & action_)
        : TransformOp(exec_context_, req_id)
        , action(action_)
    {}

    String getName() const override { return "LimitTransformOp"; }

protected:
    ReturnOpStatus transformImpl(Block & block) override;

    void transformHeaderImpl(Block & header_) override;

private:
    LimitActionPtr action;
};
} // namespace DB
