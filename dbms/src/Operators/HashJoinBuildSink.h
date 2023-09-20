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
class Join;
using JoinPtr = std::shared_ptr<Join>;

class HashJoinBuildSink : public SinkOp
{
public:
    HashJoinBuildSink(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const JoinPtr & join_ptr_,
        size_t op_index_)
        : SinkOp(exec_context_, req_id)
        , join_ptr(join_ptr_)
        , op_index(op_index_)
    {}

    String getName() const override { return "HashJoinBuildSink"; }

protected:
    OperatorStatus writeImpl(Block && block) override;

    OperatorStatus prepareImpl() override;

    OperatorStatus executeIOImpl() override;

private:
    JoinPtr join_ptr;
    size_t op_index;

    bool is_finish_status = false;
};
} // namespace DB
