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
#include <Common/Logger.h>

namespace DB
{
class Join;
using JoinPtr = std::shared_ptr<Join>;

class HashJoinBuildSink : public SinkOp
{
public:
    HashJoinBuildSink(
        PipelineExecutorStatus & exec_status_,
        const JoinPtr & join_ptr_,
        size_t concurrency_build_index_,
        const String & req_id)
        : SinkOp(exec_status_)
        , join_ptr(join_ptr_)
        , concurrency_build_index(concurrency_build_index_)
        , log(Logger::get(req_id))
    {
    }

    String getName() const override
    {
        return "HashJoinBuildSink";
    }

    OperatorStatus writeImpl(Block && block) override;

private:
    JoinPtr join_ptr;
    size_t concurrency_build_index;
    const LoggerPtr log;
};
} // namespace DB
