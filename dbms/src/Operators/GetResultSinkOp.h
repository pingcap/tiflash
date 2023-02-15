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

#include <Flash/Executor/ResultHandler.h>
#include <Operators/Operator.h>

namespace DB
{
class PhysicalGetResultSink;
using PhysicalGetResultSinkPtr = std::shared_ptr<PhysicalGetResultSink>;
// The sink operator for getting the execution results.
// Now it is used in unit tests.
class GetResultSinkOp : public SinkOp
{
public:
    GetResultSinkOp(
        PipelineExecutorStatus & exec_status_,
        const PhysicalGetResultSinkPtr & physical_sink_)
        : SinkOp(exec_status_)
        , physical_sink(physical_sink_)
    {
        assert(physical_sink);
    }

    String getName() const override
    {
        return "GetResultSinkOp";
    }

protected:
    OperatorStatus writeImpl(Block && block) override;

private:
    PhysicalGetResultSinkPtr physical_sink;
};
} // namespace DB
