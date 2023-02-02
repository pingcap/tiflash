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
// The sink operator for getting the execution results.
// Now it is used in unit tests.
class GetResultSinkOp : public SinkOp
{
public:
    explicit GetResultSinkOp(ResultHandler && result_handler_)
        : result_handler(std::move(result_handler_))
    {
    }

    String getName() const override
    {
        return "GetResultSinkOp";
    }

    OperatorStatus writeImpl(Block && block) override;

private:
    ResultHandler result_handler;
};
} // namespace DB
