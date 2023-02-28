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
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Operators/Operator.h>

namespace DB
{
class ExchangeSenderSinkOp : public SinkOp
{
public:
    ExchangeSenderSinkOp(
        PipelineExecutorStatus & exec_status_,
        std::unique_ptr<DAGResponseWriter> && writer,
        const String & req_id)
        : SinkOp(exec_status_)
        , writer(std::move(writer))
        , log(Logger::get(req_id))
    {
    }

    String getName() const override
    {
        return "ExchangeSenderSinkOp";
    }

    void operatePrefix() override;
    void operateSuffix() override;

    OperatorStatus writeImpl(Block && block) override;

    OperatorStatus prepareImpl() override;

    OperatorStatus awaitImpl() override;

private:
    std::unique_ptr<DAGResponseWriter> writer;
    const LoggerPtr log;
    size_t total_rows = 0;
};
} // namespace DB
