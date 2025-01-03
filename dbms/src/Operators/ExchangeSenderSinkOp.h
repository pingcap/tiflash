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
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Operators/Operator.h>

namespace DB
{
class ExchangeSenderSinkOp : public SinkOp
{
public:
    ExchangeSenderSinkOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        std::unique_ptr<DAGResponseWriter> && writer_)
        : SinkOp(exec_context_, req_id)
        , writer(std::move(writer_))
    {}

    String getName() const override { return "ExchangeSenderSinkOp"; }

    bool canHandleSelectiveBlock() const override { return true; }

protected:
    void operatePrefixImpl() override;
    void operateSuffixImpl() override;

    OperatorStatus writeImpl(Block && block) override;

    OperatorStatus prepareImpl() override;

    OperatorStatus awaitImpl() override;

private:
    OperatorStatus waitForWriter() const;
    OperatorStatus writeResultToOperatorStatus(WriteResult res) const;

private:
    std::unique_ptr<DAGResponseWriter> writer;
    size_t total_rows = 0;
    bool input_done = false;
};
} // namespace DB
