// Copyright 2025 PingCAP, Inc.
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
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Mpp/CTEManager.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Operators/CTE.h>
#include <Operators/CTEReader.h>
#include <Operators/Operator.h>

#include <memory>

namespace DB
{
class CTESourceOp : public SourceOp
{
public:
    CTESourceOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        std::shared_ptr<CTEReader> cte_reader_,
        const NamesAndTypes & schema)
        : SourceOp(exec_context_, req_id)
        , cte_reader(cte_reader_)
        , io_profile_info(IOProfileInfo::createForLocal(profile_info_ptr))
    {
        setHeader(Block(getColumnWithTypeAndName(schema)));
    }

    String getName() const override { return "CTESourceOp"; }

    IOProfileInfoPtr getIOProfileInfo() const override { return io_profile_info; }

protected:
    void operateSuffixImpl() override;

    OperatorStatus readImpl(Block & block) override;

    OperatorStatus awaitImpl() override;

private:
    std::shared_ptr<CTEReader> cte_reader;
    uint64_t total_rows{};
    IOProfileInfoPtr io_profile_info;
    tipb::SelectResponse resp;
};
} // namespace DB
