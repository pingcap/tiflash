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

#include <Common/typeid_cast.h>
#include <Debug/DAGProperties.h>
#include <Debug/MockExecutor/AstToPB.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Parsers/IAST.h>
#include <kvproto/mpp.pb.h>
#include <tipb/executor.pb.h>


namespace DB::mock
{
class ExchangeSenderBinder;
class ExchangeReceiverBinder;

// Convert CH AST to tipb::Executor
// Used in integration test framework and Unit test framework.
class ExecutorBinder
{
public:
    size_t index [[maybe_unused]];
    String name;
    DB::DAGSchema output_schema;
    std::vector<std::shared_ptr<ExecutorBinder>> children;

public:
    ExecutorBinder(size_t & index_, String && name_, const DAGSchema & output_schema_)
        : index(index_)
        , name(std::move(name_))
        , output_schema(output_schema_)
    {
        index_++;
    }

    std::vector<std::shared_ptr<ExecutorBinder>> getChildren() const { return children; }

    virtual void columnPrune(std::unordered_set<String> & used_columns) = 0;
    virtual bool toTiPBExecutor(
        tipb::Executor * tipb_executor,
        int32_t collator_id,
        const MPPInfo & mpp_info,
        const Context & context)
        = 0;
    virtual void toMPPSubPlan(
        size_t & executor_index,
        const DAGProperties & properties,
        std::unordered_map<
            String,
            std::pair<std::shared_ptr<ExchangeReceiverBinder>, std::shared_ptr<ExchangeSenderBinder>>> & exchange_map)
    {
        children[0]->toMPPSubPlan(executor_index, properties, exchange_map);
    }
    virtual ~ExecutorBinder() = default;
};

using ExecutorBinderPtr = std::shared_ptr<mock::ExecutorBinder>;
} // namespace DB::mock
