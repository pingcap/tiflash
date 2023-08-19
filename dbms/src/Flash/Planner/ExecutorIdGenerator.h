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

#include <Common/TiFlashException.h>
#include <common/likely.h>
#include <common/types.h>
#include <fmt/format.h>
#include <tipb/executor.pb.h>

#include <unordered_set>

namespace DB
{
class ExecutorIdGenerator
{
public:
    String generate(const tipb::Executor & executor)
    {
        String executor_id = executor.has_executor_id() ? executor.executor_id() : doGenerate(executor);
        assert(!executor_id.empty());
        RUNTIME_CHECK(ids.find(executor_id) == ids.end(), executor_id);
        ids.insert(executor_id);
        return executor_id;
    }

private:
    String doGenerate(const tipb::Executor & executor)
    {
        assert(!executor.has_executor_id());
        switch (executor.tp())
        {
        case tipb::ExecType::TypeSelection:
            return fmt::format("{}_selection", ++current_id);
        case tipb::ExecType::TypeProjection:
            return fmt::format("{}_projection", ++current_id);
        case tipb::ExecType::TypeStreamAgg:
        case tipb::ExecType::TypeAggregation:
            return fmt::format("{}_aggregation", ++current_id);
        case tipb::ExecType::TypeTopN:
            return fmt::format("{}_top_n", ++current_id);
        case tipb::ExecType::TypeLimit:
            return fmt::format("{}_limit", ++current_id);
        case tipb::ExecType::TypeExchangeSender:
            return fmt::format("{}_exchange_sender", ++current_id);
        case tipb::ExecType::TypeExchangeReceiver:
            return fmt::format("{}_exchange_receiver", ++current_id);
        case tipb::ExecType::TypeTableScan:
        case tipb::ExecType::TypePartitionTableScan:
            return fmt::format("{}_table_scan", ++current_id);
        case tipb::ExecType::TypeSort:
            return fmt::format("{}_sort", ++current_id);
        case tipb::ExecType::TypeWindow:
            return fmt::format("{}_window", ++current_id);
        case tipb::ExecType::TypeJoin:
            return fmt::format("{}_join", ++current_id);
        default:
            throw TiFlashException(
                fmt::format("Unsupported executor in DAG request: {}", executor.DebugString()),
                Errors::Planner::Unimplemented);
        }
    }

    UInt32 current_id = 0;

    std::unordered_set<String> ids;
};
} // namespace DB
