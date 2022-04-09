#pragma once

#include <Common/TiFlashException.h>
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
        if (ids.find(executor_id) != ids.end())
            throw TiFlashException(
                fmt::format("executor id ({}) duplicate", executor_id),
                Errors::Coprocessor::Internal);
        ids.insert(executor_id);
        return executor_id;
    }

private:
    String doGenerate(const tipb::Executor & executor)
    {
        assert(!executor.has_executor_id());
        switch (executor.tp())
        {
        case tipb::ExecType::TypeTableScan:
        case tipb::ExecType::TypePartitionTableScan:
            return fmt::format("{}_table_scan", ++current_id);
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
        default:
            throw TiFlashException(
                "Unsupported executor in DAG request: " + executor.DebugString(),
                Errors::Coprocessor::Unimplemented);
        }
    }

    UInt32 current_id = 0;

    std::unordered_set<String> ids;
};
} // namespace DB