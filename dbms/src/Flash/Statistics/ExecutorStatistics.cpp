#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/traverseExecutorTree.h>
#include <Flash/Statistics/ExecutorStatistics.h>
#include <Interpreters/Context.h>
#include <fmt/core.h>
#include <fmt/format.h>

namespace DB
{
namespace
{
Int64 parseId(const String & executor_id)
{
    auto split_index = executor_id.find('_');
    if (split_index == String::npos || split_index == (executor_id.size() - 1))
    {
        throw TiFlashException("Illegal executor_id: " + executor_id, Errors::Coprocessor::Internal);
    }
    return std::stoi(executor_id.substr(split_index + 1, executor_id.size()));
}
} // namespace

ExecutorStatistics::ExecutorStatistics(const String & executor_id, Context & context)
{
    auto split_index = executor_id.find('_');
    if (split_index == String::npos || split_index == (executor_id.size() - 1))
    {
        throw TiFlashException("Illegal executor_id: " + executor_id, Errors::Coprocessor::Internal);
    }

    type = executor_id.substr(0, split_index);
    id = std::stoi(executor_id.substr(split_index + 1, executor_id.size()));

    const auto * executor = context.getDAGContext()->getExecutor(executor_id);
    for (const auto * child : getChildren(*executor))
    {
        assert(child->has_executor_id());
        children.push_back(parseId(child->executor_id()));
    }
}

String ExecutorStatistics::toJson() const
{
    return fmt::format(
        R"({{"id":{},"type":"{}","children":[{}],"outbound_rows":{},"outbound_blocks":{},"outbound_bytes":{},"execution_time_ns":{}{}}})",
        id,
        type,
        fmt::join(children, ","),
        outbound_rows,
        outbound_blocks,
        outbound_bytes,
        execution_time_ns,
        extraToJson());
}
} // namespace DB