#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatistics.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>
#include <fmt/core.h>
#include <fmt/format.h>

namespace DB
{
namespace
{
inline Int64 parseId(const String & executor_id, String::size_type split_index)
{
    return std::stoi(executor_id.substr(split_index + 1, executor_id.size()));
}

inline auto getExecutorIdSplitIndex(const String & executor_id)
{
    auto split_index = executor_id.find('_');
    if (split_index == String::npos || split_index == (executor_id.size() - 1))
    {
        throw TiFlashException("Illegal executor_id: " + executor_id, Errors::Coprocessor::Internal);
    }
    return split_index;
}
} // namespace

ExecutorStatistics::ExecutorStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : dag_context(dag_context_)
{
    assert(executor->has_executor_id());
    executor_id = executor->executor_id();

    auto split_index = getExecutorIdSplitIndex(executor_id);
    type = executor_id.substr(0, split_index);
    id = parseId(executor_id, split_index);

    for (const auto * child : getChildren(*executor))
    {
        assert(child->has_executor_id());
        auto child_split_index = getExecutorIdSplitIndex(child->executor_id());
        children.push_back(parseId(child->executor_id(), child_split_index));
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