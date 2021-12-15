#pragma once

#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsBase.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>
#include <common/types.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <tipb/executor.pb.h>

#include <memory>
#include <vector>

namespace DB
{
template <typename ExecutorImpl>
class ExecutorStatistics : public ExecutorStatisticsBase
{
public:
    ExecutorStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
        : dag_context(dag_context_)
    {
        assert(executor->has_executor_id());
        executor_id = executor->executor_id();

        auto parse_id = [](const String & executor_id_) -> Int64 {
            auto split_index = executor_id_.find('_');
            if (split_index == String::npos || split_index == (executor_id_.size() - 1))
            {
                throw TiFlashException("Illegal executor_id: " + executor_id_, Errors::Coprocessor::Internal);
            }
            return std::stoi(executor_id_.substr(split_index + 1, executor_id_.size()));
        };

        type = ExecutorImpl::type;
        id = parse_id(executor_id);

        for (const auto * child : getChildren(*executor))
        {
            assert(child->has_executor_id());
            children.push_back(parse_id(child->executor_id()));
        }
    }

    virtual String toJson() const override
    {
        FmtBuffer fmt_buffer;
        fmt_buffer.fmtAppend(
            R"({{"id":{},"type":"{}","children":[{}],"outbound_rows":{},"outbound_blocks":{},"outbound_bytes":{},"execution_time_ns":{})",
            id,
            type,
            fmt::join(children, ","),
            outbound_rows,
            outbound_blocks,
            outbound_bytes,
            execution_time_ns);
        if constexpr (ExecutorImpl::has_extra_info)
        {
            fmt_buffer.append(",");
            appendExtraJson(fmt_buffer);
        }
        fmt_buffer.append("}");
        return fmt_buffer.toString();
    }

    void collectRuntimeDetail() override
    {
        throw new Exception("Unsupported");
    }

    static bool isMatch(const tipb::Executor * executor)
    {
        return ExecutorImpl::isMatch(executor);
    }

protected:
    String executor_id;
    Int64 id;
    String type;

    std::vector<Int64> children;

    size_t outbound_rows = 0;
    size_t outbound_blocks = 0;
    size_t outbound_bytes = 0;

    UInt64 execution_time_ns = 0;

    DAGContext & dag_context;

    virtual void appendExtraJson(FmtBuffer &) const {}
};
} // namespace DB