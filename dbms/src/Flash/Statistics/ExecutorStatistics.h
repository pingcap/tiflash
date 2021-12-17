#pragma once

#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsBase.h>
#include <Flash/Statistics/traverseExecutors.h>
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

        type = ExecutorImpl::type;

        getChildren(*executor).forEach([&](const tipb::Executor & child) {
            assert(child.has_executor_id());
            children.push_back(child.executor_id());
        });
    }

    virtual String toJson() const override
    {
        FmtBuffer fmt_buffer;
        fmt_buffer.fmtAppend(
            R"({{"id":"{}","type":"{}","children":[)",
            executor_id,
            type);
        fmt_buffer.joinStr(
            children.cbegin(),
            children.cend(),
            [](const String & child, FmtBuffer & bf) { bf.fmtAppend(R"("{}")", child); },
            ",");
        fmt_buffer.fmtAppend(
            R"(],"outbound_rows":{},"outbound_blocks":{},"outbound_bytes":{},"execution_time_ns":{})",
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
        const auto & profile_streams_info = dag_context.getProfileStreams(executor_id);
        assert(!profile_streams_info.input_streams.empty());
        for (const auto & input_stream : profile_streams_info.input_streams)
        {
            auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(input_stream.get());
            assert(p_stream);
            const auto & profile_info = p_stream->getProfileInfo();
            outbound_rows += profile_info.rows;
            outbound_blocks += profile_info.blocks;
            outbound_bytes += profile_info.bytes;
            execution_time_ns = std::max(execution_time_ns, profile_info.execution_time);
        }
        if constexpr (ExecutorImpl::has_extra_info)
        {
            collectExtraRuntimeDetail();
        }
    }

    static bool isMatch(const tipb::Executor * executor)
    {
        return ExecutorImpl::isMatch(executor);
    }

protected:
    String executor_id;
    String type;

    std::vector<String> children;

    size_t outbound_rows = 0;
    size_t outbound_blocks = 0;
    size_t outbound_bytes = 0;

    UInt64 execution_time_ns = 0;

    DAGContext & dag_context;

    virtual void appendExtraJson(FmtBuffer &) const {}

    virtual void collectExtraRuntimeDetail() {}
};
} // namespace DB