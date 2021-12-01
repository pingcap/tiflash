#include <Common/joinStr.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/TopNStatistics.h>
#include <Interpreters/Context.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String TopNStatistics::extraToJson() const
{
    return fmt::format(
        R"(,"limit":{},"inbound_rows":{},"inbound_blocks":{},"inbound_bytes":{})",
        limit,
        inbound_rows,
        inbound_blocks,
        inbound_bytes);
}

TopNStatistics::TopNStatistics(const tipb::Executor * executor, Context & context_)
    : ExecutorStatistics(executor, context_)
{
    assert(executor->tp() == tipb::ExecType::TypeTopN);
    const auto & top_n_executor = executor->topn();
    assert(top_n_executor.has_limit());
    limit = top_n_executor.limit();
}

bool TopNStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "TopN_");
}

void TopNStatistics::collectRuntimeDetail()
{
    const auto & profile_streams_info = context.getDAGContext()->getProfileStreams(executor_id);
    if (profile_streams_info.input_streams.size() != 1)
        throw TiFlashException(
            fmt::format("Count of MergeSortingBlockInputStream should be 1 or not {}", profile_streams_info.input_streams.size()),
            Errors::Coprocessor::Internal);

    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<MergeSortingBlockInputStream>(stream_ptr, [&](const MergeSortingBlockInputStream & stream) {
                    collectBaseInfo(this, stream.getProfileInfo());
                }),
                stream_ptr->getName(),
                "MergeSortingBlockInputStream");
        },
        [&](const BlockInputStreamPtr & child_stream_ptr) {
            throwFailCastException(
                castBlockInputStream<IProfilingBlockInputStream>(child_stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                    collectInboundInfo(this, stream.getProfileInfo());
                }),
                child_stream_ptr->getName(),
                "IProfilingBlockInputStream");
        });
}
} // namespace DB