#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/LimitStatistics.h>
#include <Interpreters/Context.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String LimitStatistics::extraToJson() const
{
    return fmt::format(
        R"(,"limit":{},"inbound_rows":{},"inbound_blocks":{},"inbound_bytes":{})",
        limit,
        inbound_rows,
        inbound_blocks,
        inbound_bytes);
}

LimitStatistics::LimitStatistics(const tipb::Executor * executor, Context & context_)
    : ExecutorStatistics(executor, context_)
{
    assert(executor->tp() == tipb::ExecType::TypeLimit);
    const auto & limit_executor = executor->limit();
    assert(limit_executor.has_limit());
    limit = limit_executor.limit();
}

bool LimitStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "Limit_");
}

void LimitStatistics::collectRuntimeDetail()
{
    const auto & profile_streams_info = context.getDAGContext()->getProfileStreams(executor_id);
    if (profile_streams_info.input_streams.size() != 1)
        throw TiFlashException(
            fmt::format("Count of LimitBlockInputStream should be 1 or not {}", profile_streams_info.input_streams.size()),
            Errors::Coprocessor::Internal);

    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<LimitBlockInputStream>(stream_ptr, [&](const LimitBlockInputStream & stream) {
                    collectBaseInfo(this, stream.getProfileInfo());
                }),
                stream_ptr->getName(),
                "LimitBlockInputStream");
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