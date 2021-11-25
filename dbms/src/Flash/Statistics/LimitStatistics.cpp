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

bool LimitStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "Limit_");
}

ExecutorStatisticsPtr LimitStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, Context & context)
{
    if (profile_streams_info.input_streams.size() != 1)
        throw TiFlashException(
            fmt::format("Count of LimitBlockInputStream should be 1 or not {}", profile_streams_info.input_streams.size()),
            Errors::Coprocessor::Internal);

    using LimitStatisticsPtr = std::shared_ptr<LimitStatistics>;
    LimitStatisticsPtr statistics = std::make_shared<LimitStatistics>(executor_id, context);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<LimitBlockInputStream>(stream_ptr, [&](const LimitBlockInputStream & stream) {
                    collectBaseInfo(statistics, stream.getProfileInfo());
                }),
                stream_ptr->getName(),
                "LimitBlockInputStream");
            return true;
        },
        [&](const BlockInputStreamPtr & child_stream_ptr) {
            throwFailCastException(
                castBlockInputStream<IProfilingBlockInputStream>(child_stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                    collectInboundInfo(statistics, stream.getProfileInfo());
                }),
                child_stream_ptr->getName(),
                "IProfilingBlockInputStream");
        });

    const auto * executor = context.getDAGContext()->getExecutor(executor_id);
    assert(executor->tp() == tipb::ExecType::TypeLimit);
    const auto & limit_executor = executor->limit();
    assert(limit_executor.has_limit());
    statistics->limit = limit_executor.limit();

    return statistics;
}
} // namespace DB