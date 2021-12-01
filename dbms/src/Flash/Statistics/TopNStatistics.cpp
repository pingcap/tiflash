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
        R"(,"limit":{},"sort_desc":"{}","inbound_rows":{},"inbound_blocks":{},"inbound_bytes":{})",
        limit,
        sort_desc,
        inbound_rows,
        inbound_blocks,
        inbound_bytes);
}

bool TopNStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "TopN_");
}

ExecutorStatisticsPtr TopNStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, Context & context)
{
    if (profile_streams_info.input_streams.size() != 1)
        throw TiFlashException(
            fmt::format("Count of MergeSortingBlockInputStream should be 1 or not {}", profile_streams_info.input_streams.size()),
            Errors::Coprocessor::Internal);

    using TopNStatisticsPtr = std::shared_ptr<TopNStatistics>;
    TopNStatisticsPtr statistics = std::make_shared<TopNStatistics>(executor_id, context);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<MergeSortingBlockInputStream>(stream_ptr, [&](const MergeSortingBlockInputStream & stream) {
                    FmtBuffer buffer;
                    const auto & sort_description = stream.getSortDescription();
                    joinStr(
                        sort_description.cbegin(),
                        sort_description.cend(),
                        buffer,
                        [](const auto & s, FmtBuffer & fb) { fb.template fmtAppend("[{}]", s.getID()); });
                    statistics->sort_desc = buffer.toString();

                    collectBaseInfo(statistics, stream.getProfileInfo());
                }),
                stream_ptr->getName(),
                "MergeSortingBlockInputStream");
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
    assert(executor->tp() == tipb::ExecType::TypeTopN);
    const auto & top_n_executor = executor->topn();
    assert(top_n_executor.has_limit());
    statistics->limit = top_n_executor.limit();

    return statistics;
}
} // namespace DB