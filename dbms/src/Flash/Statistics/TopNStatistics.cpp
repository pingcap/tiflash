#include <Common/joinStr.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/TopNStatistics.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String TopNStatistics::toJson() const
{
    return fmt::format(
        R"({{"id":"{}","type":"{}","limit":{},"sort_desc":"{}","rows_selectivity":{},"blocks_selectivity":{},"bytes_selectivity":{}}})",
        id,
        type,
        limit,
        sort_desc,
        divide(outbound_rows, inbound_rows),
        divide(outbound_blocks, inbound_blocks),
        divide(outbound_bytes, inbound_bytes));
}

bool TopNStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "TopN_");
}

ExecutorStatisticsPtr TopNStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info [[maybe_unused]], DAGContext & dag_context [[maybe_unused]])
{
    if (profile_streams_info.input_streams.size() != 1)
        throw TiFlashException(
            fmt::format("Count of MergeSortingBlockInputStream should be 1 or not {}", profile_streams_info.input_streams.size()),
            Errors::Coprocessor::Internal);

    using TopNStatisticsPtr = std::shared_ptr<TopNStatistics>;
    TopNStatisticsPtr statistics = std::make_shared<TopNStatistics>(executor_id);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<MergeSortingBlockInputStream>(stream_ptr, [&](const MergeSortingBlockInputStream & stream) {
                    statistics->limit = stream.getLimit();

                    FmtBuffer buffer;
                    const auto & sort_description = stream.getSortDescription();
                    joinStr(
                        sort_description.cbegin(),
                        sort_description.cend(),
                        buffer,
                        [](const auto & s, FmtBuffer & fb) { fb.template fmtAppend("[{}]", s.getID()); });
                    statistics->sort_desc = buffer.toString();

                    const auto & profile_info = stream.getProfileInfo();
                    statistics->outbound_rows += profile_info.rows;
                    statistics->outbound_blocks += profile_info.blocks;
                    statistics->outbound_bytes += profile_info.bytes;
                }),
                stream_ptr->getName(),
                "MergeSortingBlockInputStream");
        },
        [&](const BlockInputStreamPtr & child_stream_ptr) {
            throwFailCastException(
                castBlockInputStream<IProfilingBlockInputStream>(child_stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                    const auto & profile_info = stream.getProfileInfo();
                    statistics->inbound_rows += profile_info.rows;
                    statistics->inbound_blocks += profile_info.blocks;
                    statistics->inbound_bytes += profile_info.bytes;
                }),
                child_stream_ptr->getName(),
                "IProfilingBlockInputStream");
        });
    return statistics;
}
} // namespace DB