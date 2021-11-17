#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/LimitStatistics.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String LimitStatistics::toJson() const
{
    return fmt::format(
        R"({{"id":"{}","type":"{}","limit":{},"offset":{},"rows_selectivity":{},"blocks_selectivity":{},"bytes_selectivity":{}}})",
        id,
        type,
        limit,
        offset,
        divide(outbound_rows, inbound_rows),
        divide(outbound_blocks, inbound_blocks),
        divide(outbound_bytes, inbound_bytes));
}

bool LimitStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "Limit_");
}

ExecutorStatisticsPtr LimitStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info [[maybe_unused]], DAGContext & dag_context [[maybe_unused]])
{
    if (profile_streams_info.input_streams.size() != 1)
        throw TiFlashException(
            fmt::format("Count of LimitBlockInputStream should be 1 or not {}", profile_streams_info.input_streams.size()),
            Errors::Coprocessor::Internal);

    using LimitStatisticsPtr = std::shared_ptr<LimitStatistics>;
    LimitStatisticsPtr statistics = std::make_shared<LimitStatistics>(executor_id);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<LimitBlockInputStream>(stream_ptr, [&](const LimitBlockInputStream & stream) {
                    statistics->limit = stream.getLimit();
                    statistics->offset = stream.getOffset();
                    const auto & profile_info = stream.getProfileInfo();
                    statistics->outbound_rows += profile_info.rows;
                    statistics->outbound_blocks += profile_info.blocks;
                    statistics->outbound_bytes += profile_info.bytes;
                }),
                stream_ptr->getName(),
                "LimitBlockInputStream");
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