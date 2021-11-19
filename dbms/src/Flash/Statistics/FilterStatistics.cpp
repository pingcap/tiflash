#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/FilterStatistics.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String FilterStatistics::toJson() const
{
    return fmt::format(
        R"({{"id":"{}","type":"{}","rows_selectivity":{},"blocks_selectivity":{},"bytes_selectivity":{},"avg_rows_per_block":{},"avg_bytes_per_block":{}}})",
        id,
        type,
        divide(outbound_rows, inbound_rows),
        divide(outbound_blocks, inbound_blocks),
        divide(outbound_bytes, inbound_bytes),
        divide(outbound_rows, outbound_blocks),
        divide(outbound_bytes, outbound_blocks));
}

bool FilterStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "Selection_");
}

ExecutorStatisticsPtr FilterStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context [[maybe_unused]])
{
    using FilterStatisticsPtr = std::shared_ptr<FilterStatistics>;
    FilterStatisticsPtr statistics = std::make_shared<FilterStatistics>(executor_id);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                castBlockInputStream<FilterBlockInputStream>(stream_ptr, [&](const FilterBlockInputStream & stream) {
                    const auto & profile_info = stream.getProfileInfo();
                    statistics->outbound_rows += profile_info.rows;
                    statistics->outbound_blocks += profile_info.blocks;
                    statistics->outbound_bytes += profile_info.bytes;
                }),
                stream_ptr->getName(),
                "FilterBlockInputStream");
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