#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/LocalReadProfileInfo.h>
#include <Flash/Statistics/TableScanStatistics.h>
#include <fmt/format.h>

namespace DB
{
String TableScanStatistics::toJson() const
{
    return fmt::format(
        R"({{"id":"{}","type":"{}","connection_details":{}}})",
        id,
        type,
        arrayToJson(connection_profile_infos));
}

bool TableScanStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "TableFullScan_");
}

ExecutorStatisticsPtr TableScanStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context [[maybe_unused]])
{
    using TableScanStatisticsPtr = std::shared_ptr<TableScanStatistics>;
    TableScanStatisticsPtr statistics = std::make_shared<TableScanStatistics>(executor_id);
    LocalReadProfileInfoPtr local_read_info = std::make_shared<LocalReadProfileInfo>();
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                elseThen(
                    [&]() {
                        return castBlockInputStream<CoprocessorBlockInputStream>(stream_ptr, [&](const CoprocessorBlockInputStream & stream) {
                            const auto & cop_profile_infos = stream.getConnectionProfileInfos();
                            assert(cop_profile_infos.size() == 1);
                            statistics->connection_profile_infos.push_back(cop_profile_infos.back());
                        });
                    },
                    [&]() {
                        return castBlockInputStream<IProfilingBlockInputStream>(stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                            const auto & profile_info = stream.getProfileInfo();
                            local_read_info->rows += profile_info.rows;
                            local_read_info->blocks += profile_info.blocks;
                            local_read_info->bytes += profile_info.bytes;
                        });
                    }),
                stream_ptr->getName(),
                "CoprocessorBlockInputStream/IProfilingBlockInputStream");
        });
    statistics->connection_profile_infos.push_back(local_read_info);
    return statistics;
}
} // namespace DB