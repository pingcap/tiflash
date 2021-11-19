#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/TableScanStatistics.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB
{
String TableScanStatistics::toJson() const
{
    return fmt::format(
        R"({{"id":"{}","type":"{}"}})",
        id,
        type);
}

bool TableScanStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "TableFullScan_");
}

ExecutorStatisticsPtr TableScanStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info [[maybe_unused]], DAGContext & dag_context [[maybe_unused]])
{
    using TableScanStatisticsPtr = std::shared_ptr<TableScanStatistics>;
    TableScanStatisticsPtr statistics = std::make_shared<TableScanStatistics>(executor_id);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                elseThen(
                    [&]() {
                        return castBlockInputStream<CoprocessorBlockInputStream>(stream_ptr, [&](const CoprocessorBlockInputStream &) {});
                    },
                    [&]() {
                        return castBlockInputStream<IProfilingBlockInputStream>(stream_ptr, [&](const IProfilingBlockInputStream &) {});
                    }),
                stream_ptr->getName(),
                "CoprocessorBlockInputStream/IProfilingBlockInputStream");
        });
    return statistics;
}
} // namespace DB