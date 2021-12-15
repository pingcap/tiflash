#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/TableScanStatistics.h>
#include <fmt/format.h>

namespace DB
{
String TableScanDetail::toJson() const
{
    return fmt::format(
        R"({{"is_local":{},"packets":{},"bytes":{}}})",
        is_local,
        connection_profile_info.packets,
        connection_profile_info.bytes);
}

String TableScanStatistics::extraToJson() const
{
    return fmt::format(
        R"(,"connection_details":[{},{}])",
        local_table_scan_detail.toJson(),
        cop_table_scan_detail.toJson());
}

TableScanStatistics::TableScanStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : ExecutorStatistics(executor, dag_context_)
{
    local_table_scan_detail.is_local = true;
    cop_table_scan_detail.is_local = false;
}

bool TableScanStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "TableFullScan_");
}

void TableScanStatistics::collectRuntimeDetail()
{
    const auto & profile_streams_info = dag_context.getProfileStreams(executor_id);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                elseThen(
                    [&]() {
                        return castBlockInputStream<CoprocessorBlockInputStream>(stream_ptr, [&](const CoprocessorBlockInputStream & stream) {
                            collectBaseInfo(this, stream.getProfileInfo());
                            for (const auto & profile_info : stream.getConnectionProfileInfos())
                            {
                                cop_table_scan_detail.connection_profile_info.bytes += profile_info.bytes;
                                cop_table_scan_detail.connection_profile_info.packets += profile_info.packets;
                            }
                        });
                    },
                    [&]() {
                        return castBlockInputStream<IProfilingBlockInputStream>(stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                            const auto & profile_info = stream.getProfileInfo();
                            collectBaseInfo(this, profile_info);
                            local_table_scan_detail.connection_profile_info.bytes += profile_info.bytes;
                        });
                    }),
                stream_ptr->getName(),
                "CoprocessorBlockInputStream/IProfilingBlockInputStream");
        });
}
} // namespace DB