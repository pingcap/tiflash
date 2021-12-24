#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Statistics/TableScanImpl.h>
#include <Interpreters/Join.h>

namespace DB
{
String TableScanDetail::toJson() const
{
    return fmt::format(
        R"({{"is_local":{},"packets":{},"bytes":{}}})",
        is_local,
        packets,
        bytes);
}

void TableScanStatistics::appendExtraJson(FmtBuffer & fmt_buffer) const
{
    fmt_buffer.fmtAppend(
        R"("connection_details":[{},{}])",
        local_table_scan_detail.toJson(),
        cop_table_scan_detail.toJson());
}

void TableScanStatistics::collectExtraRuntimeDetail()
{
    const auto & io_stream_map = dag_context.getInBoundIOInputStreamsMap();
    auto it = io_stream_map.find(executor_id);
    if (it != io_stream_map.end())
    {
        for (const auto & io_stream : it->second)
        {
            if (auto * cop_stream = dynamic_cast<CoprocessorBlockInputStream *>(io_stream.get()))
            {
                for (const auto & connection_profile_info : cop_stream->getConnectionProfileInfos())
                {
                    cop_table_scan_detail.packets += connection_profile_info.packets;
                    cop_table_scan_detail.bytes += connection_profile_info.bytes;
                }
            }
            else /// local read input stream also is IProfilingBlockInputStream
            {
                auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(io_stream.get());
                assert(p_stream);
                cop_table_scan_detail.bytes += p_stream->getProfileInfo().bytes;
            }
        }
    }
}

TableScanStatistics::TableScanStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : TableScanStatisticsBase(executor, dag_context_)
{}
} // namespace DB