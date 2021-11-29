#include <Common/joinStr.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/LocalReadProfileInfo.h>
#include <Flash/Statistics/TableScanStatistics.h>
#include <Interpreters/Context.h>
#include <fmt/format.h>

namespace DB
{
String TableScanStatistics::extraToJson() const
{
    FmtBuffer buffer;
    buffer.fmtAppend(R"(,"db":"{}","table":"{}","ranges":[)", db, table);
    joinStr(
        ranges.cbegin(),
        ranges.cend(),
        buffer,
        [](const tipb::KeyRange & r, FmtBuffer & fb) {
            assert(r.has_low());
            assert(r.has_high());
            fb.fmtAppend("\"[{},{})\"", r.low(), r.high());
        },
        ",");
    buffer.fmtAppend(R"(],"connection_details":{})", arrayToJson(connection_profile_infos));
    return buffer.toString();
}

bool TableScanStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "TableFullScan_");
}

ExecutorStatisticsPtr TableScanStatistics::buildStatistics(const String & executor_id, const ProfileStreamsInfo & profile_streams_info, Context & context)
{
    using TableScanStatisticsPtr = std::shared_ptr<TableScanStatistics>;
    TableScanStatisticsPtr statistics = std::make_shared<TableScanStatistics>(executor_id, context);
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
                            collectBaseInfo(statistics, stream.getProfileInfo());
                        });
                    },
                    [&]() {
                        return castBlockInputStream<IProfilingBlockInputStream>(stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                            const auto & profile_info = stream.getProfileInfo();
                            collectBaseInfo(statistics, profile_info);
                            local_read_info->bytes += profile_info.bytes;
                        });
                    }),
                stream_ptr->getName(),
                "CoprocessorBlockInputStream/IProfilingBlockInputStream");
        });
    statistics->connection_profile_infos.push_back(local_read_info);

    const auto * executor = context.getDAGContext()->getExecutor(executor_id);
    assert(executor->tp() == tipb::ExecType::TypeTableScan);
    const auto & table_scan_executor = executor->tbl_scan();
    assert(table_scan_executor.has_table_id());
    auto & tmt_ctx = context.getTMTContext();
    auto storage = tmt_ctx.getStorages().get(table_scan_executor.table_id());
    assert(storage != nullptr);
    statistics->db = storage->getDatabaseName();
    statistics->table = storage->getTableName();
    for (const auto & range : table_scan_executor.ranges())
        statistics->ranges.push_back(range);

    return statistics;
}
} // namespace DB