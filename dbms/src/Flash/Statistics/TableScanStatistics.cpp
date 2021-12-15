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
    return fmt::format(
        R"(,"db":"{}","table":"{}","connection_details":{})",
        db,
        table,
        arrayToJson(connection_profile_infos));
}

TableScanStatistics::TableScanStatistics(const tipb::Executor * executor, Context & context_)
    : ExecutorStatistics(executor, context_)
{
    assert(executor->tp() == tipb::ExecType::TypeTableScan);
    const auto & table_scan_executor = executor->tbl_scan();
    assert(table_scan_executor.has_table_id());
    auto & tmt_ctx = context.getTMTContext();
    auto storage = tmt_ctx.getStorages().get(table_scan_executor.table_id());
    assert(storage != nullptr);
    db = storage->getDatabaseName();
    table = storage->getTableName();
}

bool TableScanStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "TableFullScan_") || startsWith(executor_id, "TableRangeScan_");
}

void TableScanStatistics::collectRuntimeDetail()
{
    const auto & profile_streams_info = context.getDAGContext()->getProfileStreams(executor_id);
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
                            connection_profile_infos.push_back(cop_profile_infos.back());
                            collectBaseInfo(this, stream.getProfileInfo());
                        });
                    },
                    [&]() {
                        return castBlockInputStream<IProfilingBlockInputStream>(stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                            const auto & profile_info = stream.getProfileInfo();
                            collectBaseInfo(this, profile_info);
                            local_read_info->bytes += profile_info.bytes;
                        });
                    }),
                stream_ptr->getName(),
                "CoprocessorBlockInputStream/IProfilingBlockInputStream");
        });
    connection_profile_infos.push_back(local_read_info);
}
} // namespace DB