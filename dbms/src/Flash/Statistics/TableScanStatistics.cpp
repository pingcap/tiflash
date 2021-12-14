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
TableScanStatistics::TableScanStatistics(const tipb::Executor * executor, Context & context_)
    : ExecutorStatistics(executor, context_)
{}

bool TableScanStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "TableFullScan_");
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