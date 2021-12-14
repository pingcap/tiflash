#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/TableScanStatistics.h>
#include <fmt/format.h>

namespace DB
{
TableScanStatistics::TableScanStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : ExecutorStatistics(executor, dag_context_)
{}

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
                        });
                    },
                    [&]() {
                        return castBlockInputStream<IProfilingBlockInputStream>(stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                            collectBaseInfo(this, stream.getProfileInfo());
                        });
                    }),
                stream_ptr->getName(),
                "CoprocessorBlockInputStream/IProfilingBlockInputStream");
        });
}
} // namespace DB