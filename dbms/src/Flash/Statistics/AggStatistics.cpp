#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/AggStatistics.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <fmt/format.h>

namespace DB
{
String AggStatistics::extraToJson() const
{
    return fmt::format(
        R"(,"inbound_rows":{},"inbound_blocks":{},"inbound_bytes":{},"hash_table_rows":{})",
        inbound_rows,
        inbound_blocks,
        inbound_bytes,
        hash_table_rows);
}

bool AggStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "HashAgg_") || startsWith(executor_id, "StreamAgg_");
}

AggStatistics::AggStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : ExecutorStatistics(executor, dag_context_)
{}

void AggStatistics::collectRuntimeDetail()
{
    const auto & profile_streams_info = dag_context.getProfileStreams(executor_id);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                elseThen(
                    [&]() {
                        return castBlockInputStream<AggregatingBlockInputStream>(stream_ptr, [&](const AggregatingBlockInputStream & stream) {
                            hash_table_rows += stream.getAggregatedDataVariantsSizeWithoutOverflowRow();
                            collectBaseInfo(this, stream.getProfileInfo());
                        });
                    },
                    [&]() {
                        return castBlockInputStream<ParallelAggregatingBlockInputStream>(stream_ptr, [&](const ParallelAggregatingBlockInputStream & stream) {
                            hash_table_rows += stream.getAggregatedDataVariantsSizeWithoutOverflowRow();
                            collectBaseInfo(this, stream.getProfileInfo());
                        });
                    }),
                stream_ptr->getName(),
                "AggregatingBlockInputStream/ParallelAggregatingBlockInputStream");
        },
        [&](const BlockInputStreamPtr & child_stream_ptr) {
            throwFailCastException(
                castBlockInputStream<IProfilingBlockInputStream>(child_stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                    collectInboundInfo(this, stream.getProfileInfo());
                }),
                child_stream_ptr->getName(),
                "IProfilingBlockInputStream");
        });
}

} // namespace DB