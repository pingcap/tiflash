#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>
#include <Flash/Statistics/FilterStatistics.h>
#include <Interpreters/Context.h>
#include <fmt/format.h>

namespace DB
{
String FilterStatistics::extraToJson() const
{
    return fmt::format(
        R"(,"inbound_rows":{},"inbound_blocks":{},"inbound_bytes":{})",
        inbound_rows,
        inbound_blocks,
        inbound_bytes);
}

FilterStatistics::FilterStatistics(const tipb::Executor * executor, Context & context_)
    : ExecutorStatistics(executor, context_)
{}

bool FilterStatistics::hit(const String & executor_id)
{
    return startsWith(executor_id, "Selection_");
}

void FilterStatistics::collectRuntimeDetail()
{
    const auto & profile_streams_info = context.getDAGContext()->getProfileStreams(executor_id);
    visitBlockInputStreams(
        profile_streams_info.input_streams,
        [&](const BlockInputStreamPtr & stream_ptr) {
            throwFailCastException(
                elseThen(
                    [&]() {
                        return castBlockInputStream<FilterBlockInputStream>(stream_ptr, [&](const FilterBlockInputStream & stream) {
                            collectBaseInfo(this, stream.getProfileInfo());
                        });
                    },
                    [&]() {
                        return castBlockInputStream<IProfilingBlockInputStream>(stream_ptr, [&](const IProfilingBlockInputStream & stream) {
                            collectBaseInfo(this, stream.getProfileInfo());
                        });
                    }),
                stream_ptr->getName(),
                "FilterBlockInputStream/IProfilingBlockInputStream");
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