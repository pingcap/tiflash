#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/ProfileStreamsInfo.h>
#include <Flash/Coprocessor/recordProfileStreams.h>

namespace DB
{
void recordProfileStreams(DAGContext & dag_context, DAGPipeline & pipeline, const String & key, UInt32 qb_id)
{
    auto & profile_streams_info = dag_context.getProfileStreamsMap()[key];
    profile_streams_info.qb_id = qb_id;
    pipeline.transform([&](const auto & stream) { profile_streams_info.input_streams.push_back(stream); });
}
} // namespace DB