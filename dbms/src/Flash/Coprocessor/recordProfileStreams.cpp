#include <Flash/Coprocessor/recordProfileStreams.h>

namespace DB
{
void recordProfileStreams(std::map<String, ProfileStreamsInfo> & profile_streams_map, DAGPipeline & pipeline, const String & key, UInt32 qb_id)
{
    profile_streams_map[key].qb_id = qb_id;
    for (auto & stream : pipeline.streams)
    {
        profile_streams_map[key].input_streams.push_back(stream);
    }
    for (auto & stream : pipeline.streams_with_non_joined_data)
    {
        profile_streams_map[key].input_streams.push_back(stream);
    }
}
} // namespace DB