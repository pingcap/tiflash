#pragma once

#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <common/types.h>

namespace DB
{
void recordProfileStreams(const DAGQuerySource & dag, DAGPipeline & pipeline, const String & key, UInt32 qb_id = 0)
{
    dag.getDAGContext().getProfileStreamsMap()[key].qb_id = qb_id;
    for (auto & stream : pipeline.streams)
    {
        dag.getDAGContext().getProfileStreamsMap()[key].input_streams.push_back(stream);
    }
    for (auto & stream : pipeline.streams_with_non_joined_data)
    {
        dag.getDAGContext().getProfileStreamsMap()[key].input_streams.push_back(stream);
    }
}
} // namespace DB