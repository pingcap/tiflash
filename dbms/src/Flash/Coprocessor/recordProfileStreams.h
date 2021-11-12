#pragma once

#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/ProfileStreamsInfo.h>
#include <common/types.h>

#include <map>

namespace DB
{
void recordProfileStreams(std::map<String, ProfileStreamsInfo> & profile_streams_map, DAGPipeline & pipeline, const String & key, UInt32 qb_id = 0);
} // namespace DB