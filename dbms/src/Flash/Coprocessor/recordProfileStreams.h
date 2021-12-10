#pragma once

#include <Flash/Coprocessor/DAGPipeline.h>
#include <common/types.h>

#include <map>

namespace DB
{
class DAGContext;
void recordProfileStreams(DAGContext & dag_context, DAGPipeline & pipeline, const String & key, UInt32 qb_id);
} // namespace DB