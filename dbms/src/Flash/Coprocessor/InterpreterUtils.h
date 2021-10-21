#pragma once

#include <Common/LogWithPrefix.h>
#include <Flash/Coprocessor/DAGPipeline.h>

namespace DB
{
void restoreConcurrency(DAGPipeline & pipeline, size_t concurrency, const LogWithPrefixPtr & log);
} // namespace DB
