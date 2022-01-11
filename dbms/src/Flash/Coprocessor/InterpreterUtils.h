#pragma once

#include <Common/LogWithPrefix.h>
#include <Flash/Coprocessor/DAGPipeline.h>

namespace DB
{
void restoreConcurrency(
    DAGPipeline & pipeline,
    size_t concurrency,
    const LogWithPrefixPtr & log);

BlockInputStreamPtr combinedNonJoinedDataStream(
    DAGPipeline & pipeline,
    size_t max_threads,
    const LogWithPrefixPtr & log,
    bool ignore_block = false);

void executeUnion(
    DAGPipeline & pipeline,
    size_t max_streams,
    const LogWithPrefixPtr & log,
    bool ignore_block = false);
} // namespace DB
