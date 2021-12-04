#pragma once

#include <Common/LogWithPrefix.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>

namespace DB
{
void restoreConcurrency(DAGPipeline & pipeline, size_t concurrency, const LogWithPrefixPtr & log);

BlockInputStreamPtr combinedNonJoinedDataStream(DAGPipeline & pipeline, size_t max_threads, const LogWithPrefixPtr & log);

void executeUnion(DAGPipeline & pipeline, size_t max_streams, const LogWithPrefixPtr & log);

void recordProfileStreams(DAGContext & dag_context, const DAGPipeline & pipeline, const String & key, UInt32 query_block_id);
void recordProfileStream(DAGContext & dag_context, const BlockInputStreamPtr & stream, const String & key, UInt32 query_block_id);

ExpressionActionsPtr generateProjectExpressionActions(
    const Block & header,
    const Context & context,
    const NamesWithAliases & project_cols);

} // namespace DB
