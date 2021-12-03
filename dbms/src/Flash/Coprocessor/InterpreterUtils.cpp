#include <DataStreams/SharedQueryBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Interpreters/Context.h>

namespace DB
{
void restoreConcurrency(DAGPipeline & pipeline, size_t concurrency, const LogWithPrefixPtr & log)
{
    if (concurrency > 1 && pipeline.streams.size() == 1 && pipeline.streams_with_non_joined_data.empty())
    {
        BlockInputStreamPtr shared_query_block_input_stream
            = std::make_shared<SharedQueryBlockInputStream>(concurrency * 5, pipeline.firstStream(), log);
        pipeline.streams.assign(concurrency, shared_query_block_input_stream);
    }
}

BlockInputStreamPtr combinedNonJoinedDataStream(DAGPipeline & pipeline, size_t max_threads, const LogWithPrefixPtr & log)
{
    BlockInputStreamPtr ret = nullptr;
    if (pipeline.streams_with_non_joined_data.size() == 1)
        ret = pipeline.streams_with_non_joined_data.at(0);
    else if (pipeline.streams_with_non_joined_data.size() > 1)
        ret = std::make_shared<UnionBlockInputStream<>>(pipeline.streams_with_non_joined_data, nullptr, max_threads, log);
    pipeline.streams_with_non_joined_data.clear();
    return ret;
}

void executeUnion(DAGPipeline & pipeline, size_t max_streams, const LogWithPrefixPtr & log)
{
    if (pipeline.streams.size() == 1 && pipeline.streams_with_non_joined_data.empty())
        return;
    auto non_joined_data_stream = combinedNonJoinedDataStream(pipeline, max_streams, log);
    if (!pipeline.streams.empty())
    {
        pipeline.firstStream() = std::make_shared<UnionBlockInputStream<>>(pipeline.streams, non_joined_data_stream, max_streams, log);
        pipeline.streams.resize(1);
    }
    else if (non_joined_data_stream != nullptr)
    {
        pipeline.streams.push_back(non_joined_data_stream);
    }
}

void recordProfileStreams(DAGContext & dag_context, const DAGPipeline & pipeline, const String & key, UInt32 query_block_id)
{
    dag_context.getProfileStreamsMap()[key].qb_id = query_block_id;
    for (auto & stream : pipeline.streams)
        dag_context.getProfileStreamsMap()[key].input_streams.push_back(stream);
    for (auto & stream : pipeline.streams_with_non_joined_data)
        dag_context.getProfileStreamsMap()[key].input_streams.push_back(stream);
}

void recordProfileStream(DAGContext & dag_context, const BlockInputStreamPtr & stream, const String & key, UInt32 query_block_id)
{
    dag_context.getProfileStreamsMap()[key].qb_id = query_block_id;
    dag_context.getProfileStreamsMap()[key].input_streams.push_back(stream);
}

ExpressionActionsPtr generateProjectExpressionActions(
    const Block & header,
    const Context & context,
    const NamesWithAliases & project_cols)
{
    NamesAndTypesList input_column;
    for (const auto & column : header.getColumnsWithTypeAndName())
    {
        input_column.emplace_back(column.name, column.type);
    }
    ExpressionActionsPtr project = std::make_shared<ExpressionActions>(input_column, context.getSettingsRef());
    project->add(ExpressionAction::project(project_cols));
    return project;
}

} // namespace DB
