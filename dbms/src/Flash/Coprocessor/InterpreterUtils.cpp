#include <DataStreams/SharedQueryBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Flash/Coprocessor/InterpreterUtils.h>

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
} // namespace DB
