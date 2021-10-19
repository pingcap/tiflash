#include <DataStreams/SharedQueryBlockInputStream.h>
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
}

