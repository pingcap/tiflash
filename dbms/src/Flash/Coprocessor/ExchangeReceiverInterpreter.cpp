#include <Common/TiFlashException.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/ExchangeReceiverInterpreter.h>
#include <Flash/Coprocessor/InterpreterUtils.h>

namespace DB
{
ExchangeReceiverInterpreter::ExchangeReceiverInterpreter(
    Context & context_,
    const DAGQueryBlock & query_block_,
    size_t max_streams_,
    bool keep_session_timezone_info_,
    const std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & exchange_receiver_map_,
    const LogWithPrefixPtr & log_)
    : DAGInterpreterBase(
        context_,
        query_block_,
        max_streams_,
        keep_session_timezone_info_,
        log_)
    , exchange_receiver_map(exchange_receiver_map_)
{
    assert(query_block.source->tp() == tipb::ExecType::TypeExchangeReceiver);
}

// To execute a query block, you have to:
// 1. generate the date stream and push it to pipeline.
// 2. assign the analyzer
// 3. construct a final projection, even if it's not necessary. just construct it.
// Talking about projection, it has following rules.
// 1. if the query block does not contain agg, then the final project is the same as the source Executor
// 2. if the query block contains agg, then the final project is the same as agg Executor
// 3. if the cop task may contains more then 1 query block, and the current query block is not the root
//    query block, then the project should add an alias for each column that needs to be projected, something
//    like final_project.emplace_back(col.name, query_block.qb_column_prefix + col.name);
void ExchangeReceiverInterpreter::executeImpl(DAGPipelinePtr & pipeline)
{
    auto it = exchange_receiver_map.find(query_block.source_name);
    if (unlikely(it == exchange_receiver_map.end()))
        throw Exception("Can not find exchange receiver for " + query_block.source_name, ErrorCodes::LOGICAL_ERROR);
    // todo choose a more reasonable stream number
    for (size_t i = 0; i < max_streams; i++)
    {
        BlockInputStreamPtr stream = std::make_shared<ExchangeReceiverInputStream>(it->second, log);
        dagContext().getRemoteInputStreams().push_back(stream);
        stream = std::make_shared<SquashingBlockInputStream>(stream, 8192, 0, log);
        pipeline->streams.push_back(stream);
    }
    std::vector<NameAndTypePair> source_columns;
    Block block = pipeline->firstStream()->getHeader();
    for (const auto & col : block.getColumnsWithTypeAndName())
    {
        source_columns.emplace_back(NameAndTypePair(col.name, col.type));
    }
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
    recordProfileStreams(dagContext(), *pipeline, query_block.source_name, query_block.id);
}
} // namespace DB
