#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGStringConverter.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Flash/Coprocessor/StreamingDAGBlockInputStream.h>
#include <Interpreters/Aggregator.h>
#include <Storages/StorageMergeTree.h>
#include <pingcap/coprocessor/Client.h>


namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
extern const int TOO_MANY_COLUMNS;
extern const int SCHEMA_VERSION_ERROR;
extern const int UNKNOWN_EXCEPTION;
extern const int COP_BAD_DAG_REQUEST;
} // namespace ErrorCodes

InterpreterDAG::InterpreterDAG(Context & context_, const DAGQuerySource & dag_)
    : context(context_),
      dag(dag_),
      keep_session_timezone_info(
          dag.getEncodeType() == tipb::EncodeType::TypeChunk || dag.getEncodeType() == tipb::EncodeType::TypeCHBlock),
      log(&Logger::get("InterpreterDAG"))
{
    const Settings & settings = context.getSettingsRef();
    if (dag.isBatchCop())
        max_streams = settings.max_threads;
    else
        max_streams = 1;
    if (max_streams > 1)
    {
        max_streams *= settings.max_streams_to_max_threads_ratio;
    }
}

BlockInputStreams InterpreterDAG::executeQueryBlock(DAGQueryBlock & query_block, std::vector<SubqueriesForSets> & subqueriesForSets)
{
    if (!query_block.children.empty())
    {
        std::vector<BlockInputStreams> input_streams_vec;
        for (auto & child : query_block.children)
        {
            BlockInputStreams child_streams = executeQueryBlock(*child, subqueriesForSets);
            input_streams_vec.push_back(child_streams);
        }
        DAGQueryBlockInterpreter query_block_interpreter(
            context, input_streams_vec, query_block, keep_session_timezone_info, dag.getDAGRequest(), dag.getAST(), dag, subqueriesForSets);
        return query_block_interpreter.execute();
    }
    else
    {
        DAGQueryBlockInterpreter query_block_interpreter(
            context, {}, query_block, keep_session_timezone_info, dag.getDAGRequest(), dag.getAST(), dag, subqueriesForSets);
        return query_block_interpreter.execute();
    }
}

BlockIO InterpreterDAG::execute()
{
    /// region_info should based on the source executor, however
    /// tidb does not support multi-table dag request yet, so
    /// it is ok to use the same region_info for the whole dag request
    std::vector<SubqueriesForSets> subqueriesForSets;
    BlockInputStreams streams = executeQueryBlock(*dag.getQueryBlock(), subqueriesForSets);

    Pipeline pipeline;
    pipeline.streams = streams;

    if (dag.writer->writer != nullptr)
    {
        bool collect_exec_summary
            = dag.getDAGRequest().has_collect_execution_summaries() && dag.getDAGRequest().collect_execution_summaries();
        for (auto & stream : pipeline.streams)
            stream = std::make_shared<StreamingDAGBlockInputStream>(stream, dag.writer, context.getSettings().dag_records_per_chunk,
                dag.getEncodeType(), dag.getResultFieldTypes(), stream->getHeader(), dag.getDAGContext(), collect_exec_summary,
                dag.getDAGRequest().has_root_executor());
    }
    DAGQueryBlockInterpreter::executeUnion(pipeline, max_streams);
    if (!subqueriesForSets.empty())
    {
        const Settings & settings = context.getSettingsRef();
        pipeline.firstStream() = std::make_shared<CreatingSetsBlockInputStream>(pipeline.firstStream(), std::move(subqueriesForSets),
            SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode));
    }

    BlockIO res;
    res.in = pipeline.firstStream();
    return res;
}
} // namespace DB
