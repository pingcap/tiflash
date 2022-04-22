#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/DAGStringConverter.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Interpreters/Aggregator.h>
#include <Storages/Transaction/TMTContext.h>
#include <pingcap/coprocessor/Client.h>

namespace DB
{
InterpreterDAG::InterpreterDAG(Context & context_, const DAGQuerySource & dag_)
    : context(context_)
    , dag(dag_)
    , mpp_exchange_receiver_maps(dagContext().getMPPExchangeReceiverMapRef())
{
    const Settings & settings = context.getSettingsRef();
    if (dagContext().isBatchCop() || dagContext().isMPPTask())
        max_streams = settings.max_threads;
    else
        max_streams = 1;
    if (max_streams > 1)
    {
        max_streams *= settings.max_streams_to_max_threads_ratio;
    }
}

/** executeQueryBlock recursively converts all the children of the DAGQueryBlock and itself (Coprocessor DAG request)
  * into an array of IBlockInputStream (element of physical executing plan of TiFlash)
  */
BlockInputStreams InterpreterDAG::executeQueryBlock(DAGQueryBlock & query_block, std::vector<SubqueriesForSets> & subqueries_for_sets)
{
    std::vector<BlockInputStreams> input_streams_vec;
    for (auto & child : query_block.children)
    {
        BlockInputStreams child_streams = executeQueryBlock(*child, subqueries_for_sets);
        input_streams_vec.push_back(child_streams);
    }
    DAGQueryBlockInterpreter query_block_interpreter(
        context,
        input_streams_vec,
        query_block,
        max_streams,
        dagContext().keep_session_timezone_info || !query_block.isRootQueryBlock(),
        subqueries_for_sets,
        mpp_exchange_receiver_maps);
    return query_block_interpreter.execute();
}

void InterpreterDAG::initMPPExchangeReceiver(const DAGQueryBlock & dag_query_block)
{
    for (const auto & child_qb : dag_query_block.children)
    {
        initMPPExchangeReceiver(*child_qb);
    }
    if (dag_query_block.source->tp() == tipb::ExecType::TypeExchangeReceiver)
    {
        mpp_exchange_receiver_maps[dag_query_block.source_name] = std::make_shared<ExchangeReceiver>(
            std::make_shared<GRPCReceiverContext>(
                context.getTMTContext().getKVCluster(),
                context.getTMTContext().getMPPTaskManager(),
                context.getSettings().enable_local_tunnel),
            dag_query_block.source->exchange_receiver(),
            dagContext().getMPPTaskMeta(),
            max_streams,
            dagContext().log);
    }
}

BlockIO InterpreterDAG::execute()
{
    if (dagContext().isMPPTask())
        /// Due to learner read, DAGQueryBlockInterpreter may take a long time to build
        /// the query plan, so we init mpp exchange receiver before executeQueryBlock
        initMPPExchangeReceiver(*dag.getRootQueryBlock());
    /// region_info should base on the source executor, however
    /// tidb does not support multi-table dag request yet, so
    /// it is ok to use the same region_info for the whole dag request
    std::vector<SubqueriesForSets> subqueries_for_sets;
    BlockInputStreams streams = executeQueryBlock(*dag.getRootQueryBlock(), subqueries_for_sets);
    DAGPipeline pipeline;
    pipeline.streams = streams;

    /// add union to run in parallel if needed
    if (context.getDAGContext()->isMPPTask())
        /// MPPTask do not need the returned blocks.
        executeUnion(pipeline, max_streams, dagContext().log, /*ignore_block=*/true);
    else
        executeUnion(pipeline, max_streams, dagContext().log);
    if (!subqueries_for_sets.empty())
    {
        const Settings & settings = context.getSettingsRef();
        pipeline.firstStream() = std::make_shared<CreatingSetsBlockInputStream>(
            pipeline.firstStream(),
            std::move(subqueries_for_sets),
            SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode),
            dagContext().getMPPTaskId(),
            dagContext().log);
    }

    BlockIO res;
    res.in = pipeline.firstStream();
    return res;
}
} // namespace DB
