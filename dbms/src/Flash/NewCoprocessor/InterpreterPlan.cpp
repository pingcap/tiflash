#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/NewCoprocessor/InterpreterPlan.h>
#include <Flash/NewCoprocessor/PlanInterpreter.h>
#include <Flash/Plan/Plans.h>
#include <Flash/Plan/foreach.h>
#include <Interpreters/Aggregator.h>
#include <Storages/Transaction/TMTContext.h>
#include <pingcap/coprocessor/Client.h>

namespace DB
{
InterpreterPlan::InterpreterPlan(Context & context_, const PlanQuerySource & plan_query_source_)
    : context(context_)
    , plan_query_source(plan_query_source_)
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
BlockInputStreams InterpreterPlan::executePlan(std::vector<SubqueriesForSets> & subqueries_for_sets)
{
    return PlanInterpreter(
               context,
               plan_query_source.getRootPlan(),
               max_streams,
               dagContext().keep_session_timezone_info,
               subqueries_for_sets,
               mpp_exchange_receiver_maps)
        .execute();
}

void InterpreterPlan::initMPPExchangeReceiver()
{
    foreachDown(plan_query_source.getRootPlan(), [&](const PlanPtr & plan) {
        if (plan->tp() == tipb::ExecType::TypeExchangeReceiver)
        {
            plan->toImpl<ExchangeReceiverPlan>([&](const ExchangeReceiverPlan & receiver) {
                mpp_exchange_receiver_maps[receiver.executor_id] = std::make_shared<ExchangeReceiver>(
                    std::make_shared<GRPCReceiverContext>(
                        context.getTMTContext().getKVCluster(),
                        context.getTMTContext().getMPPTaskManager(),
                        context.getSettingsRef().enable_local_tunnel),
                    receiver.impl,
                    dagContext().getMPPTaskMeta(),
                    max_streams,
                    dagContext().log);
            });
        }
    });
}

BlockIO InterpreterPlan::execute()
{
    if (dagContext().isMPPTask())
        /// Due to learner read, DAGQueryBlockInterpreter may take a long time to build
        /// the query plan, so we init mpp exchange receiver before executeQueryBlock
        initMPPExchangeReceiver();
    /// region_info should base on the source executor, however
    /// tidb does not support multi-table dag request yet, so
    /// it is ok to use the same region_info for the whole dag request
    std::vector<SubqueriesForSets> subqueries_for_sets;
    BlockInputStreams streams = executePlan(subqueries_for_sets);
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
