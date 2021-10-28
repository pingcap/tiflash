#include <Common/FailPoint.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/ExchangeSender.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGStringConverter.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Interpreters/Aggregator.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/TMTContext.h>
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

InterpreterDAG::InterpreterDAG(Context & context_, const DAGQuerySource & dag_, const LogWithPrefixPtr & log_)
    : context(context_)
    , dag(dag_)
    , keep_session_timezone_info(
          dag.getEncodeType() == tipb::EncodeType::TypeChunk || dag.getEncodeType() == tipb::EncodeType::TypeCHBlock)
    , log(log_)
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
        keep_session_timezone_info,
        dag,
        subqueries_for_sets,
        mpp_exchange_receiver_maps,
        log);
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
            std::make_shared<GRPCReceiverContext>(context.getTMTContext().getKVCluster()),
            dag_query_block.source->exchange_receiver(),
            dag.getDAGContext().getMPPTaskMeta(),
            max_streams,
            log);
    }
}

BlockIO InterpreterDAG::execute()
{
    if (dag.getDAGContext().isMPPTask())
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

    /// only run in MPP
    if (context.getDAGContext()->tunnel_set != nullptr)
    {
        /// add exchange sender on the top of operators
        const auto & exchange_sender = dag.getDAGRequest().root_executor().exchange_sender();
        /// get partition column ids
        const auto & part_keys = exchange_sender.partition_keys();
        std::vector<Int64> partition_col_id;
        TiDB::TiDBCollators collators;
        /// in case TiDB is an old version, it has no collation info
        bool has_collator_info = exchange_sender.types_size() != 0;
        if (has_collator_info && part_keys.size() != exchange_sender.types_size())
        {
            throw TiFlashException(std::string(__PRETTY_FUNCTION__)
                                       + ": Invalid plan, in ExchangeSender, the length of partition_keys and types is not the same when TiDB new collation is "
                                         "enabled",
                                   Errors::Coprocessor::BadRequest);
        }
        for (int i = 0; i < part_keys.size(); i++)
        {
            const auto & expr = part_keys[i];
            assert(isColumnExpr(expr));
            auto column_index = decodeDAGInt64(expr.val());
            partition_col_id.emplace_back(column_index);
            if (has_collator_info && getDataTypeByFieldType(expr.field_type())->isString())
            {
                collators.emplace_back(getCollatorFromFieldType(exchange_sender.types(i)));
            }
            else
            {
                collators.emplace_back(nullptr);
            }
        }
        restoreConcurrency(pipeline, dag.getDAGContext().final_concurrency, log);
        pipeline.transform([&](auto & stream) {
            // construct writer
            std::unique_ptr<DAGResponseWriter> response_writer = std::make_unique<StreamingDAGResponseWriter<MPPTunnelSetPtr>>(
                context.getDAGContext()->tunnel_set,
                partition_col_id,
                collators,
                exchange_sender.tp(),
                context.getSettings().dag_records_per_chunk,
                context.getSettings().batch_send_min_limit,
                dag.getEncodeType(),
                dag.getResultFieldTypes(),
                dag.getDAGContext(),
                log);
            stream = std::make_shared<ExchangeSender>(stream, std::move(response_writer), log);
        });
    }

    /// add union to run in parallel if needed
    DAGQueryBlockInterpreter::executeUnion(pipeline, max_streams, log);
    if (!subqueries_for_sets.empty())
    {
        const Settings & settings = context.getSettingsRef();
        pipeline.firstStream() = std::make_shared<CreatingSetsBlockInputStream>(
            pipeline.firstStream(),
            std::move(subqueries_for_sets),
            SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode),
            dag.getDAGContext().getMPPTaskId(),
            log);
    }

    BlockIO res;
    res.in = pipeline.firstStream();
    return res;
}
} // namespace DB
