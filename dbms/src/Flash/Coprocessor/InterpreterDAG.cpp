// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Interpreters/Context.h>

namespace DB
{
InterpreterDAG::InterpreterDAG(Context & context_, const DAGQuerySource & dag_)
    : context(context_)
    , dag(dag_)
{
    const Settings & settings = context.getSettingsRef();
    if (dagContext().isBatchCop() || dagContext().isMPPTask())
        max_streams = settings.max_threads;
    else if (dagContext().isTest())
        max_streams = dagContext().initialize_concurrency;
    else
        max_streams = 1;

    if (max_streams > 1)
    {
        max_streams *= settings.max_streams_to_max_threads_ratio;
    }
}

void setRestorePipelineConcurrency(DAGQueryBlock & query_block)
{
    if (query_block.source->tp() == tipb::ExecType::TypeWindow)
    {
        assert(query_block.children.size() == 1);
        query_block.children.back()->can_restore_pipeline_concurrency = false;
    }
}

DAGContext & InterpreterDAG::dagContext() const
{
    return *context.getDAGContext();
}

/** executeQueryBlock recursively converts all the children of the DAGQueryBlock and itself (Coprocessor DAG request)
  * into an array of IBlockInputStream (element of physical executing plan of TiFlash)
  */
BlockInputStreams InterpreterDAG::executeQueryBlock(DAGQueryBlock & query_block)
{
    std::vector<BlockInputStreams> input_streams_vec;
    setRestorePipelineConcurrency(query_block);
    for (auto & child : query_block.children)
    {
        BlockInputStreams child_streams = executeQueryBlock(*child);
        input_streams_vec.push_back(child_streams);
    }
    DAGQueryBlockInterpreter query_block_interpreter(
        context,
        input_streams_vec,
        query_block,
        max_streams);
    return query_block_interpreter.execute();
}

BlockIO InterpreterDAG::execute()
{
    /// Due to learner read, DAGQueryBlockInterpreter may take a long time to build
    /// the query plan, so we init mpp exchange receiver before executeQueryBlock
    dagContext().initExchangeReceiverIfMPP(context, max_streams);

    BlockInputStreams streams = executeQueryBlock(*dag.getRootQueryBlock());
    DAGPipeline pipeline;
    pipeline.streams = streams;
    /// add union to run in parallel if needed
    if (dagContext().isMPPTask())
        /// MPPTask do not need the returned blocks.
        executeUnion(pipeline, max_streams, dagContext().log, /*ignore_block=*/true);
    else
        executeUnion(pipeline, max_streams, dagContext().log);
    if (dagContext().hasSubquery())
    {
        const Settings & settings = context.getSettingsRef();
        pipeline.firstStream() = std::make_shared<CreatingSetsBlockInputStream>(
            pipeline.firstStream(),
            std::move(dagContext().moveSubqueries()),
            SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode),
            dagContext().log->identifier());
    }
    BlockIO res;
    res.in = pipeline.firstStream();
    return res;
}
} // namespace DB
