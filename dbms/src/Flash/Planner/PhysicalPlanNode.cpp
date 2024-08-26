// Copyright 2023 PingCAP, Inc.
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

#include <Common/FmtUtils.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/PipelineBuilder.h>
#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/PhysicalPlanNode.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanNode::PhysicalPlanNode(
    const String & executor_id_,
    const PlanType & type_,
    const NamesAndTypes & schema_,
    const FineGrainedShuffle & fine_grained_shuffle_,
    const String & req_id_)
    : executor_id(executor_id_)
    , req_id(req_id_)
    , type(type_)
    , schema(schema_)
    , fine_grained_shuffle(fine_grained_shuffle_)
    , log(Logger::get(fmt::format("{}_{}_{}", req_id, type_.toString(), executor_id_)))
{}

String PhysicalPlanNode::toString()
{
    auto schema_to_string = [&]() {
        FmtBuffer buffer;
        buffer.joinStr(
            schema.cbegin(),
            schema.cend(),
            [](const auto & item, FmtBuffer & buf) { buf.fmtAppend("<{}, {}>", item.name, item.type->getName()); },
            ", ");
        return buffer.toString();
    };
    return fmt::format(
        "<{}, {}> | is_tidb_operator: {}, schema: {}",
        type.toString(),
        executor_id,
        is_tidb_operator,
        schema_to_string());
}

String PhysicalPlanNode::toSimpleString()
{
    return fmt::format("{}|{}", type.toString(), isTiDBOperator() ? executor_id : "NonTiDBOperator");
}

void PhysicalPlanNode::finalize(const Names & parent_require)
{
    if unlikely (finalized)
    {
        LOG_WARNING(log, "Should not reach here, {}-{} already finalized", type.toString(), executor_id);
        return;
    }
    auto block_to_schema_string = [&](const Block & block) {
        FmtBuffer buffer;
        buffer.joinStr(
            block.cbegin(),
            block.cend(),
            [](const auto & item, FmtBuffer & buf) { buf.fmtAppend("<{}, {}>", item.name, item.type->getName()); },
            ", ");
        return buffer.toString();
    };
    auto block_before_finalize = getSampleBlock();
    finalizeImpl(parent_require);
    finalized = true;
    auto block_after_finalize = getSampleBlock();
    if (block_before_finalize.columns() != block_after_finalize.columns())
    {
        LOG_DEBUG(
            log,
            "Finalize pruned some columns: before finalize: {}, after finalize: {}",
            block_to_schema_string(block_before_finalize),
            block_to_schema_string(block_after_finalize));
    }
}

void PhysicalPlanNode::recordProfileStreams(DAGPipeline & pipeline, const Context & context)
{
    auto & profile_streams_map = context.getDAGContext()->getProfileStreamsMap();
    /// The profile stream of some operators has been recorded.
    /// For example, `DAGStorageInterpreter` records the profile streams of PhysicalTableScan.
    if (profile_streams_map.find(executor_id) == profile_streams_map.end())
    {
        auto & profile_streams = profile_streams_map[executor_id];
        pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
    }
}

void PhysicalPlanNode::buildBlockInputStream(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    buildBlockInputStreamImpl(pipeline, context, max_streams);
    if (is_tidb_operator)
        recordProfileStreams(pipeline, context);
    if (is_restore_concurrency)
    {
        context.getDAGContext()->updateFinalConcurrency(pipeline.streams.size(), max_streams);
        restoreConcurrency(
            pipeline,
            context.getDAGContext()->final_concurrency,
            context.getSettingsRef().max_buffered_bytes_in_executor,
            log);
    }
}

void PhysicalPlanNode::buildPipelineExecGroup(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t concurrency)
{
    buildPipelineExecGroupImpl(exec_context, group_builder, context, concurrency);
    if (is_tidb_operator)
        context.getDAGContext()->addOperatorProfileInfos(executor_id, group_builder.getCurProfileInfos());
}

void PhysicalPlanNode::buildPipeline(
    PipelineBuilder & builder,
    Context & context,
    PipelineExecutorContext & exec_context)
{
    RUNTIME_CHECK(childrenSize() <= 1);
    if (childrenSize() == 1)
        children(0)->buildPipeline(builder, context, exec_context);
    builder.addPlanNode(shared_from_this());
}

EventPtr PhysicalPlanNode::sinkComplete(PipelineExecutorContext & exec_context)
{
    if (getFineGrainedShuffle().enabled())
        return nullptr;
    return doSinkComplete(exec_context);
}

EventPtr PhysicalPlanNode::doSinkComplete(PipelineExecutorContext & /*exec_status*/)
{
    return nullptr;
}
} // namespace DB
