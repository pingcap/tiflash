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

#include <Common/Logger.h>
#include <DataStreams/ExchangeSenderBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/ExchangeSenderInterpreterHelper.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Mpp/newMPPExchangeWriter.h>
#include <Flash/Planner/plans/PhysicalExchangeSender.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalExchangeSender::build(
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::ExchangeSender & exchange_sender,
    const FineGrainedShuffle & fine_grained_shuffle,
    const PhysicalPlanNodePtr & child)
{
    assert(child);

    std::vector<Int64> partition_col_ids = ExchangeSenderInterpreterHelper::genPartitionColIds(exchange_sender);
    TiDB::TiDBCollators partition_col_collators = ExchangeSenderInterpreterHelper::genPartitionColCollators(exchange_sender);

    auto physical_exchange_sender = std::make_shared<PhysicalExchangeSender>(
        executor_id,
        child->getSchema(),
        log->identifier(),
        child,
        partition_col_ids,
        partition_col_collators,
        exchange_sender.tp(),
        fine_grained_shuffle);
    // executeUnion will be call after sender.transform, so don't need to restore concurrency.
    physical_exchange_sender->disableRestoreConcurrency();
    return physical_exchange_sender;
}

void PhysicalExchangeSender::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->transform(pipeline, context, max_streams);

    auto & dag_context = *context.getDAGContext();

    RUNTIME_ASSERT(dag_context.isMPPTask() && dag_context.tunnel_set != nullptr, log, "exchange_sender only run in MPP");

    String extra_info;
    if (fine_grained_shuffle.enable())
    {
        extra_info = String(enableFineGrainedShuffleExtraInfo);
        RUNTIME_CHECK(exchange_type == tipb::ExchangeType::Hash, ExchangeType_Name(exchange_type));
        RUNTIME_CHECK(fine_grained_shuffle.stream_count <= 1024, fine_grained_shuffle.stream_count);
    }
    else
    {
        restoreConcurrency(pipeline, dag_context.final_concurrency, log);
    }
    pipeline.transform([&](auto & stream) {
        // construct writer
        std::unique_ptr<DAGResponseWriter> response_writer = newMPPExchangeWriter(
            dag_context.tunnel_set,
            partition_col_ids,
            partition_col_collators,
            exchange_type,
            context.getSettingsRef().dag_records_per_chunk,
            context.getSettingsRef().batch_send_min_limit,
            dag_context,
            fine_grained_shuffle.enable(),
            fine_grained_shuffle.stream_count,
            fine_grained_shuffle.batch_size);
        stream = std::make_shared<ExchangeSenderBlockInputStream>(stream, std::move(response_writer), log->identifier());
        stream->setExtraInfo(extra_info);
    });
}

void PhysicalExchangeSender::finalize(const Names & parent_require)
{
    child->finalize(parent_require);
}

const Block & PhysicalExchangeSender::getSampleBlock() const
{
    return child->getSampleBlock();
}
} // namespace DB
