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

#include <Common/Logger.h>
#include <DataStreams/ExchangeSenderBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/ExchangeSenderInterpreterHelper.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Planner/plans/PhysicalExchangeSender.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanPtr PhysicalExchangeSender::build(
    const String & executor_id,
    const String & req_id,
    const tipb::ExchangeSender & exchange_sender,
    PhysicalPlanPtr child)
{
    assert(child);

    std::vector<Int64> partition_col_ids = ExchangeSenderInterpreterHelper::genPartitionColIds(exchange_sender);
    TiDB::TiDBCollators partition_col_collators = ExchangeSenderInterpreterHelper::genPartitionColCollators(exchange_sender);

    auto physical_exchange_sender = std::make_shared<PhysicalExchangeSender>(
        executor_id,
        child->getSchema(),
        req_id,
        partition_col_ids,
        partition_col_collators,
        exchange_sender.tp());
    physical_exchange_sender->appendChild(child);
    return physical_exchange_sender;
}

void PhysicalExchangeSender::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->transform(pipeline, context, max_streams);

    auto & dag_context = *context.getDAGContext();
    restoreConcurrency(pipeline, dag_context.final_concurrency, log);

    RUNTIME_ASSERT(dag_context.isMPPTask() && dag_context.tunnel_set != nullptr, log, "exchange_sender only run in MPP");

    int stream_id = 0;
    pipeline.transform([&](auto & stream) {
        // construct writer
        std::unique_ptr<DAGResponseWriter> response_writer = std::make_unique<StreamingDAGResponseWriter<MPPTunnelSetPtr>>(
            dag_context.tunnel_set,
            partition_col_ids,
            partition_col_collators,
            exchange_type,
            context.getSettingsRef().dag_records_per_chunk,
            context.getSettingsRef().batch_send_min_limit,
            stream_id++ == 0, /// only one stream needs to sending execution summaries for the last response
            dag_context);
        stream = std::make_shared<ExchangeSenderBlockInputStream>(stream, std::move(response_writer), log->identifier());
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
