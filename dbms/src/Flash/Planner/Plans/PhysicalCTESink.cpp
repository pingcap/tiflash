// Copyright 2025 PingCAP, Inc.
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

#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/Plans/PhysicalCTESink.h>
#include <Operators/CTE.h>
#include <Operators/CTESinkOp.h>

#include <memory>

namespace DB
{
PhysicalPlanNodePtr PhysicalCTESink::build(
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::ExchangeSender & exchange_sender,
    const FineGrainedShuffle & fine_grained_shuffle,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);

    // std::vector<Int64> partition_col_ids = ExchangeSenderInterpreterHelper::genPartitionColIds(exchange_sender);
    // TiDB::TiDBCollators partition_col_collators
    //     = ExchangeSenderInterpreterHelper::genPartitionColCollators(exchange_sender);

    // auto physical_exchange_sender = std::make_shared<PhysicalExchangeSender>(
    //     executor_id,
    //     child->getSchema(),
    //     fine_grained_shuffle,
    //     log->identifier(),
    //     child,
    //     partition_col_ids,
    //     partition_col_collators,
    //     exchange_sender.tp(),
    //     exchange_sender.compression());
    // // executeUnion will be call after sender.transform, so don't need to restore concurrency.
    // physical_exchange_sender->disableRestoreConcurrency();
    // return physical_exchange_sender;
}

void PhysicalCTESink::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & /*context*/,
    size_t /*concurrency*/)
{
    size_t partition_id = 0;
    group_builder.transform([&](auto & builder) {
        std::shared_ptr<CTE> cte; // TODO get it from CTEManager
        builder.setSinkOp(std::make_unique<CTESinkOp>(exec_context, log->identifier(), cte));
        ++partition_id;
    });
}

void PhysicalCTESink::finalizeImpl(const Names & parent_require)
{
    child->finalize(parent_require);
}

const Block & PhysicalCTESink::getSampleBlock() const
{
    return child->getSampleBlock();
}

} // namespace DB
