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

#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/Plans/PhysicalCTESink.h>
#include <Interpreters/Context.h>
#include <Operators/CTE.h>
#include <Operators/CTESinkOp.h>

#include <memory>

namespace DB
{
PhysicalPlanNodePtr PhysicalCTESink::build(
    const String & executor_id,
    const LoggerPtr & log,
    const FineGrainedShuffle & fine_grained_shuffle,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);

    auto physical_cte_sink = std::make_shared<PhysicalCTESink>(
        executor_id,
        child->getSchema(),
        fine_grained_shuffle,
        log->identifier(),
        child);
    physical_cte_sink->disableRestoreConcurrency();
    return physical_cte_sink;
}

void PhysicalCTESink::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t)
{
    std::shared_ptr<CTE> cte = context.getDAGContext()->getCTESink();
    RUNTIME_CHECK(cte);

    cte->checkSinkConcurrency(group_builder.concurrency());

    size_t id = 0;
    group_builder.transform([&](auto & builder) {
        builder.setSinkOp(std::make_unique<CTESinkOp>(exec_context, log->identifier(), cte, id));
        id++;
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
