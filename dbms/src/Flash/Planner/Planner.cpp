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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/Planner/Planner.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>

namespace DB
{
Planner::Planner(Context & context_)
    : context(context_)
    , max_streams(context.getMaxStreams())
    , log(Logger::get(dagContext().log ? dagContext().log->identifier() : ""))
{}

void recursiveSetBlockInputStreamParent(BlockInputStreamPtr self, const IBlockInputStream * parent)
{
    if (self->getParent() != nullptr)
        return;

    for (auto & child : self->getChildren())
    {
        recursiveSetBlockInputStreamParent(child, self.get());
    }
    self->setParent(parent);
}

BlockInputStreamPtr Planner::execute()
{
    DAGPipeline pipeline;
    executeImpl(pipeline);
    executeCreatingSets(pipeline, context, max_streams, log);
    pipeline.transform([](auto & stream) { recursiveSetBlockInputStreamParent(stream, nullptr); });
    return pipeline.firstStream();
}

DAGContext & Planner::dagContext() const
{
    return *context.getDAGContext();
}

void Planner::executeImpl(DAGPipeline & pipeline)
{
    PhysicalPlan physical_plan{context, log->identifier()};

    physical_plan.build(dagContext().dag_request());
    physical_plan.outputAndOptimize();

    physical_plan.buildBlockInputStream(pipeline, context, max_streams);
}
} // namespace DB
