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
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalPartialTopN.h>
#include <Interpreters/Context.h>
#include <Transforms/ExpressionTransform.h>
#include <Transforms/SortingSink.h>
#include <Transforms/TransformsPipeline.h>

namespace DB
{
void PhysicalPartialTopN::transformImpl(DAGPipeline &, Context &, size_t)
{
    throw Exception("Unsupport");
}

void PhysicalPartialTopN::finalize(const Names & parent_require)
{
    Names required_output = parent_require;
    required_output.reserve(required_output.size() + order_descr.size());
    for (const auto & desc : order_descr)
        required_output.emplace_back(desc.column_name);
    before_sort_actions->finalize(required_output);

    child->finalize(before_sort_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(before_sort_actions, child->getSampleBlock().columns());

    FinalizeHelper::checkSampleBlockContainsParentRequire(getSampleBlock(), parent_require);
}

const Block & PhysicalPartialTopN::getSampleBlock() const
{
    return before_sort_actions->getSampleBlock();
}

void PhysicalPartialTopN::transform(TransformsPipeline & pipeline, Context & context)
{
    child->transform(pipeline, context);

    pipeline.transform([&](auto & transforms) {
        transforms->append(std::make_shared<ExpressionTransform>(before_sort_actions));
        transforms->setSink(std::make_shared<SortingSink>(order_descr, limit, sort_breaker));
    });
    sort_breaker->initForRead(pipeline.getHeader());
}
} // namespace DB
