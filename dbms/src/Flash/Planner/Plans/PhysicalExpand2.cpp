// Copyright 2023 PingCAP, Ltd.
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

#include <Common/FailPoint.h>
#include <Common/TiFlashException.h>
#include <DataStreams/ExpandBlockInputStream.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalExpand2.h>
#include <Interpreters/Context.h>
#include <Operators/Expand2TransformOp.h>
#include <Operators/ExpressionTransformOp.h>
#include <fmt/format.h>

namespace DB
{

PhysicalPlanNodePtr PhysicalExpand2::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Expand2 & expand,
    const PhysicalPlanNodePtr & child)
{
    assert(child);

    if (unlikely(expand.proj_exprs().empty()))
    {
        //should not reach here
        throw TiFlashException("Expand executor without projections indicated by grouping sets", Errors::Planner::BadRequest);
    }

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};

    auto input_col_size = child->getSchema().size();
    NamesAndTypes schema;
    NamesWithAliasesVec project_cols_vec;
    ExpressionActionsPtrVec expression_actions_ptr_vec;
    for (auto i = 0; i < expand.proj_exprs().size(); i++)
    {
        // For every level, it's an individual actions.
        ExpressionActionsPtr one_level_expand_actions = PhysicalPlanHelper::newActions(child->getSampleBlock());
        const auto & level_exprs = expand.proj_exprs().Get(i);
        NamesWithAliases project_cols;
        for (auto j = 0; j < level_exprs.exprs().size(); j++)
        {
            auto expr = level_exprs.exprs().Get(j);
            analyzer.addNullableActionForColumnRef(expr, one_level_expand_actions);

            auto expr_name = analyzer.getActions(expr, one_level_expand_actions);
            const auto & col = one_level_expand_actions->getSampleBlock().getByName(expr_name);

            // link the current projected block column name with unified output column name.
            auto output_name = static_cast<size_t>(j) < input_col_size ? child->getSchema()[j].name : expand.generated_output_names().Get(j - input_col_size);
            project_cols.emplace_back(col.name, output_name);
            // for N level projection, collecting the first level's projected col's type is enough.
            if (i == 0)
                schema.emplace_back(output_name, col.type);
        }
        project_cols_vec.emplace_back(project_cols);
        expression_actions_ptr_vec.emplace_back(one_level_expand_actions);
    }
    auto expand2 = std::make_shared<Expand2>(expression_actions_ptr_vec, project_cols_vec);
    auto physical_expand = std::make_shared<PhysicalExpand2>(
        executor_id,
        schema,
        child->getFineGrainedShuffle(),
        log->identifier(),
        child,
        expand2);

    return physical_expand;
}

// Block input stream transform.
void PhysicalExpand2::expandTransform(DAGPipeline & child_pipeline)
{
    String expand_extra_info = fmt::format("expand2, expand_executor_id = {}: leveled projections: {}", execId(), shared_expand->getLevelProjectionDes());
    child_pipeline.transform([&](auto & stream) {
        stream = std::make_shared<ExpandBlockInputStream>(stream, shared_expand, schema, log->identifier());
        stream->setExtraInfo(expand_extra_info);
    });
}

// Pipeline execution transform.
void PhysicalExpand2::buildPipelineExecGroup(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    Context & /*context*/,
    size_t /*concurrency*/)
{
    group_builder.transform([&](auto & builder) {
        builder.appendTransformOp(std::make_unique<Expand2TransformOp>(exec_status, log->identifier(), sample_block, shared_expand));
    });
}

void PhysicalExpand2::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->buildBlockInputStream(pipeline, context, max_streams);
    expandTransform(pipeline);
}

void PhysicalExpand2::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    // child.finalize() is not necessary here, since what expand required is just what the child output names (input cols).
}

const Block & PhysicalExpand2::getSampleBlock() const
{
    return sample_block;
}
} // namespace DB
