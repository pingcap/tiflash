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
        // should not reach here
        throw TiFlashException(
            "Expand executor without projections indicated by grouping sets",
            Errors::Planner::BadRequest);
    }

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};

    auto input_col_size = child->getSchema().size();
    NamesAndTypes schema;
    NamesAndTypes gen_schema;
    NamesWithAliasesVec project_cols_vec;
    ExpressionActionsPtrVec expression_actions_ptr_vec;

    // By now, tidb projection has only two situation:
    //     1: agg/join + shuffler + projection
    //     2: tidb_reader + shuffler + projection
    // in the exchanger sender of the shuffler, we can always add the alias change via 'buildFinalProjection'.
    // rootProjection services as substituting the user-level alias name.
    // nonRootProjection services as adding prefix alias name for distinguishing between fragments.
    //
    // but Expand + Projection is an exception.
    // let's say we have a projection with two col-ref: [col2, col1]. while the source columns are [col1, col2].
    // the projection actions will be nil, since all the column can be found in current analyzer, while the column
    // position is switched in the nonRootProjection of exchangeSender, accompanied by adding prefix alias name.
    //
    // while for case in {Expand + Projection} here, there is no shuffler between Expand OP and Projection here.
    // which will causing the child schema columns that Expand can see is different from child's sample block it saw.
    // so adding column positions switching and fetching logic here in expand-pre-actions.
    ExpressionActionsPtr header_actions = PhysicalPlanHelper::newActions(child->getSampleBlock());
    auto final_project_aliases = analyzer.genNonRootFinalProjectAliases("");
    header_actions->add(ExpressionAction::project(final_project_aliases));

    // pre-detect the nullability change action in the first projection level and generate the pre-actions.
    // for rollup, it's level projections may show like below:
    // Says child schema is: [unrelated grouping cols projection..., col1, col2, col3]
    // Rollup(col1, col2, col3): schema should be same for every level projection: [unrelated grouping cols projection..., col1, col2, col3, gid]
    //
    // Algorithm:                                1: ----+------+------+----> 2:
    //     [unrelated grouping cols projection...] null,| null,| null,| gid-4
    //     [unrelated grouping cols projection...] col1,v null,| null,| gid-3
    //     [unrelated grouping cols projection...] col1,  col2,v null,| gid-2
    //     [unrelated grouping cols projection...] col1,  col2,  col3,v gid-1
    //
    // we should detect every grouping set column-ref (means col1, col2, col3 if any) which may has nullability change after expand OP.
    // since level projection order is not always fixed, leading the last level projection always has the most grouping column ref as above,
    // 1: we will traverse horizontally to locate a column which is with not-null from child schema while Expand specified them as nullable in field type.
    // 2: then traverse down vertically to find a specific column-ref [should skip literal null vertically], then generating CONVERT_TO_NULLABLE action for it.
    assert(!expand.proj_exprs().empty());
    auto first_proj_level = expand.proj_exprs().Get(0);
    auto horizontal_size = first_proj_level.exprs().size();
    auto vertical_size = expand.proj_exprs().size();
    for (auto i = 0; i < horizontal_size; ++i)
    {
        // horizontally search nullability change column.
        auto expr = first_proj_level.exprs().Get(i);
        // record the ref-col nullable attributes for header.
        if (static_cast<size_t>(i) < input_col_size
            && (expr.has_field_type() && (expr.field_type().flag() & TiDB::ColumnFlagNotNull) == 0)
            && !child->getSchema()[i].type->isNullable())
        {
            // vertically search column-ref rather than literal null.
            for (auto j = 0; j < vertical_size; ++j)
            {
                // relocate expr.
                expr = expand.proj_exprs().Get(j).exprs().Get(i);
                if (isColumnExpr(expr))
                {
                    auto col = getColumnNameAndTypeForColumnExpr(expr, analyzer.getCurrentInputColumns());
                    header_actions->add(ExpressionAction::convertToNullable(col.name));
                    break;
                }
            }
        }
    }
    NamesAndTypes new_source_cols;
    for (const auto & origin_col : header_actions->getSampleBlock().getNamesAndTypesList())
        new_source_cols.emplace_back(origin_col.name, origin_col.type);
    analyzer.reset(new_source_cols);

    for (auto i = 0; i < expand.proj_exprs().size(); i++)
    {
        // For every level, it's an individual actions.
        ExpressionActionsPtr one_level_expand_actions
            = PhysicalPlanHelper::newActions(header_actions->getSampleBlock());
        const auto & level_exprs = expand.proj_exprs().Get(i);
        NamesWithAliases project_cols;
        for (auto j = 0; j < level_exprs.exprs().size(); j++)
        {
            auto expr = level_exprs.exprs().Get(j);

            auto expr_name = analyzer.getActions(expr, one_level_expand_actions);
            const auto & col = one_level_expand_actions->getSampleBlock().getByName(expr_name);

            // link the current projected block column name with unified output column name.
            auto output_name = static_cast<size_t>(j) < input_col_size
                ? analyzer.getCurrentInputColumns()[j].name
                : expand.generated_output_names().Get(j - input_col_size);
            project_cols.emplace_back(col.name, output_name);
            // for N level projection, collecting the first level's projected col's type is enough.
            if (i == 0)
            {
                // record the generated appended schema.
                if (static_cast<size_t>(j) >= input_col_size)
                    gen_schema.emplace_back(output_name, col.type);
                schema.emplace_back(output_name, col.type);
            }
        }
        project_cols_vec.emplace_back(project_cols);
        expression_actions_ptr_vec.emplace_back(one_level_expand_actions);
    }
    // add column that used to for header, append it to the pre-actions as well (convenient for header generation).
    for (const auto & gen_col : gen_schema)
    {
        ColumnWithTypeAndName column;
        column.column = gen_col.type->createColumn();
        column.name = gen_col.name;
        column.type = gen_col.type;
        header_actions->add(ExpressionAction::addColumn(column));
    }
    header_actions->finalize(toNames(schema));

    auto expand2 = std::make_shared<Expand2>(expression_actions_ptr_vec, header_actions, project_cols_vec);
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
    String expand_extra_info = fmt::format(
        "expand2, expand_executor_id = {}: leveled projections: {}",
        execId(),
        shared_expand->getLevelProjectionDes());
    child_pipeline.transform([&](auto & stream) {
        // make the header ahead for every stream.
        auto input_header = stream->getHeader();
        shared_expand->getBeforeExpandActions()->execute(input_header);
        // construct the ExpandBlockInputStream.
        stream = std::make_shared<ExpandBlockInputStream>(stream, shared_expand, input_header, log->identifier());
        stream->setExtraInfo(expand_extra_info);
    });
}

// Pipeline execution transform.
void PhysicalExpand2::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & /*context*/,
    size_t /*concurrency*/)
{
    auto input_header = group_builder.getCurrentHeader();
    // make the header ahead
    shared_expand->getBeforeExpandActions()->execute(input_header);
    group_builder.transform([&](auto & builder) {
        builder.appendTransformOp(
            std::make_unique<Expand2TransformOp>(exec_context, log->identifier(), input_header, shared_expand));
    });
}

void PhysicalExpand2::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->buildBlockInputStream(pipeline, context, max_streams);
    expandTransform(pipeline);
}

void PhysicalExpand2::finalizeImpl(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    child->finalize(shared_expand->getBeforeExpandActions()->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(
        shared_expand->getBeforeExpandActions(),
        child->getSampleBlock().columns());
    FinalizeHelper::checkSampleBlockContainsParentRequire(getSampleBlock(), parent_require);
}

const Block & PhysicalExpand2::getSampleBlock() const
{
    return sample_block;
}
} // namespace DB
