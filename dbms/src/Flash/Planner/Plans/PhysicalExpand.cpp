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

#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <DataStreams/ExpandBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalExpand.h>
#include <Interpreters/Context.h>
#include <fmt/format.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalExpand::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Expand & expand,
    const PhysicalPlanNodePtr & child)
{
    assert(child);

    child->finalize();

    if (unlikely(expand.grouping_sets().empty()))
    {
        //should not reach here
        throw TiFlashException("Expand executor without grouping sets", Errors::Planner::BadRequest);
    }

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr before_expand_actions = PhysicalPlanHelper::newActions(child->getSampleBlock(), context);


    auto shared_expand = analyzer.buildExpandGroupingColumns(expand, before_expand_actions);

    // construct sample block.
    NamesAndTypes expand_output_columns;
    auto child_header = child->getSchema();
    for (const auto & one : child_header)
    {
        expand_output_columns.emplace_back(one.name, shared_expand->isInGroupSetColumn(one.name)? makeNullable(one.type): one.type);
    }
    expand_output_columns.emplace_back(shared_expand->grouping_identifier_column_name, shared_expand->grouping_identifier_column_type);

    auto physical_expand = std::make_shared<PhysicalExpand>(
        executor_id,
        expand_output_columns,
        log->identifier(),
        child,
        shared_expand,
        Block(expand_output_columns));

    return physical_expand;
}


void PhysicalExpand::expandTransform(DAGPipeline & child_pipeline, Context & context)
{
    auto expand_actions = PhysicalPlanHelper::newActions(child_pipeline.firstStream()->getHeader(), context);
    expand_actions->add(ExpressionAction::expandSource(shared_expand));
    String expand_extra_info = fmt::format("expand, expand_executor_id = {}", execId());
    child_pipeline.transform([&](auto &stream) {
        stream = std::make_shared<ExpandBlockInputStream>(stream, expand_actions);
        stream->setExtraInfo(expand_extra_info);
    });
}

void PhysicalExpand::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->transform(pipeline, context, max_streams);
    expandTransform(pipeline, context);
}

void PhysicalExpand::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    Names required_output;
    required_output.reserve( shared_expand->getGroupSetNum());    // grouping set column should be existed in the child output schema.
    auto name_set = std::set<String>();
    shared_expand->getAllGroupSetColumnNames(name_set);
    // append parent_require column it may expect self-filled groupingID.
    for (const auto & one : parent_require)
    {
        if (one != Expand::grouping_identifier_column_name)
        {
            name_set.insert(one);
        }
    }
    for (const auto & grouping_name: name_set) {
        required_output.emplace_back(grouping_name);
    }
    child->finalize(required_output);
}

const Block & PhysicalExpand::getSampleBlock() const
{
    return sample_block;
}
} // namespace DB
