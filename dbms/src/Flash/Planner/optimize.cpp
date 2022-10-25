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

#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Planner/PhysicalPlanVisitor.h>
#include <Flash/Planner/optimize.h>
#include <Flash/Planner/plans/PhysicalAggregation.h>
#include <Flash/Planner/plans/PhysicalTableScan.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB
{
class Rule
{
public:
    virtual PhysicalPlanNodePtr apply(const Context & context, PhysicalPlanNodePtr plan, const LoggerPtr & log) = 0;

    virtual ~Rule() = default;
};
using RulePtr = std::shared_ptr<Rule>;

class FinalizeRule : public Rule
{
public:
    PhysicalPlanNodePtr apply(const Context &, PhysicalPlanNodePtr plan, const LoggerPtr &) override
    {
        plan->finalize();
        return plan;
    }

    static RulePtr create() { return std::make_shared<FinalizeRule>(); }
};

/// This rule applies to `select count(const non-null) from t ...` queries
/// TiDB planner will rewrite `count(*)` with `count(1)`, so `count(*)` is also covered
/// rewrite the plan to read the delmark_column instead of handle_column.
class RewriteCountStarRule : public Rule
{
public:
    PhysicalPlanNodePtr apply(const Context &, PhysicalPlanNodePtr plan, const LoggerPtr & log) override
    {
        // We will rewrite `count(const non-null)` to `count()` by https://github.com/pingcap/tiflash/pull/5898
        // so make sure all the aggregators are `count()`
        auto aggr_nodes = DB::PhysicalPlanVisitor::getAggarationNodes(plan);
        for (auto & aggr_node : aggr_nodes)
        {
            auto aggr = std::dynamic_pointer_cast<PhysicalAggregation>(aggr_node);
            auto & aggr_descs = aggr->getAggregateDescriptions();
            if (aggr_descs.size() != 1)
                return plan;
            if (aggr_descs[0].function->getName() != "count" && aggr_descs[0].argument_names.empty())
                return plan;
        }
        // if all the aggregators are `count()`, then the columns to read must be the handle_column
        // so we do not need to do any check here
        // just rewrite the table scan to read the delmark_column instead of handle_column
        auto table_scan_nodes = DB::PhysicalPlanVisitor::getTableScanNodes(plan);
        for (auto & table_scan_node : table_scan_nodes)
        {
            auto table_scan = std::dynamic_pointer_cast<PhysicalTableScan>(table_scan_node);
            table_scan->replaceColumnToRead(EXTRA_HANDLE_COLUMN_NAME, TAG_COLUMN_NAME, std::make_shared<DataTypeUInt8>());
        }
        LOG_INFO(log, "Rewrite count(const non-null) to read the delmark_column instead of handle_column by RewriteCountStarRule");
        return plan;
    }

    static RulePtr create() { return std::make_shared<RewriteCountStarRule>(); }
};

PhysicalPlanNodePtr optimize(const Context & context, PhysicalPlanNodePtr plan, const LoggerPtr & log)
{
    assert(plan);
    static std::vector<RulePtr> rules{FinalizeRule::create(), RewriteCountStarRule::create()};
    for (const auto & rule : rules)
    {
        plan = rule->apply(context, plan, log);
        assert(plan);
    }
    return plan;
}
} // namespace DB
