#pragma once

#include <Flash/Planner/PhysicalPlan.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/Join.h>

namespace DB
{
class PhysicalJoin : public PhysicalPlan
{
public:
    PhysicalJoin(
        const String & executor_id_,
        const NamesAndTypes & schema_)
        : PhysicalPlan(executor_id_, PlanType::Aggregation, schema_)
    {}

    PhysicalPlanPtr children(size_t i) const override
    {
        assert(i <= 1);
        if (0 == i)
        {
            assert(probe_child);
            return probe_child;
        }
        else
        {
            assert(build_child);
            return build_child;
        }
    }

    void setChild(size_t i, const PhysicalPlanPtr & new_child) override
    {
        assert(i <= 1);
        if (0 == i)
            probe_child = new_child;
        else
            build_child = new_child;
    }

    void appendChild(const PhysicalPlanPtr & new_child) override
    {
        assert(new_child);
        if (!probe_child)
        {
            assert(!build_child);
            probe_child = new_child;
        }
        else
        {
            assert(!build_child);
            build_child = new_child;
        }
    }

    size_t childrenSize() const override { return 2; };

    void transform(DAGPipeline & pipeline, const Context & context, size_t max_streams) override;

    bool finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    PhysicalPlanPtr probe_child;
    PhysicalPlanPtr build_child;

    /// prepare join keys and filter_column_name
    ExpressionActionsPtr probe_prepare_expr;
    ExpressionActionsPtr build_prepare_expr;

    Names probe_key_names;
    String probe_filter_column_name;
    Names build_key_names;
    String build_filter_column_name;

    TiDB::TiDBCollators join_key_collators;

    ExpressionActionsPtr other_condition_expr;
    String other_filter_column_name;
    String other_eq_filter_from_in_column_name;

    String match_helper_name;

    ASTTableJoin::Kind kind;
    ASTTableJoin::Strictness strictness;

    NamesAndTypesList columns_added_by_join;

    void recordJoinExecuteInfo(const JoinPtr & join_ptr, DAGContext & dag_context);
};
} // namespace DB