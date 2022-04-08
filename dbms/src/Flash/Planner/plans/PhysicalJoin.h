#pragma once

#include <Flash/Planner/plans/PhysicalBinary.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Join.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalJoin : public PhysicalBinary
{
public:
    static PhysicalPlanPtr build(
        const Context & context,
        const String & executor_id,
        const tipb::Join & join,
        PhysicalPlanPtr left,
        PhysicalPlanPtr right);

    PhysicalJoin(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const JoinPtr & join_ptr_,
        const NamesAndTypesList & columns_added_by_join_,
        const ExpressionActionsPtr & prepare_build_actions_,
        const ExpressionActionsPtr & prepare_probe_actions_);

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

    const PhysicalPlanPtr & build() const { return left; }
    const PhysicalPlanPtr & probe() const { return right; }

    JoinPtr join_ptr;
    NamesAndTypesList columns_added_by_join;
    ExpressionActionsPtr prepare_build_actions;
    ExpressionActionsPtr prepare_probe_actions;

    Block sample_block;
};
} // namespace DB