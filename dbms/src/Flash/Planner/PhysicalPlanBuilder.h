#pragma once

#include <Flash/Planner/PhysicalPlan.h>
#include <tipb/executor.pb.h>
#include <tipb/expression.pb.h>

namespace DB
{
class PhysicalPlanBuilder
{
public:
    explicit PhysicalPlanBuilder(Context & context_)
        : context(context_)
    {}

    void build(const String & executor_id, const tipb::Executor * executor);

    void buildSource(
        const String & executor_id,
        const NamesAndTypes & source_schema,
        const Block & source_sample_block);

    void buildNonRootFinalProjection(const String & column_prefix);
    void buildRootFinalProjection(
        const std::vector<tipb::FieldType> & require_schema,
        const std::vector<Int32> & output_offsets,
        const String & column_prefix,
        bool keep_session_timezone_info);

    PhysicalPlanPtr getResult() const
    {
        assert(cur_plans.size() == 1);
        return cur_plans.back();
    }

private:
    std::vector<PhysicalPlanPtr> cur_plans;

    Context & context;
};
} // namespace DB