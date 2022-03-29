#pragma once

#include <Flash/Planner/plans/PhysicalUnary.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalLimit : public PhysicalUnary
{
public:
    static PhysicalPlanPtr build(
        const String & executor_id,
        const tipb::Limit & limit,
        const PhysicalPlanPtr & child);

    PhysicalLimit(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        size_t limit_)
        : PhysicalUnary(executor_id_, PlanType::Limit, schema_)
        , limit(limit_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, const Context & context, size_t max_streams) override;

    size_t limit;
};
} // namespace DB