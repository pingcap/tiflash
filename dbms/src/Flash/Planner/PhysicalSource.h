#pragma once

#include <Core/Block.h>
#include <Flash/Planner/PhysicalPlan.h>

namespace DB
{
class PhysicalSource : public PhysicalPlan
{
public:
    PhysicalSource(
        const String & executor_id_,
        const Names & schema_,
        const Block & sample_block_)
        : PhysicalPlan(executor_id_, PlanType::Source, schema_)
        , sample_block(sample_block_)
    {}

    PhysicalPlanPtr children(size_t) const override
    {
        throw Exception("");
    }

    void setChild(size_t, const PhysicalPlanPtr &) override
    {
        throw Exception("");
    }

    void appendChild(const PhysicalPlanPtr &) override
    {
        throw Exception("");
    }

    size_t childrenSize() const override { return 0; };

    void transform(DAGPipeline &, Context &, size_t) override {}

    bool finalize(const Names &) override { return false; }

    const Block & getSampleBlock() const override { return sample_block; }

private:
    Block sample_block;
};
} // namespace DB