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
        const NamesAndTypes & schema_,
        const Block & sample_block_)
        : PhysicalPlan(executor_id_, PlanType::Source, schema_)
        , sample_block(sample_block_)
    {}

    PhysicalPlanPtr children(size_t) const override
    {
        throw Exception("the children size of PhysicalSource is zero");
    }

    void setChild(size_t, const PhysicalPlanPtr &) override
    {
        throw Exception("the children size of PhysicalSource is zero");
    }

    void appendChild(const PhysicalPlanPtr &) override
    {
        throw Exception("the children size of PhysicalSource is zero");
    }

    size_t childrenSize() const override { return 0; };

    void transformImpl(DAGPipeline &, const Context &, size_t) override {}

    void finalize(const Names &) override {}

    const Block & getSampleBlock() const override { return sample_block; }

private:
    Block sample_block;
};
} // namespace DB