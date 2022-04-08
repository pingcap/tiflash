#pragma once

#include <Core/Block.h>
#include <Flash/Planner/plans/PhysicalLeaf.h>

namespace DB
{
class PhysicalSource : public PhysicalLeaf
{
public:
    static PhysicalPlanPtr build(
        const String & executor_id,
        const NamesAndTypes & source_schema,
        const Block & source_sample_block)
    {
        return std::make_shared<PhysicalSource>(executor_id, source_schema, source_sample_block);
    }

    PhysicalSource(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const Block & sample_block_)
        : PhysicalLeaf(executor_id_, PlanType::Source, schema_)
        , sample_block(sample_block_)
    {
        is_record_profile_streams = false;
    }

    void transformImpl(DAGPipeline &, Context &, size_t) override {}

    void finalize(const Names &) override {}

    const Block & getSampleBlock() const override { return sample_block; }

private:
    Block sample_block;
};
} // namespace DB