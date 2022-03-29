#pragma once

#include <Flash/Planner/PhysicalPlan.h>
#include <tipb/select.pb.h>

namespace DB
{
class PhysicalExchangeSender : public PhysicalPlan
{
public:
    PhysicalExchangeSender(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const std::vector<Int64> & partition_col_id_,
        const TiDB::TiDBCollators & collators_,
        const tipb::ExchangeType & exchange_type_)
        : PhysicalPlan(executor_id_, PlanType::ExchangeSender, schema_)
        , partition_col_id(partition_col_id_)
        , collators(collators_)
        , exchange_type(exchange_type_)
    {}

    PhysicalPlanPtr children(size_t i) const override
    {
        assert(i == 0);
        assert(child);
        return child;
    }

    void setChild(size_t i, const PhysicalPlanPtr & new_child) override
    {
        assert(i == 0);
        child = new_child;
    }

    void appendChild(const PhysicalPlanPtr & new_child) override
    {
        assert(!child);
        assert(new_child);
        child = new_child;
    }

    size_t childrenSize() const override { return 1; };

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, const Context & context, size_t max_streams) override;

    PhysicalPlanPtr child;

    std::vector<Int64> partition_col_id;
    TiDB::TiDBCollators collators;
    tipb::ExchangeType exchange_type;
};
} // namespace DB