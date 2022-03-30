#pragma once

#include <Flash/Planner/plans/PhysicalUnary.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

namespace DB
{
class PhysicalExchangeSender : public PhysicalUnary
{
public:
    static PhysicalPlanPtr build(
        const String & executor_id,
        const tipb::ExchangeSender & exchange_sender,
        PhysicalPlanPtr child);

    PhysicalExchangeSender(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const std::vector<Int64> & partition_col_id_,
        const TiDB::TiDBCollators & collators_,
        const tipb::ExchangeType & exchange_type_)
        : PhysicalUnary(executor_id_, PlanType::ExchangeSender, schema_)
        , partition_col_id(partition_col_id_)
        , collators(collators_)
        , exchange_type(exchange_type_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, const Context & context, size_t max_streams) override;

    std::vector<Int64> partition_col_id;
    TiDB::TiDBCollators collators;
    tipb::ExchangeType exchange_type;
};
} // namespace DB