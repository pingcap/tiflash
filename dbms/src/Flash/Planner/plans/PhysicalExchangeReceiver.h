#pragma once

#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Flash/Planner/plans/PhysicalLeaf.h>
#include <tipb/executor.pb.h>

namespace DB
{
class ExchangeReceiver;

class PhysicalExchangeReceiver : public PhysicalLeaf
{
public:
    static PhysicalPlanPtr build(
        const Context & context,
        const String & executor_id);

    PhysicalExchangeReceiver(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const std::shared_ptr<ExchangeReceiver> & mpp_exchange_receiver_)
        : PhysicalLeaf(executor_id_, PlanType::ExchangeSender, schema_)
        , mpp_exchange_receiver(mpp_exchange_receiver_)
    {
        ColumnsWithTypeAndName columns;
        for (const auto & column : schema_)
            columns.emplace_back(column.type, column.name);
        sample_block = Block(columns);
    }

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, const Context & context, size_t max_streams) override;

    std::shared_ptr<ExchangeReceiver> mpp_exchange_receiver;
    Block sample_block;
};
} // namespace DB