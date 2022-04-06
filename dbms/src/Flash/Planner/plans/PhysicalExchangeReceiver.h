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
        const std::shared_ptr<ExchangeReceiver> & mpp_exchange_receiver_);

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

    std::shared_ptr<ExchangeReceiver> mpp_exchange_receiver;
    Block sample_block;
};
} // namespace DB