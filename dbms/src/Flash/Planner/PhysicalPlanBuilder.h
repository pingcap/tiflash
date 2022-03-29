#pragma once

#include <Flash/Planner/PhysicalPlan.h>
#include <Interpreters/ExpressionActions.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalPlanBuilder
{
public:
    PhysicalPlanBuilder(Context & context_)
        : context(context_)
        , settings(context.getSettingsRef())
    {}

    void buildAggregation(const String & executor_id, const tipb::Aggregation & aggregation);
    void buildFilter(const String & executor_id, const tipb::Selection & selection);
    void buildLimit(const String & executor_id, const tipb::Limit & limit);
    void buildTopN(const String & executor_id, const tipb::TopN & top_n);
    void buildExchangeSender(const String & executor_id, const tipb::ExchangeSender & exchange_sender);
    void buildSource(const String & executor_id, const NamesAndTypes & source_schema, const Block & source_sample_block);

    void buildNonRootFinalProjection(const String & column_prefix);
    void buildRootFinalProjection(
        const std::vector<tipb::FieldType> & require_schema,
        const std::vector<Int32> & output_offsets,
        const String & column_prefix,
        bool keep_session_timezone_info);

    PhysicalPlanPtr getRes() const { return cur_plan; }

private:
    ExpressionActionsPtr newActionsForNewPlan();

    ExpressionActionsPtr newActionsFromSchema();

    void assignCurPlan(const PhysicalPlanPtr & new_cur_plan);

    PhysicalPlanPtr cur_plan;

    Context & context;
    Settings settings;
    NamesAndTypes schema;
};
} // namespace DB