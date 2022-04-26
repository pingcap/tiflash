#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Planner/PhysicalPlanBuilder.h>
#include <Flash/Planner/plans/PhysicalAggregation.h>
#include <Flash/Planner/plans/PhysicalExchangeSender.h>
#include <Flash/Planner/plans/PhysicalFilter.h>
#include <Flash/Planner/plans/PhysicalLimit.h>
#include <Flash/Planner/plans/PhysicalProjection.h>
#include <Flash/Planner/plans/PhysicalSource.h>
#include <Flash/Planner/plans/PhysicalTopN.h>
#include <Interpreters/Context.h>

namespace DB
{
void PhysicalPlanBuilder::build(const String & executor_id, const tipb::Executor * executor)
{
    assert(executor);
    switch (executor->tp())
    {
    case tipb::ExecType::TypeProjection:
        cur_plans.push_back(PhysicalProjection::build(context, executor_id, log->identifier(), executor->projection(), popBack()));
        break;
    case tipb::ExecType::TypeAggregation:
    case tipb::ExecType::TypeStreamAgg:
        cur_plans.push_back(PhysicalAggregation::build(context, executor_id, log->identifier(), executor->aggregation(), popBack()));
        break;
    case tipb::ExecType::TypeSelection:
        cur_plans.push_back(PhysicalFilter::build(context, executor_id, log->identifier(), executor->selection(), popBack()));
        break;
    case tipb::ExecType::TypeExchangeSender:
        cur_plans.push_back(PhysicalExchangeSender::build(executor_id, log->identifier(), executor->exchange_sender(), popBack()));
        break;
    case tipb::ExecType::TypeLimit:
        cur_plans.push_back(PhysicalLimit::build(executor_id, log->identifier(), executor->limit(), popBack()));
        break;
    case tipb::ExecType::TypeTopN:
        cur_plans.push_back(PhysicalTopN::build(context, executor_id, log->identifier(), executor->topn(), popBack()));
        break;
    default:
        throw TiFlashException(fmt::format("{} executor is not supported", executor->tp()), Errors::Coprocessor::Unimplemented);
    }
}

void PhysicalPlanBuilder::buildNonRootFinalProjection(const String & column_prefix)
{
    cur_plans.push_back(PhysicalProjection::buildNonRootFinal(context, log->identifier(), column_prefix, popBack()));
}

DAGContext & PhysicalPlanBuilder::dagContext() const
{
    return *context.getDAGContext();
}

void PhysicalPlanBuilder::buildRootFinalProjection(const String & column_prefix)
{
    cur_plans.push_back(PhysicalProjection::buildRootFinal(
        context,
        log->identifier(),
        dagContext().output_field_types,
        dagContext().output_offsets,
        column_prefix,
        dagContext().keep_session_timezone_info,
        popBack()));
}

PhysicalPlanPtr PhysicalPlanBuilder::popBack()
{
    RUNTIME_ASSERT(!cur_plans.empty(), log, "cur_plans is empty, cannot popBack");
    PhysicalPlanPtr back = cur_plans.back();
    cur_plans.pop_back();
    return back;
}

void PhysicalPlanBuilder::buildSource(const Block & sample_block)
{
    cur_plans.push_back(PhysicalSource::build(sample_block, log->identifier()));
}
} // namespace DB
