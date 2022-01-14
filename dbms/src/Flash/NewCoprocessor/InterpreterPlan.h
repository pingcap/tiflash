#pragma once

#include <DataStreams/BlockIO.h>
#include <Flash/NewCoprocessor/PlanQuerySource.h>
#include <Interpreters/IInterpreter.h>

namespace DB
{
class Context;
class ExchangeReceiver;

/** build ch plan from plan: plan -> ch plan
  */
class InterpreterPlan : public IInterpreter
{
public:
    InterpreterPlan(Context & context_, const PlanQuerySource & plan_query_source_);

    ~InterpreterPlan() = default;

    BlockIO execute() override;

private:
    BlockInputStreams executePlan(std::vector<SubqueriesForSets> & subqueries_for_sets);
    void initMPPExchangeReceiver();

    DAGContext & dagContext() const { return *context.getDAGContext(); }

    Context & context;
    const PlanQuerySource & plan_query_source;
    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;
    // key: plan.executor_id of ExchangeReceiver nodes in plan tree.
    std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> mpp_exchange_receiver_maps;
};
} // namespace DB