#pragma once

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Plan/PlanBase.h>
#include <Interpreters/Context.h>
#include <Interpreters/IQuerySource.h>

namespace DB
{
/// PlanQuerySource is an adaptor between Plan and CH's executeQuery.
class PlanQuerySource : public IQuerySource
{
public:
    explicit PlanQuerySource(Context & context_);

    std::tuple<std::string, ASTPtr> parse(size_t) override;
    String str(size_t max_query_size) override;
    std::unique_ptr<IInterpreter> interpreter(Context & context, QueryProcessingStage::Enum stage) override;

    PlanPtr getRootPlan() const { return root_plan; }

    DAGContext & getDAGContext() const { return *context.getDAGContext(); }

private:
    Context & context;
    PlanPtr root_plan;
};

} // namespace DB
