#include <Flash/Plan/Plan.h>
#include <Flash/Plan/Plans.h>
#include <Flash/Plan/toPlan.h>
#include <common/types.h>
#include <fmt/format.h>
#include <tipb/select.pb.h>

#include <memory>

namespace DB
{
namespace
{
PlanPtr arrayToPlan(const tipb::DAGRequest & dag_request)
{
    auto to_plan = [](const tipb::Executor & executor, size_t i) -> PlanPtr {
        // for executors dag request, executor maybe isn't has executor_id
        auto executor_id = [&](const String & type) {
            return executor.has_executor_id() ? executor.executor_id() : fmt::format("{}_{}", type, i);
        };
        switch (executor.tp())
        {
        case tipb::ExecType::TypeTableScan:
            return std::make_shared<TableScanPlan>(executor.tbl_scan(), executor_id("table_scan"));
        case tipb::ExecType::TypeJoin:
            return std::make_shared<JoinPlan>(executor.join(), executor_id("join"));
        case tipb::ExecType::TypeSelection:
            return std::make_shared<FilterPlan>(executor.selection(), executor_id("selection"));
        case tipb::ExecType::TypeAggregation:
        case tipb::ExecType::TypeStreamAgg:
            return std::make_shared<AggPlan>(executor.aggregation(), executor_id("aggregation"));
        case tipb::ExecType::TypeTopN:
            return std::make_shared<TopNPlan>(executor.topn(), executor_id("top_n"));
        case tipb::ExecType::TypeLimit:
            return std::make_shared<LimitPlan>(executor.limit(), executor_id("limit"));
        case tipb::ExecType::TypeProjection:
            return std::make_shared<ProjectPlan>(executor.projection(), executor_id("projection"));
        case tipb::ExecType::TypeExchangeSender:
            return std::make_shared<ExchangeSenderPlan>(executor.exchange_sender(), executor_id("exchange_sender"));
        case tipb::ExecType::TypeExchangeReceiver:
            return std::make_shared<ExchangeReceiverPlan>(executor.exchange_receiver(), executor_id("exchange_receiver"));
        default:
            throw TiFlashException("Should not reach here", Errors::Coprocessor::Internal);
        }
    };

    int iter = dag_request.executors_size() - 1;
    assert(iter >= 0);
    PlanPtr next;
    PlanPtr cur = to_plan(dag_request.executors(iter), iter);
    --iter;
    for (; iter >= 0; --iter)
    {
        next = to_plan(dag_request.executors(iter), iter);
        // TODO remove pushed down filter
        if (cur->tp() == tipb::TypeTableScan && next->tp() == tipb::TypeSelection)
        {
            cur->toImpl<TableScanPlan>([&next](TableScanPlan & table_scan) {
                table_scan.pushDownFilter(std::dynamic_pointer_cast<FilterPlan>(next));
            });
        }
        next->appendChild(cur);
        cur = next;
    }
    return cur;
}

PlanPtr treeToPlan(const tipb::Executor & executor)
{
    assert(executor.has_executor_id());
    switch (executor.tp())
    {
    case tipb::ExecType::TypeTableScan:
        return std::make_shared<TableScanPlan>(executor.tbl_scan(), executor.executor_id());
    case tipb::ExecType::TypeJoin:
    {
        PlanPtr join = std::make_shared<JoinPlan>(executor.join(), executor.executor_id());

        // split left query block
        PlanPtr join_left_place = std::make_shared<PlacePlan>();
        join_left_place->appendChild(treeToPlan(executor.join().children(0)));
        join->appendChild(join_left_place);

        // split right query block
        PlanPtr join_right_place = std::make_shared<PlacePlan>();
        join_right_place->appendChild(treeToPlan(executor.join().children(1)));
        join->appendChild(join_right_place);

        return join;
    }
    case tipb::ExecType::TypeSelection:
    {
        std::shared_ptr<FilterPlan> sel = std::make_shared<FilterPlan>(executor.selection(), executor.executor_id());
        PlanPtr sel_child = treeToPlan(executor.selection().child());
        // TODO remove pushed down filter
        if (sel_child->tp() == tipb::TypeTableScan)
        {
            sel_child->toImpl<TableScanPlan>([&sel](TableScanPlan & table_scan) {
                table_scan.pushDownFilter(sel);
            });
        }
        sel->appendChild(sel_child);
        return sel;
    }
    case tipb::ExecType::TypeAggregation:
    case tipb::ExecType::TypeStreamAgg:
    {
        PlanPtr agg = std::make_shared<AggPlan>(executor.aggregation(), executor.executor_id());
        agg->appendChild(treeToPlan(executor.aggregation().child()));
        return agg;
    }
    case tipb::ExecType::TypeTopN:
    {
        PlanPtr top_n = std::make_shared<TopNPlan>(executor.topn(), executor.executor_id());
        top_n->appendChild(treeToPlan(executor.topn().child()));
        return top_n;
    }
    case tipb::ExecType::TypeLimit:
    {
        PlanPtr limit = std::make_shared<LimitPlan>(executor.limit(), executor.executor_id());
        limit->appendChild(treeToPlan(executor.limit().child()));
        return limit;
    }
    case tipb::ExecType::TypeProjection:
    {
        PlanPtr proj = std::make_shared<ProjectPlan>(executor.projection(), executor.executor_id());
        PlanPtr proj_place = std::make_shared<PlacePlan>();
        // split query block
        proj_place->appendChild(treeToPlan(executor.projection().child()));
        proj->appendChild(proj_place);
        return proj;
    }
    case tipb::ExecType::TypeExchangeSender:
    {
        PlanPtr sender = std::make_shared<ExchangeSenderPlan>(executor.exchange_sender(), executor.executor_id());
        sender->appendChild(treeToPlan(executor.exchange_sender().child()));
        return sender;
    }
    case tipb::ExecType::TypeExchangeReceiver:
        return std::make_shared<ExchangeReceiverPlan>(executor.exchange_receiver(), executor.executor_id());
    default:
        throw TiFlashException("Should not reach here", Errors::Coprocessor::Internal);
    }
}
} // namespace

PlanPtr toPlan(const tipb::DAGRequest & dag_request)
{
    assert(dag_request.executors_size() > 0 || dag_request.has_root_executor());
    if (dag_request.executors_size() > 0)
    {
        return arrayToPlan(dag_request);
    }
    else // dag_request->has_root_executor()
    {
        return treeToPlan(dag_request.root_executor());
    }
}
} // namespace DB