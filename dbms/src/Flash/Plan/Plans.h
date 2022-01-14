#pragma once

#include <Flash/Plan/Plan.h>
#include <tipb/executor.pb.h>

namespace DB
{
struct AggImpl
{
    static constexpr size_t children_size = 1;

    using Executor = tipb::Aggregation;

    static tipb::ExecType tp()
    {
        // stream agg is looked as agg.
        return tipb::ExecType::TypeAggregation;
    }
};
using AggPlan = Plan<AggImpl>;

struct FilterImpl
{
    static constexpr size_t children_size = 1;

    using Executor = tipb::Selection;

    static tipb::ExecType tp()
    {
        return tipb::ExecType::TypeSelection;
    }
};
using FilterPlan = Plan<FilterImpl>;

struct LimitImpl
{
    static constexpr size_t children_size = 1;

    using Executor = tipb::Limit;

    static tipb::ExecType tp()
    {
        return tipb::ExecType::TypeLimit;
    }
};
using LimitPlan = Plan<LimitImpl>;

struct ProjectImpl
{
    static constexpr size_t children_size = 1;

    using Executor = tipb::Projection;

    static tipb::ExecType tp()
    {
        return tipb::ExecType::TypeProjection;
    }
};
using ProjectPlan = Plan<ProjectImpl>;

struct TopNImpl
{
    static constexpr size_t children_size = 1;

    using Executor = tipb::TopN;

    static tipb::ExecType tp()
    {
        return tipb::ExecType::TypeTopN;
    }
};
using TopNPlan = Plan<TopNImpl>;

struct JoinImpl
{
    static constexpr size_t children_size = 2;

    using Executor = tipb::Join;

    static tipb::ExecType tp()
    {
        return tipb::ExecType::TypeJoin;
    }
};
using JoinPlan = Plan<JoinImpl>;

struct ExchangeReceiverImpl
{
    static constexpr size_t children_size = 0;

    using Executor = tipb::ExchangeReceiver;

    static tipb::ExecType tp()
    {
        return tipb::ExecType::TypeExchangeReceiver;
    }
};
using ExchangeReceiverPlan = Plan<ExchangeReceiverImpl>;

struct ExchangeSenderImpl
{
    static constexpr size_t children_size = 1;

    using Executor = tipb::ExchangeSender;

    static tipb::ExecType tp()
    {
        return tipb::ExecType::TypeExchangeSender;
    }
};
using ExchangeSenderPlan = Plan<ExchangeSenderImpl>;

struct TableScanImpl
{
    static constexpr size_t children_size = 0;

    using Executor = tipb::TableScan;

    static tipb::ExecType tp()
    {
        return tipb::ExecType::TypeTableScan;
    }
};
using TableScanPlanBase = Plan<TableScanImpl>;
class TableScanPlan : public TableScanPlanBase
{
public:
    TableScanPlan(const TableScanImpl::Executor & executor_, const String & executor_id_)
        : TableScanPlanBase(executor_, executor_id_)
    {}

    std::shared_ptr<FilterPlan> pushed_down_filter;

    void pushDownFilter(const std::shared_ptr<FilterPlan> & filter)
    {
        if (pushed_down_filter)
            throw TiFlashException("table scan already has a push-down filter", Errors::Coprocessor::Internal);
        if (!filter)
            throw TiFlashException("pushed down filter is nullptr", Errors::Coprocessor::Internal);
        pushed_down_filter = filter;
    }
};
} // namespace DB