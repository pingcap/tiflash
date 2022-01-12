#pragma once

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

    using Executor = tipb::Selection;

    static tipb::ExecType tp()
    {
        return tipb::ExecType::TypeSelection;
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
}