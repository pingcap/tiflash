#pragma once

#include <common/types.h>

namespace DB
{
enum PlanType
{
    Selection,
    Aggregation,
    Limit,
    TopN,
    Projection,
    Source,
};

String toString(const PlanType & plan_type);
} // namespace DB