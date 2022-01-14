#pragma once

#include <Flash/Plan/PlanBase.h>
#include <tipb/select.pb.h>

namespace DB
{
PlanPtr toPlan(const tipb::DAGRequest & dag_request);
} // namespace DB