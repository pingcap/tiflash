// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/Exception.h>
#include <Flash/Planner/PlanType.h>

namespace DB
{
String toString(const PlanType & plan_type)
{
    switch (plan_type)
    {
    case PhysicalJoinType:
        return "Join";
    case Selection:
        return "Selection";
    case Aggregation:
        return "Aggregation";
    case Limit:
        return "Limit";
    case TopN:
        return "TopN";
    case Projection:
        return "Projection";
    case Source:
        return "Source";
    case ExchangeSender:
        return "ExchangeSender";
    case TableScan:
        return "TableScan";
    default:
        throw Exception("Unknown PlanType");
    }
}
} // namespace DB