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

#include <Common/TiFlashException.h>
#include <Flash/Planner/PlanType.h>

namespace DB
{
String PlanType::toString() const
{
    switch (enum_value)
    {
    case Limit:
        return "Limit";
    case TopN:
        return "TopN";
    case Filter:
        return "Filter";
    case Aggregation:
        return "Aggregation";
    case ExchangeSender:
        return "ExchangeSender";
    case MockExchangeSender:
        return "MockExchangeSender";
    case ExchangeReceiver:
        return "ExchangeReceiver";
    case MockExchangeReceiver:
        return "MockExchangeReceiver";
    case Projection:
        return "Projection";
    case Window:
        return "Window";
    case WindowSort:
        return "WindowSort";
    case TableScan:
        return "TableScan";
    case MockTableScan:
        return "MockTableScan";
    case Join:
        return "Join";
    default:
        throw TiFlashException("Unknown PlanType", Errors::Planner::Internal);
    }
}
} // namespace DB
