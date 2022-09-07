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
#define M(t) \
    case t:  \
        return #t;
        M(Limit)
        M(TopN)
        M(PartialTopN)
        M(FinalTopN)
        M(Filter)
        M(Aggregation)
        M(FinalAggregation)
        M(PartialAggregation)
        M(ExchangeSender)
        M(MockExchangeSender)
        M(ExchangeReceiver)
        M(MockExchangeReceiver)
        M(Projection)
        M(Window)
        M(WindowSort)
        M(TableScan)
        M(MockTableScan)
        M(Join)
        M(JoinProbe)
        M(NonJoinProbe)
        M(JoinBuild)
        M(ResultHandler)
        M(PipelineBreaker)
#undef M
    default:
        throw TiFlashException("Unknown PlanType", Errors::Planner::Internal);
    }
}
} // namespace DB
