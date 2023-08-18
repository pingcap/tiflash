// Copyright 2023 PingCAP, Inc.
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

#pragma once

#include <common/types.h>

namespace DB
{
struct PlanType
{
    enum PlanTypeEnum
    {
        Limit = 0,
        TopN = 1,
        Filter = 2,
        Aggregation = 3,
        ExchangeSender = 4,
        MockExchangeSender = 5,
        ExchangeReceiver = 6,
        MockExchangeReceiver = 7,
        Projection = 8,
        Window = 9,
        WindowSort = 10,
        TableScan = 11,
        MockTableScan = 12,
        Join = 13,
        AggregationBuild = 14,
        AggregationConvergent = 15,
        Expand = 16,
        JoinBuild = 17,
        JoinProbe = 18,
        GetResult = 19,
    };
    PlanTypeEnum enum_value;

    PlanType(int value = 0) // NOLINT(google-explicit-constructor)
        : enum_value(static_cast<PlanTypeEnum>(value))
    {}

    PlanType & operator=(int value)
    {
        this->enum_value = static_cast<PlanTypeEnum>(value);
        return *this;
    }

    operator int() const // NOLINT(google-explicit-constructor)
    {
        return this->enum_value;
    }

    String toString() const;
};
} // namespace DB
