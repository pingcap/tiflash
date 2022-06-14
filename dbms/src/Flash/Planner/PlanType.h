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

#pragma once

#include <common/types.h>

namespace DB
{
struct PlanType
{
    enum PlanTypeEnum
    {
        Source = 0,
        Limit = 1,
        TopN = 2,
        Selection = 3,
        Aggregation = 4,
        ExchangeSender = 5,
        MockExchangeSender = 6,
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
