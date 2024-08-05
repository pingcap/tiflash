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

#include <Core/AccurateComparison.h>
#include <Storages/DeltaMerge/Index/RSResult.h>
#include <Storages/DeltaMerge/Index/ValueComparison.h>

namespace DB
{
namespace DM
{
namespace RoughCheck
{
template <template <typename, typename> class Op>
using Cmp = ValueComparision<Op>;

#define IS_LEGAL(lef_field, right_value) \
    (Cmp<EqualsOp>::compare(lef_field, type, right_value) != ValueCompareResult::CanNotCompare)
#define EQUAL(lef_field, right_value) (Cmp<EqualsOp>::compare(lef_field, type, right_value) == ValueCompareResult::True)
#define LESS(lef_field, right_value) (Cmp<LessOp>::compare(lef_field, type, right_value) == ValueCompareResult::True)
#define GREATER(lef_field, right_value) \
    (Cmp<GreaterOp>::compare(lef_field, type, right_value) == ValueCompareResult::True)
#define LESS_EQ(lef_field, right_value) \
    (Cmp<LessOrEqualsOp>::compare(lef_field, type, right_value) == ValueCompareResult::True)
#define GREATER_EQ(lef_field, right_value) \
    (Cmp<GreaterOrEqualsOp>::compare(lef_field, type, right_value) == ValueCompareResult::True)


struct CheckEqual
{
    template <typename T>
    static RSResult check(const Field & v, const DataTypePtr & type, const T & min, const T & max)
    {
        if (!IS_LEGAL(v, min))
            return RSResult::Some;

        //    if (min == max && v == min)
        //        return All;
        //    else if (v >= min && v <= max)
        //        return Some;
        //    else
        //        return None;

        if (min == max && EQUAL(v, min))
            return RSResult::All;
        else if (GREATER_EQ(v, min) && LESS_EQ(v, max))
            return RSResult::Some;
        else
            return RSResult::None;
    }
};

struct CheckIn
{
    template <typename T>
    static RSResult check(const std::vector<Field> & values, const DataTypePtr & type, const T & min, const T & max)
    {
        RSResult result = RSResult::None;
        for (const auto & v : values)
        {
            if (result == RSResult::All)
                break;
            // skip null value
            if (v.isNull())
                continue;
            result = result || CheckEqual::check<T>(v, type, min, max);
        }
        return result;
    }
};

struct CheckGreater
{
    template <typename T>
    static RSResult check(const Field & v, const DataTypePtr & type, const T & min, const T & max)
    {
        if (!IS_LEGAL(v, min))
            return RSResult::Some;

        //    if (v >= max)
        //        return None;
        //    else if (v < min)
        //        return All;
        //    return Some;

        if (GREATER_EQ(v, max))
            return RSResult::None;
        else if (LESS(v, min))
            return RSResult::All;
        else
            return RSResult::Some;
    }
};

struct CheckGreaterEqual
{
    template <typename T>
    static RSResult check(const Field & v, const DataTypePtr & type, T min, T max)
    {
        if (!IS_LEGAL(v, min))
            return RSResult::Some;

        //    if (v > max)
        //        return None;
        //    else if (v <= min)
        //        return All;
        //    return Some;

        if (GREATER(v, max))
            return RSResult::None;
        else if (LESS_EQ(v, min))
            return RSResult::All;
        else
            return RSResult::Some;
    }
};

#undef IS_LEGAL
#undef EQUAL
#undef LESS
#undef GREATER
#undef LESS_EQ
#undef GREATER_EQ

} // namespace RoughCheck
} // namespace DM
} // namespace DB
