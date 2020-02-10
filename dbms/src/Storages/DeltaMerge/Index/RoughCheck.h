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

static constexpr int TRUE   = 1;
static constexpr int FAILED = 0;

template <template <typename, typename> class Op>
using Cmp = ValueComparision<Op>;

#define IS_LEGAL(lef_field, right_value) (Cmp<EqualsOp>::compare(lef_field, type, right_value) != FAILED)
#define EQUAL(lef_field, right_value) (Cmp<EqualsOp>::compare(lef_field, type, right_value) == TRUE)
#define LESS(lef_field, right_value) (Cmp<LessOp>::compare(lef_field, type, right_value) == TRUE)
#define GREATER(lef_field, right_value) (Cmp<GreaterOp>::compare(lef_field, type, right_value) == TRUE)
#define LESS_EQ(lef_field, right_value) (Cmp<LessOrEqualsOp>::compare(lef_field, type, right_value) == TRUE)
#define GREATER_EQ(lef_field, right_value) (Cmp<GreaterOrEqualsOp>::compare(lef_field, type, right_value) == TRUE)


template <typename T>
inline RSResult checkEqual(const Field & v, const DataTypePtr & type, const T & min, const T & max)
{
    if (!IS_LEGAL(v, min))
        return Some;

    //    if (min == max && v == min)
    //        return All;
    //    else if (v >= min && v <= max)
    //        return Some;
    //    else
    //        return None;

    if (min == max && EQUAL(v, min))
        return All;
    else if (GREATER_EQ(v, min) && LESS_EQ(v, max))
        return Some;
    else
        return None;
}

template <typename T>
inline RSResult checkGreater(const Field & v, const DataTypePtr & type, const T & min, const T & max)
{
    if (!IS_LEGAL(v, min))
        return Some;

    //    if (v >= max)
    //        return None;
    //    else if (v < min)
    //        return All;
    //    return Some;

    if (GREATER_EQ(v, max))
        return None;
    else if (LESS(v, min))
        return All;
    else
        return Some;
}

template <typename T>
inline RSResult checkGreaterEqual(const Field & v, const DataTypePtr & type, T min, T max)
{
    if (!IS_LEGAL(v, min))
        return Some;

    //    if (v > max)
    //        return None;
    //    else if (v <= min)
    //        return All;
    //    return Some;

    if (GREATER(v, max))
        return None;
    else if (LESS_EQ(v, min))
        return All;
    else
        return Some;
}

#undef IS_LEGAL
#undef EQUAL
#undef LESS
#undef GREATER
#undef LESS_EQ
#undef GREATER_EQ

} // namespace RoughCheck
} // namespace DM
} // namespace DB