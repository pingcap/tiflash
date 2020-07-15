#pragma once

#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/PKRange.h>
#include <Storages/DeltaMerge/Range.h>

namespace DB
{
namespace DM
{

inline RSOperatorPtr toFilter(PKRange & pk_range)
{
    auto handle_range = pk_range.toHandleRange();
    Attr handle_attr  = {EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_TYPE};
    auto left         = createGreaterEqual(handle_attr, Field(handle_range.start), -1);
    auto right        = createLess(handle_attr, Field(handle_range.end), -1);
    return createAnd({left, right});
}

inline RSOperatorPtr withHandleRange(const RSOperatorPtr & filter, PKRange & pk_range)
{
    return !filter ? toFilter(pk_range) : createAnd({toFilter(pk_range), filter});
}

} // namespace DM
} // namespace DB