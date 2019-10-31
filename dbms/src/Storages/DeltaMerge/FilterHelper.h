#pragma once

#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Range.h>

namespace DB
{
namespace DM
{

inline RSOperatorPtr toFilter(HandleRange handle_range)
{
    Attr handle_attr = {EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_TYPE};
    auto left        = createGreaterEqual(handle_attr, Field(handle_range.start), -1);
    auto right       = createLess(handle_attr, Field(handle_range.end), -1);
    return createAnd({left, right});
}

inline RSOperatorPtr withHanleRange(const RSOperatorPtr & filter, HandleRange handle_range)
{
    return !filter ? toFilter(handle_range) : createAnd({toFilter(handle_range), filter});
}

} // namespace DM
} // namespace DB