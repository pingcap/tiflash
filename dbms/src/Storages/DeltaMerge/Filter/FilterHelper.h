#pragma once

#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRange.h>

namespace DB
{
namespace DM
{

inline RSOperatorPtr toFilter(RowKeyRange & rowkey_range)
{
    Attr handle_attr = {EXTRA_HANDLE_COLUMN_NAME,
                        EXTRA_HANDLE_COLUMN_ID,
                        rowkey_range.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE};
    if (rowkey_range.is_common_handle)
    {
        auto left  = createGreaterEqual(handle_attr, Field(rowkey_range.start.value->data(), rowkey_range.start.value->size()), -1);
        auto right = createLess(handle_attr, Field(rowkey_range.end.value->data(), rowkey_range.end.value->size()), -1);
        return createAnd({left, right});
    }
    else
    {
        auto left  = createGreaterEqual(handle_attr, Field(rowkey_range.start.int_value), -1);
        auto right = createLess(handle_attr, Field(rowkey_range.end.int_value), -1);
        return createAnd({left, right});
    }
}

} // namespace DM
} // namespace DB