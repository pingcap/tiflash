#pragma once

#include <algorithm>

#include <Common/typeid_cast.h>
#include <Core/Types.h>

namespace DB
{
namespace DM
{

struct ColumnAndValue
{
    UInt16 column;
    UInt64 value;

    ColumnAndValue(UInt16 column_, UInt64 value_) : column(column_), value(value_) {}
};

using ColumnAndValues = std::vector<ColumnAndValue>;

/// A tuple referenced to columns.
struct RefTuple
{
    ColumnAndValues values;

    RefTuple(UInt16 column, UInt64 value) : values{ColumnAndValue(column, value)} {}

    RefTuple(const ColumnAndValues & values_) : values(values_)
    {
        std::sort(values.begin(), values.end(), [](const ColumnAndValue & a, const ColumnAndValue & b) { return a.column < b.column; });
    }
};

using RefTuples = std::vector<RefTuple>;

} // namespace DM
} // namespace DB