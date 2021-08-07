#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>
#include <fmt/core.h>

namespace DB
{

// mock an array that returns same value at all indexex.
template <typename T>
class FakeArray
{
public:
    FakeArray(const T & value_) : value(value_) {}

    const T & operator[](ssize_t) const { return value; }

private:
    T value;
};

template <typename T>
auto getColumnData(const ColumnConst * column)
{
    return FakeArray<T>(column->getValue<T>());
}

template <typename T, typename U>
auto & getColumnData(const ColumnVector<U> * column)
{
    static_assert(std::is_same_v<T, U>);
    return column->getData();
}

template <typename T, typename U>
auto & getColumnData(const ColumnDecimal<U> * column)
{
    static_assert(std::is_same_v<T, U>);
    return column->getData();
}

} // namespace DB
