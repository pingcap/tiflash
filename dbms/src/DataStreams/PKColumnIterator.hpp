#pragma once

#include <Storages/Transaction/TiKVHandle.h>

namespace DB
{
struct PKColumnIterator : public std::iterator<std::random_access_iterator_tag, UInt64, size_t>
{
    PKColumnIterator & operator++()
    {
        ++pos;
        return *this;
    }

    PKColumnIterator & operator--()
    {
        --pos;
        return *this;
    }

    PKColumnIterator & operator=(const PKColumnIterator & itr)
    {
        pos = itr.pos;
        column = itr.column;
        return *this;
    }

    UInt64 operator*() const { return column->getUInt(pos); }

    size_t operator-(const PKColumnIterator & itr) const { return pos - itr.pos; }

    PKColumnIterator(const int pos_, const IColumn * column_) : pos(pos_), column(column_) {}

    void operator+=(size_t n) { pos += n; }

    size_t pos;
    const IColumn * column;
};

template<typename HandleType>
inline bool PkCmp(const UInt64 & a, const TiKVHandle::Handle<HandleType> & b)
{
    return static_cast<HandleType>(a) < b;
}
}
