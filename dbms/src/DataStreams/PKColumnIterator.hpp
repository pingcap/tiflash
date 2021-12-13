#pragma once

#include <Storages/Transaction/TiKVHandle.h>

namespace DB
{
struct PKColumnIterator
{
    using iterator_type = PKColumnIterator;
    using iterator_category = std::random_access_iterator_tag;
    using value_type = UInt64;
    using difference_type = size_t;
    using pointer = value_type *;
    using reference = value_type &;

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
        copy(itr);
        return *this;
    }

    UInt64 operator*() const { return column->getUInt(pos); }

    size_t operator-(const PKColumnIterator & itr) const { return pos - itr.pos; }

    PKColumnIterator(const int pos_, const IColumn * column_)
        : pos(pos_)
        , column(column_)
    {}

    PKColumnIterator(const PKColumnIterator & itr) { copy(itr); }

    void operator+=(size_t n) { pos += n; }

    size_t pos;
    const IColumn * column;

private:
    inline void copy(const PKColumnIterator & itr)
    {
        pos = itr.pos;
        column = itr.column;
    }
};

template <typename HandleType>
inline bool PkCmp(const UInt64 & a, const TiKVHandle::Handle<HandleType> & b)
{
    return static_cast<HandleType>(a) < b;
}
} // namespace DB
