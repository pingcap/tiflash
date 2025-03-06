// Copyright 2025 PingCAP, Inc.
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

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>

namespace DB::DM
{
// `ColumnView` is a class that provides unified access to both Int64 handles and String handles.
template <typename T>
class ColumnView
{
    static_assert(false, "Only support Int64 and String");
};

template <>
class ColumnView<Int64>
{
public:
    ColumnView(const IColumn & col)
        : data(toColumnVectorData<Int64>(col))
    {}

    auto begin() const { return data.begin(); }

    auto end() const { return data.end(); }

    Int64 operator[](size_t index) const
    {
        assert(index < data.size());
        return data[index];
    }

    size_t size() const { return data.size(); }

private:
    const PaddedPODArray<Int64> & data;
};

template <>
class ColumnView<String>
{
public:
    ColumnView(const IColumn & col)
        : offsets(typeid_cast<const ColumnString &>(col).getOffsets())
        , chars(typeid_cast<const ColumnString &>(col).getChars())
    {}

    class Iterator
    {
    public:
        using iterator_category = std::random_access_iterator_tag;
        using value_type = std::string_view;
        using difference_type = std::ptrdiff_t;

        Iterator(const IColumn::Offsets & offsets, const ColumnString::Chars_t & chars, size_t pos)
            : pos(pos)
            , offsets(&offsets)
            , chars(&chars)
        {}

        value_type operator*() const
        {
            assert((*offsets)[-1] == 0);
            const auto off = (*offsets)[pos - 1];
            const auto size = (*offsets)[pos] - (*offsets)[pos - 1] - 1;
            return std::string_view(reinterpret_cast<const char *>(chars->data() + off), size);
        }

        Iterator operator+(difference_type n) { return Iterator{*offsets, *chars, pos + n}; }

        Iterator operator-(difference_type n) { return Iterator{*offsets, *chars, pos - n}; }

        difference_type operator-(const Iterator & other) const { return pos - other.pos; }

        Iterator & operator++()
        {
            ++pos;
            return *this;
        }

        Iterator & operator--()
        {
            --pos;
            return *this;
        }

        Iterator operator++(int)
        {
            Iterator tmp = *this;
            ++pos;
            return tmp;
        }

        Iterator operator--(int)
        {
            Iterator tmp = *this;
            --pos;
            return tmp;
        }

        Iterator & operator+=(difference_type n)
        {
            pos += n;
            return *this;
        }

        Iterator & operator-=(difference_type n)
        {
            pos -= n;
            return *this;
        }

        // Perform a lexicographic comparison of elements.
        // Assume `this->offsets == other.offsets && this->chars == other.chars`,
        // so it equal to `this->pos <=> other.pos`.
        auto operator<=>(const Iterator & other) const = default;

    private:
        size_t pos = 0;
        const IColumn::Offsets * offsets; // Using pointer for operator assignment
        const ColumnString::Chars_t * chars;
    };

    auto begin() const { return Iterator(offsets, chars, 0); }

    auto end() const { return Iterator(offsets, chars, offsets.size()); }

    std::string_view operator[](size_t index) const
    {
        assert(index < offsets.size());
        const auto off = offsets[index - 1];
        const auto size = offsets[index] - offsets[index - 1] - 1;
        return std::string_view(reinterpret_cast<const char *>(chars.data() + off), size);
    }

    size_t size() const { return offsets.size(); }

private:
    const IColumn::Offsets & offsets;
    const ColumnString::Chars_t & chars;
};

} // namespace DB::DM
