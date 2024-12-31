// Copyright 2024 PingCAP, Inc.
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

namespace DB::DM
{
template <>
class HandleColumn<Int64>
{
public:
    HandleColumn(const IColumn & col)
        : data(toColumnVectorData<Int64>(col))
    {}

    auto begin() const { return data.begin(); }

    auto end() const { return data.end(); }

private:
    const PaddedPODArray<Int64> & data;
};

template <>
class HandleColumn<String>
{
public:
    HandleColumn(const IColumn & col)
        : offsets(typeid_cast<const ColumnString &>(col).getOffsets())
        , chars(typeid_cast<const ColumnString &>(col).getChars())
    {}

    class Iterator
    {
    public:
        Iterator(const IColumn::Offsets & offsets, const ColumnString::Chars_t & chars, size_t pos)
            : offsets(offsets)
            , chars(chars)
            , pos(pos)
        {}

        std::string_view operator*() const
        {
            assert(chars_offsets[-1] == 0);
            const auto off = offsets[pos];
            const auto size = offsets[pos] - offsets[pos - 1] - 1;
            return std::string_view(&chars[off], size);
        }

        Iterator & operator+(size_t n)
        {
            pos += n;
            return *this;
        }

        Iterator & operator-(size_t n)
        {
            pos -= n;
            return *this;
        }

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

        Iterator & operator++(int) = delete;
        Iterator & operator--(int) = delete;

        bool operator!=(const Iterator & other) const { return pos != other.pos; }

    private:
        const IColumn::Offsets & offsets;
        const ColumnString::Chars_t & chars;
        size_t pos = 0;
    };

    auto begin() const { return Iterator(offsets, chars, 0); }

    auto end() const { return Iterator(offsets, chars, offsets.size()); }

private:
    const IColumn::Offsets & offsets;
    const ColumnString::Chars_t & chars;
};

} // namespace DB::DM
