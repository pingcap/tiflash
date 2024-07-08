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

#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>

namespace DB::DM
{

// BitmapFilterView provides a subset of a BitmapFilter.
// Accessing BitmapFilterView[i] becomes accessing filter[offset+i].
class BitmapFilterView
{
private:
    BitmapFilterPtr filter;
    UInt32 filter_offset;
    UInt32 filter_size;

public:
    explicit BitmapFilterView(const BitmapFilterPtr & filter_, UInt32 offset_, UInt32 size_)
        : filter(filter_)
        , filter_offset(offset_)
        , filter_size(size_)
    {
        RUNTIME_CHECK(filter_offset + filter_size <= filter->size(), filter_offset, filter_size, filter->size());
    }

    /**
     * @brief Create a BitmapFilter and construct a BitmapFilterView with it.
     * Should be only used in tests.
     */
    static BitmapFilterView createWithFilter(UInt32 size, bool default_value)
    {
        return BitmapFilterView(std::make_shared<BitmapFilter>(size, default_value), 0, size);
    }

    inline bool get(UInt32 n) const
    {
        RUNTIME_CHECK(n < filter_size);
        return filter->get(filter_offset + n);
    }

    inline bool operator[](UInt32 n) const { return get(n); }

    inline UInt32 size() const { return filter_size; }

    inline UInt32 offset() const { return filter_offset; }

    String toDebugString() const
    {
        String s(size(), '1');
        for (UInt32 i = 0; i < size(); i++)
        {
            if (!get(i))
            {
                s[i] = '0';
            }
        }
        return s;
    }

    // Return how many valid rows.
    size_t count() const
    {
        size_t n = 0;
        for (UInt32 i = 0; i < size(); i++)
        {
            if (get(i))
                n++;
        }
        return n;
    }
};

} // namespace DB::DM
