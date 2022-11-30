// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Segment.h>
#include "common/types.h"

namespace DB::DM
{
BitmapFilter::BitmapFilter(UInt32 size_, const SegmentSnapshotPtr & snapshot_)
    : filter(size_, false)
    , snap(snapshot_)
    , all_match(false)
{}

void BitmapFilter::set(const UInt32 * data, UInt32 size)
{
    for (UInt32 i = 0; i < size; i++)
    {
        UInt32 row_id = *(data + i);
        RUNTIME_CHECK(row_id < filter.size(), row_id, filter.size());
        filter[row_id] = true;
    }
}

void BitmapFilter::set(const ColumnPtr & col)
{
    const auto * v = toColumnVectorDataPtr<UInt32>(col);
    set(v->data(), v->size());
}

void BitmapFilter::get(IColumn::Filter & f, UInt32 start, UInt32 limit) const
{
    RUNTIME_CHECK(start + limit <= filter.size(), start, limit, filter.size());
    if (all_match)
    {
        static const UInt8 match = 1;
        f.assign(static_cast<size_t>(limit), match);
    }
    else
    {
        for (UInt32 i = 0; i < limit; i++)
        {
            f[i] = filter[i + start];
        }
    }
}

bool BitmapFilter::checkPack(UInt32 start, UInt32 limit) const
{
    RUNTIME_CHECK(start + limit <= filter.size(), start, limit, filter.size());
    // TODO: make sure it can be vectorized
    for (UInt32 i = start; i < start + limit; ++i)
    {
        if (filter[i])
        {
            return true;
        }
    }
    return false;
}

void BitmapFilter::andWith(const BitmapFilterPtr & other)
{
    // TODO: make sure it can be vectorized
    RUNTIME_CHECK(filter.size() == other->filter.size(), filter.size(), other->filter.size());
    for (UInt32 i = 0; i < filter.size(); ++i)
    {
        filter[i] = filter[i] && other->filter[i];
    }
}

SegmentSnapshotPtr BitmapFilter::snapshot() const
{
    return snap == nullptr ? nullptr : snap->clone();
}

void BitmapFilter::runOptimize()
{
    bool temp = true;
    for (bool i : filter)
    {
        temp = (temp && i);
    }
    all_match = temp;
}

String BitmapFilter::toDebugString() const
{
    // String s(filter.size(), '1');
    UInt32 positive = 0;
    for (auto i : filter)
    {
        if (i)
        {
            // s[i] = '0';
            ++positive;
        }
    }
    return fmt::format("size={}, positive={}", filter.size(), positive);
}
} // namespace DB::DM