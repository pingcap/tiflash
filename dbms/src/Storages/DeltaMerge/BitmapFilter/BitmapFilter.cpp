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
#include <Storages/DeltaMerge/Segment.h>

namespace DB::DM
{
ArrayBitmapFilter::ArrayBitmapFilter(UInt64 size_, SegmentSnapshotPtr snapshot_)
    : filter(size_, 0)
    , snap(snapshot_)
{}

void ArrayBitmapFilter::set(const ColumnPtr & col)
{
    for (size_t i = 0; i < col->size(); i++)
    {
        UInt64 row_id = col->getUInt(i);
        if (likely(row_id < filter.size()))
        {
            filter[row_id] = 1;
        }
        else
        {
            throw Exception(fmt::format("SegmentRowId {} is greater or equal than filter size {}", row_id, filter.size()));
        }
    }
}

void ArrayBitmapFilter::get(IColumn::Filter & f, UInt64 start, UInt64 limit) const
{
    for (UInt64 i = 0; i < limit; i++)
    {
        f[i] = filter[start + i];
    }
}

SegmentSnapshotPtr ArrayBitmapFilter::snapshot() const
{
    return snap;
}

RoaringBitmapFilter::RoaringBitmapFilter(UInt64 size_, SegmentSnapshotPtr snapshot_)
    : sz(size_)
    , snap(snapshot_)
    , all_match(false)
{}

// TODO(jinhelin): since most of the bits in bitmap is 1, reverse it to save memory.
void RoaringBitmapFilter::set(const ColumnPtr & col)
{
    for (size_t i = 0; i < col->size(); i++)
    {
        UInt64 row_id = col->getUInt(i);
        if (likely(row_id < sz))
        {
            rrbitmap.add(row_id);
        }
        else
        {
            throw Exception(fmt::format("SegmentRowId {} is greater or equal than filter size {}", row_id, sz));
        }
    }
}

void RoaringBitmapFilter::get(IColumn::Filter & f, UInt64 start, UInt64 limit) const
{
    if (all_match)
    {
        static const UInt8 match = 1;
        f.assign(f.size(), match);
        return;
    }

    for (UInt64 i = 0; i < limit; i++)
    {
        f[i] = rrbitmap.contains(start + i);
    }
}

SegmentSnapshotPtr RoaringBitmapFilter::snapshot() const
{
    return snap;
}

void RoaringBitmapFilter::runOptimize()
{
    rrbitmap.runOptimize();
    rrbitmap.shrinkToFit();
    all_match = rrbitmap.cardinality() == sz;
    if (all_match)
    {
        rrbitmap.clear();
    }
}

} // namespace DB::DM