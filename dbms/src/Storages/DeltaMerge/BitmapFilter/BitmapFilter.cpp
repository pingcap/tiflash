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
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include "common/types.h"

namespace DB::DM
{
ArrayBitmapFilter::ArrayBitmapFilter(UInt64 size_, SegmentSnapshotPtr snapshot_)
    : filter(size_, 0)
    , snap(snapshot_)
{}

void ArrayBitmapFilter::set(const ColumnPtr & col)
{
    const auto * v = toColumnVectorDataPtr<UInt64>(col);
    for (auto p = v->begin(); p != v->end(); ++p)
    {
        UInt64 row_id = *p;
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

void RoaringBitmapFilter::set(const ColumnPtr & col)
{
    const auto * v = toColumnVectorDataPtr<UInt64>(col);
    // TODO(jinhelin): check row_id < sz
    rrbitmap.addMany(col->size(), v->data());
/*
    for (auto p = v->begin(); p != v->end(); ++p)
    {
        UInt64 row_id = *p;
        if (likely(row_id < sz))
        {
            rrbitmap.add(row_id);
        }
        else
        {
            throw Exception(fmt::format("SegmentRowId {} is greater or equal than filter size {}", row_id, sz));
        }
    }*/
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
//===================================

RoaringBitmap32Filter::RoaringBitmap32Filter(UInt32 size_, SegmentSnapshotPtr snapshot_)
    : sz(size_)
    , snap(snapshot_)
    , all_match(false)
{}

void RoaringBitmap32Filter::set(const ColumnPtr & col)
{
    const auto * v = toColumnVectorDataPtr<UInt64>(col);

    std::vector<UInt32> t;
    t.reserve(col->size());
    for (auto p = v->begin(); p != v->end(); ++p)
    {
        t.push_back(static_cast<UInt32>(*p));
    }
    // TODO(jinhelin): check row_id < sz
    rrbitmap.addMany(t.size(), t.data());
/*
    for (auto p = v->begin(); p != v->end(); ++p)
    {
        UInt64 row_id = *p;
        if (likely(row_id < sz))
        {
            rrbitmap.add(row_id);
        }
        else
        {
            throw Exception(fmt::format("SegmentRowId {} is greater or equal than filter size {}", row_id, sz));
        }
    }*/
}

void RoaringBitmap32Filter::get(IColumn::Filter & f, UInt32 start, UInt32 limit) const
{
    if (all_match)
    {
        static const UInt8 match = 1;
        f.assign(f.size(), match);
        return;
    }

    for (UInt32 i = 0; i < limit; i++)
    {
        f[i] = rrbitmap.contains(start + i);
    }
}

SegmentSnapshotPtr RoaringBitmap32Filter::snapshot() const
{
    return snap;
}

void RoaringBitmap32Filter::runOptimize()
{
    rrbitmap.runOptimize();
    rrbitmap.shrinkToFit();
    all_match = rrbitmap.cardinality() == sz;
    if (all_match)
    {
        //rrbitmap.clear();
    }
}
} // namespace DB::DM