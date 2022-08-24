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

namespace DB::DM
{
ArrayBitmapFilter::ArrayBitmapFilter(UInt32 size_, const SegmentSnapshotPtr & snapshot_, const std::vector<UInt32> & data)
    : filter(size_, 0)
    , snap(snapshot_)
    , all_match(false)
{
    set(data.data(), data.size());
}

ArrayBitmapFilter::ArrayBitmapFilter(const SegmentSnapshotPtr & snapshot_)
    : snap(snapshot_)
    , all_match(true)
{}

void ArrayBitmapFilter::set(const UInt32 * data, UInt32 size)
{
    for (UInt32 i = 0; i < size; i++)
    {
        UInt32 row_id = *(data + i);
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

std::vector<UInt32> ArrayBitmapFilter::toUInt32Array() const
{
    std::vector<UInt32> v;
    v.reserve(filter.size());
    for (UInt32 i = 0; i < filter.size(); i++)
    {
        if (filter[i])
        {
            v.emplace_back(i);
        }
    }
    return v;
}

void ArrayBitmapFilter::set(const ColumnPtr & col)
{
    const auto * v = toColumnVectorDataPtr<UInt32>(col);
    set(v->data(), v->size());
}

void ArrayBitmapFilter::get(IColumn::Filter & f, UInt32 start, UInt32 limit) const
{
    if (all_match)
    {
        static const UInt8 match = 1;
        f.assign(static_cast<size_t>(limit), match);
        return;
    }

    for (UInt32 i = 0; i < limit; i++)
    {
        f[i] = filter[start + i];
    }
}

SegmentSnapshotPtr ArrayBitmapFilter::snapshot() const
{
    return snap;
}

void ArrayBitmapFilter::runOptimize()
{
    bool temp = true;
    for (auto i : filter)
    {
        temp = (temp && i);
    }
    all_match = temp;
}

RoaringBitmapFilterPtr ArrayBitmapFilter::toRoaringBitmapFilter() const
{
    if (all_match)
    {
        return std::make_shared<RoaringBitmapFilter>(snap);
    }
    else
    {
        auto v = toUInt32Array();
        return std::make_shared<RoaringBitmapFilter>(filter.size(), snap, v);
    }
}
// =================================================================================

RoaringBitmapFilter::RoaringBitmapFilter(UInt32 size_, const SegmentSnapshotPtr & snapshot_, const std::vector<UInt32> & data)
    : sz(size_)
    , rrbitmap(data.size(), data.data())
    , snap(snapshot_)
    , all_match(false)
{}

RoaringBitmapFilter::RoaringBitmapFilter(const SegmentSnapshotPtr & snapshot_)
    : sz(0)
    , snap(snapshot_)
    , all_match(true)
{}

void RoaringBitmapFilter::set(const ColumnPtr & col)
{
    const auto * v = toColumnVectorDataPtr<UInt32>(col);
    // TODO(jinhelin): check row_id < sz
    rrbitmap.addMany(v->size(), v->data());
}

void RoaringBitmapFilter::get(IColumn::Filter & f, UInt32 start, UInt32 limit) const
{
    if (all_match)
    {
        static const UInt8 match = 1;
        f.assign(static_cast<size_t>(limit), match);
        return;
    }

    for (UInt32 i = 0; i < limit; i++)
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
}

ArrayBitmapFilterPtr RoaringBitmapFilter::toArrayBitmapFilter() const
{
    if (all_match)
    {
        return std::make_shared<ArrayBitmapFilter>(snap);
    }
    else
    {
        std::vector<UInt32> v(rrbitmap.cardinality());
        rrbitmap.toUint32Array(v.data());
        return std::make_shared<ArrayBitmapFilter>(sz, snap, v);
    }
}

} // namespace DB::DM