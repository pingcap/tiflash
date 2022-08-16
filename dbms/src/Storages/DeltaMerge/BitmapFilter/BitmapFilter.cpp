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
ArrayBitmapFilter::ArrayBitmapFilter(size_t size_, SegmentSnapshotPtr snapshot_)
    : filter(size_, 0)
    , snap(snapshot_)
{}

void ArrayBitmapFilter::set(const ColumnPtr & col)
{
    for (size_t i = 0; i < col->size(); i++)
    {
        auto row_id = col->getUInt(i);
        if (row_id >= filter.size())
        {
            throw Exception(fmt::format("SegmentRowId {} is greater or equal than filter size {}", row_id, filter.size()));
        }
        filter[row_id] = 1;
    }
}

void ArrayBitmapFilter::get(IColumn::Filter & f, size_t start, size_t limit) const
{
    for (size_t i = 0; i < limit; i++)
    {
        f[i] = filter[start + i];
    }
}

SegmentSnapshotPtr ArrayBitmapFilter::snapshot() const
{
    return snap;
}

RoaringBitmapFilter::RoaringBitmapFilter(size_t size_, SegmentSnapshotPtr snapshot_)
    : filter(size_, 0)
    , snap(snapshot_)
{}

// TODO(jinhelin): since most of the bits in bitmap is 1, reverse it to save memory.
void RoaringBitmapFilter::set(const ColumnPtr & col)
{
    for (size_t i = 0; i < col->size(); i++)
    {
        auto row_id = col->getUInt(i);
        if (row_id >= filter.size())
        {
            throw Exception(fmt::format("SegmentRowId {} is greater or equal than filter size {}", row_id, filter.size()));
        }
        filter[row_id] = 1;
    }
}

void RoaringBitmapFilter::get(IColumn::Filter & f, size_t start, size_t limit) const
{
    for (size_t i = 0; i < limit; i++)
    {
        f[i] = filter[start + i];
    }
}

SegmentSnapshotPtr RoaringBitmapFilter::snapshot() const
{
    return snap;
}

} // namespace DB::DM