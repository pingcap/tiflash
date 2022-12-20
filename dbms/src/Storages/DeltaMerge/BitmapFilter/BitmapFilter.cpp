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
BitmapFilter::BitmapFilter(UInt32 size_, const SegmentSnapshotPtr & snapshot_)
    : filter(size_, false)
    , snap(snapshot_)
    , all_match(false)
{}

void BitmapFilter::set(const ColumnPtr & col, const FilterPtr f)
{
    const auto * v = toColumnVectorDataPtr<UInt32>(col);
    set(v->data(), v->size(), f);
}

void BitmapFilter::set(const UInt32 * data, UInt32 size, const FilterPtr f)
{
    if (size == 0)
    {
        return;
    }
    //size_t max_row_id = *std::max_element(data, data + size);
    //RUNTIME_CHECK(max_row_id < filter.size(), max_row_id, filter.size());
    if (f == nullptr)
    {
         for (UInt32 i = 0; i < size; i++)
        {
            UInt32 row_id = *(data + i);
            filter[row_id] = true;
        }
    }
    else
    {
        RUNTIME_CHECK(size == f->size(), size, f->size());
        for (UInt32 i = 0; i < size; i++)
        {
            UInt32 row_id = *(data + i);
            filter[row_id] = (*f)[i];
        }
    }
}

bool BitmapFilter::get(IColumn::Filter & f, UInt32 start, UInt32 limit) const
{
    RUNTIME_CHECK(start + limit <= filter.size(), start, limit, filter.size());
    auto begin = filter.cbegin() + start;
    auto end = filter.cbegin() + start + limit;
    if (all_match || std::find(begin, end, false) == end)
    {
        //static const UInt8 match = 1;
        //f.assign(static_cast<size_t>(limit), match);
        return true;
    }
    else
    {
        std::copy(begin, end, f.begin());
        return false;
    }
}

SegmentSnapshotPtr & BitmapFilter::snapshot()
{
    return snap;
}

void BitmapFilter::runOptimize()
{
    all_match = std::find(filter.begin(), filter.end(), false) == filter.end();
}

String BitmapFilter::toDebugString() const
{
    String s(filter.size(), '1');
    for (UInt32 i = 0; i < filter.size(); i++)
    {
        if (!filter[i])
        {
            s[i] = '0';
        }
    }
    return fmt::format("{}", s);
}
} // namespace DB::DM