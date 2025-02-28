// Copyright 2023 PingCAP, Inc.
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

namespace DB::DM
{
BitmapFilter::BitmapFilter(UInt32 size_, bool default_value)
    : filter(size_, static_cast<UInt8>(default_value))
    , all_match(default_value)
{}

void BitmapFilter::set(BlockInputStreamPtr & stream)
{
    stream->readPrefix();
    for (;;)
    {
        FilterPtr f = nullptr;
        auto blk = stream->read(f, /*res_filter*/ true);
        if (unlikely(!blk))
        {
            break;
        }

        const auto & row_ids_col = blk.segmentRowIdCol();
        const auto * v = toColumnVectorDataPtr<UInt32>(row_ids_col);
        assert(v != nullptr); // the segmentRowIdCol must be a UInt32 column
        set(std::span{v->data(), v->size()}, f);
    }
    stream->readSuffix();
}

void BitmapFilter::set(std::span<const UInt32> row_ids, const FilterPtr & f)
{
    if (row_ids.empty())
    {
        return;
    }
    if (!f)
    {
        for (auto row_id : row_ids)
        {
            filter[row_id] = true;
        }
    }
    else
    {
        RUNTIME_CHECK(row_ids.size() == f->size(), row_ids.size(), f->size());
        for (UInt32 i = 0; i < row_ids.size(); ++i)
        {
            filter[row_ids[i]] = (*f)[i];
        }
    }
}

void BitmapFilter::set(UInt32 start, UInt32 limit, bool value)
{
    RUNTIME_CHECK(start + limit <= filter.size(), start, limit, filter.size());
    std::fill_n(filter.begin() + start, limit, static_cast<UInt8>(value));
}

bool BitmapFilter::get(IColumn::Filter & f, UInt32 start, UInt32 limit) const
{
    RUNTIME_CHECK(start + limit <= filter.size(), start, limit, filter.size());
    auto begin = filter.cbegin() + start;
    auto end = filter.cbegin() + start + limit;
    if (all_match || std::find(begin, end, static_cast<UInt8>(false)) == end)
    {
        return true;
    }
    else
    {
        std::copy(begin, end, f.begin());
        return false;
    }
}

void BitmapFilter::rangeAnd(IColumn::Filter & f, UInt32 start, UInt32 limit) const
{
    RUNTIME_CHECK(start + limit <= filter.size() && f.size() == limit);
    auto begin = filter.cbegin() + start;
    if (!all_match)
    {
        std::transform(f.begin(), f.end(), begin, f.begin(), [](const auto a, const auto b) { return a && b; });
    }
}

void BitmapFilter::merge(const BitmapFilter & other)
{
    RUNTIME_CHECK(filter.size() == other.filter.size());
    if (all_match)
    {
        return;
    }
    for (UInt32 i = 0; i < filter.size(); ++i)
    {
        filter[i] = filter[i] || other.filter[i];
    }
}

void BitmapFilter::intersect(const BitmapFilter & other)
{
    RUNTIME_CHECK(filter.size() == other.filter.size());
    if (all_match)
    {
        std::copy(other.filter.cbegin(), other.filter.cend(), filter.begin());
        all_match = other.all_match;
        return;
    }
    for (UInt32 i = 0; i < filter.size(); ++i)
    {
        filter[i] = filter[i] && other.filter[i];
    }
    all_match = all_match && other.all_match;
}

bool BitmapFilter::isAllNotMatch(UInt32 start, UInt32 limit) const
{
    RUNTIME_CHECK(start + limit <= filter.size());
    if (all_match)
    {
        return false;
    }
    auto begin = filter.cbegin() + start;
    auto end = filter.cbegin() + start + limit;
    return std::find(begin, end, static_cast<UInt8>(true)) == end;
}

void BitmapFilter::append(const BitmapFilter & other)
{
    filter.reserve(filter.size() + other.filter.size());
    std::copy(other.filter.cbegin(), other.filter.cend(), std::back_inserter(filter));
    all_match = all_match && other.all_match;
}

void BitmapFilter::runOptimize()
{
    all_match = std::find(filter.begin(), filter.end(), static_cast<UInt8>(false)) == filter.end();
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

size_t BitmapFilter::count() const
{
    return std::count(filter.cbegin(), filter.cend(), static_cast<UInt8>(true));
}
} // namespace DB::DM
