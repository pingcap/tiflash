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

#pragma once

#include <Columns/IColumn.h>

#include <roaring.hh>

namespace DB::DM
{
struct SegmentSnapshot;
using SegmentSnapshotPtr = std::shared_ptr<SegmentSnapshot>;

class ArrayBitmapFilter;
class RoaringBitmapFilter;
using ArrayBitmapFilterPtr = std::shared_ptr<ArrayBitmapFilter>;
using RoaringBitmapFilterPtr = std::shared_ptr<RoaringBitmapFilter>;

class ArrayBitmapFilter
{
public:
    ArrayBitmapFilter(UInt32 size_, const SegmentSnapshotPtr & snapshot_, const std::vector<UInt32> & data = {});
    explicit ArrayBitmapFilter(const SegmentSnapshotPtr & snapshot_);
    void set(const ColumnPtr & col);
    void get(IColumn::Filter & f, UInt32 start, UInt32 limit) const;
    SegmentSnapshotPtr snapshot() const;
    void runOptimize();
    RoaringBitmapFilterPtr toRoaringBitmapFilter() const;

private:
    void set(const UInt32 * data, UInt32 size);
    std::vector<UInt32> toUInt32Array() const;

    IColumn::Filter filter;
    SegmentSnapshotPtr snap;
    bool all_match;
};

class RoaringBitmapFilter
{
public:
    RoaringBitmapFilter(UInt32 size_, const SegmentSnapshotPtr & snapshot_, const std::vector<UInt32> & data = {});
    explicit RoaringBitmapFilter(const SegmentSnapshotPtr & snapshot_);
    void set(const ColumnPtr & col);
    void get(IColumn::Filter & f, UInt32 start, UInt32 limit) const;
    SegmentSnapshotPtr snapshot() const;
    void runOptimize();
    ArrayBitmapFilterPtr toArrayBitmapFilter() const;

private:
    UInt32 sz;
    roaring::Roaring rrbitmap;
    SegmentSnapshotPtr snap;
    bool all_match;
};
} // namespace DB::DM