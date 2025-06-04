// Copyright 2025 PingCAP, Inc.
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

#include <Storages/DeltaMerge/Filter/ColumnRange_fwd.h>
#include <Storages/DeltaMerge/Filter/IntegerSet.h>
#include <Storages/KVStore/Types.h>

#include <magic_enum.hpp>

namespace DB::DM
{

using IndexID = DB::IndexID;
using ColumnID = DB::ColumnID;

enum class ColumnRangeType
{
    Unsupported,
    Single,
    And,
    Or,
};

// ColumnRange represents the range of values for columns.
class ColumnRange
{
public:
    explicit ColumnRange(ColumnRangeType type_)
        : type(type_)
    {}

    virtual ~ColumnRange() = default;

    virtual ColumnRangePtr invert() const = 0;

    virtual String toDebugString() = 0;

    // @return a bitmap filter that represents the result of the range check
    // @param search: a function to search the inverted index for a single column range
    // @param size: the size of the bitmap filter
    virtual BitmapFilterPtr check(std::function<BitmapFilterPtr(const SingleColumnRangePtr &)> search, size_t size) = 0;

public:
    ColumnRangeType type = ColumnRangeType::Unsupported;
};

class UnsupportedColumnRange : public ColumnRange
{
public:
    explicit UnsupportedColumnRange()
        : ColumnRange(ColumnRangeType::Unsupported)
    {}

    static const ColumnRangePtr Instance;

    static ColumnRangePtr create() { return Instance; }

    ColumnRangePtr invert() const override { return Instance; }

    String toDebugString() override { return String(magic_enum::enum_name(type)); }

    BitmapFilterPtr check(std::function<BitmapFilterPtr(const SingleColumnRangePtr &)>, size_t size) override
    {
        return std::make_shared<BitmapFilter>(size, true);
    }
};

class SingleColumnRange
    : public ColumnRange
    , public std::enable_shared_from_this<SingleColumnRange>
{
public:
    explicit SingleColumnRange(ColumnID column_id_, IndexID index_id_, const IntegerSetPtr & set_)
        : ColumnRange(ColumnRangeType::Single)
        , column_id(column_id_)
        , index_id(index_id_)
        , set(set_)
    {}

    static ColumnRangePtr create(ColumnID col_id, IndexID index_id, const IntegerSetPtr & set)
    {
        return std::make_shared<SingleColumnRange>(col_id, index_id, set);
    }

    ColumnRangePtr invert() const override
    {
        return std::make_shared<SingleColumnRange>(column_id, index_id, set->invert());
    }

    String toDebugString() override { return fmt::format("{}: {}", column_id, set->toDebugString()); }

    BitmapFilterPtr check(std::function<BitmapFilterPtr(const SingleColumnRangePtr &)> search, size_t) override
    {
        return search(shared_from_this());
    }

public:
    ColumnID column_id;
    IndexID index_id;
    IntegerSetPtr set;
};

class LogicalOpColumnRange
    : public ColumnRange
    , public std::enable_shared_from_this<LogicalOpColumnRange>
{
protected:
    explicit LogicalOpColumnRange(ColumnRangeType type, const ColumnRanges & children_)
        : ColumnRange(type)
        , children(children_)
    {}

public:
    virtual ColumnRangePtr tryOptimize() = 0;

    String toDebugString() override
    {
        FmtBuffer buf;
        buf.fmtAppend("{}[", magic_enum::enum_name(type));
        buf.joinStr(
            children.cbegin(),
            children.cend(),
            [](const auto & child, FmtBuffer & fb) { fb.append(child->toDebugString()); },
            ", ");
        buf.append("]");
        return buf.toString();
    }

protected:
    ColumnRanges children;
};

class AndColumnRange : public LogicalOpColumnRange
{
public:
    explicit AndColumnRange(const ColumnRanges & children_)
        : LogicalOpColumnRange(ColumnRangeType::And, children_)
    {}

    ~AndColumnRange() override = default;

    static ColumnRangePtr create(const ColumnRanges & children)
    {
        auto set = std::make_shared<AndColumnRange>(children);
        return set->tryOptimize();
    }

    ColumnRangePtr invert() const override;

    ColumnRangePtr tryOptimize() override;

    BitmapFilterPtr check(std::function<BitmapFilterPtr(const SingleColumnRangePtr &)> search, size_t size) override;
};

class OrColumnRange : public LogicalOpColumnRange
{
public:
    explicit OrColumnRange(const ColumnRanges & children_)
        : LogicalOpColumnRange(ColumnRangeType::Or, children_)
    {}

    ~OrColumnRange() override = default;

    static ColumnRangePtr create(const ColumnRanges & children)
    {
        auto set = std::make_shared<OrColumnRange>(children);
        return set->tryOptimize();
    }

    ColumnRangePtr invert() const override;

    ColumnRangePtr tryOptimize() override;

    BitmapFilterPtr check(std::function<BitmapFilterPtr(const SingleColumnRangePtr &)> search, size_t size) override;
};

} // namespace DB::DM
