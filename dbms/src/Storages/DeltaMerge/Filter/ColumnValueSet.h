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

#include <Storages/DeltaMerge/Filter/IntegerSet.h>
#include <Storages/KVStore/Types.h>

#include <magic_enum.hpp>

namespace DB::DM
{

using IndexID = DB::IndexID;
using ColumnID = DB::ColumnID;

class ColumnValueSet;
using ColumnValueSetPtr = std::shared_ptr<ColumnValueSet>;
using ColumnValueSets = std::vector<ColumnValueSetPtr>;

class SingleColumnValueSet;
using SingleColumnValueSetPtr = std::shared_ptr<SingleColumnValueSet>;

enum class ColumnValueSetType
{
    Unsupported,
    Single,
    And,
    Or,
};

class ColumnValueSet : public std::enable_shared_from_this<ColumnValueSet>
{
public:
    explicit ColumnValueSet(ColumnValueSetType type_)
        : type(type_)
    {}

    virtual ~ColumnValueSet() = default;

    virtual ColumnValueSetPtr invert() const = 0;

    virtual String toDebugString() = 0;

    virtual BitmapFilterPtr check(std::function<BitmapFilterPtr(const ColumnValueSetPtr &, size_t)> search, size_t size)
        = 0;

public:
    ColumnValueSetType type = ColumnValueSetType::Unsupported;
};

class UnsupportedColumnValueSet : public ColumnValueSet
{
public:
    static const ColumnValueSetPtr Instance;

    explicit UnsupportedColumnValueSet()
        : ColumnValueSet(ColumnValueSetType::Unsupported)
    {}

    static ColumnValueSetPtr create() { return Instance; }

    ColumnValueSetPtr invert() const override { return Instance; }

    String toDebugString() override { return String(magic_enum::enum_name(type)); }

    BitmapFilterPtr check(std::function<BitmapFilterPtr(const ColumnValueSetPtr &, size_t size)>, size_t size) override
    {
        return std::make_shared<BitmapFilter>(size, true);
    }
};

class SingleColumnValueSet : public ColumnValueSet
{
public:
    explicit SingleColumnValueSet(ColumnID column_id_, IndexID index_id_, const IntegerSetPtr & set_)
        : ColumnValueSet(ColumnValueSetType::Single)
        , column_id(column_id_)
        , index_id(index_id_)
        , set(set_)
    {}

    static ColumnValueSetPtr create(ColumnID col_id, IndexID index_id, const IntegerSetPtr & set)
    {
        return std::make_shared<SingleColumnValueSet>(col_id, index_id, set);
    }

    ColumnValueSetPtr invert() const override
    {
        return std::make_shared<SingleColumnValueSet>(column_id, index_id, set->invert());
    }

    String toDebugString() override { return fmt::format("{}: {}", column_id, set->toDebugString()); }

    BitmapFilterPtr check(std::function<BitmapFilterPtr(const ColumnValueSetPtr &, size_t size)> search, size_t size)
        override
    {
        return search(shared_from_this(), size);
    }

public:
    ColumnID column_id;
    IndexID index_id;
    IntegerSetPtr set;
};

class LogicalOpColumnValueSet : public ColumnValueSet
{
public:
    explicit LogicalOpColumnValueSet(ColumnValueSetType type, const ColumnValueSets & children_)
        : ColumnValueSet(type)
        , children(children_)
    {}

    virtual ColumnValueSetPtr tryOptimize() = 0;

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
    ColumnValueSets children;
};

class AndColumnValueSet : public LogicalOpColumnValueSet
{
public:
    ~AndColumnValueSet() override = default;

    explicit AndColumnValueSet(const ColumnValueSets & children_)
        : LogicalOpColumnValueSet(ColumnValueSetType::And, children_)
    {}

    static ColumnValueSetPtr create(const ColumnValueSets & children)
    {
        auto set = std::make_shared<AndColumnValueSet>(children);
        return set->tryOptimize();
    }

    ColumnValueSetPtr invert() const override;

    ColumnValueSetPtr tryOptimize() override;

    BitmapFilterPtr check(std::function<BitmapFilterPtr(const ColumnValueSetPtr &, size_t size)> search, size_t size)
        override;
};

class OrColumnValueSet : public LogicalOpColumnValueSet
{
public:
    ~OrColumnValueSet() override = default;

    explicit OrColumnValueSet(const ColumnValueSets & children_)
        : LogicalOpColumnValueSet(ColumnValueSetType::Or, children_)
    {}

    static ColumnValueSetPtr create(const ColumnValueSets & children)
    {
        auto set = std::make_shared<OrColumnValueSet>(children);
        return set->tryOptimize();
    }

    ColumnValueSetPtr invert() const override;

    ColumnValueSetPtr tryOptimize() override;

    BitmapFilterPtr check(std::function<BitmapFilterPtr(const ColumnValueSetPtr &, size_t size)> search, size_t size)
        override;
};

} // namespace DB::DM
