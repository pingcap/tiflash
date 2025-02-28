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

#include <Core/Field.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/Index/InvertedIndex.h>
#include <common/types.h>

#include <memory>

namespace DB::DM
{

class IntegerSet;
using IntegerSetPtr = std::shared_ptr<IntegerSet>;

using Fields = std::vector<Field>;

enum class SetType : UInt8
{
    Empty,
    All,
    Value,
    Range,
    Composite,
};

// IntegerSet is a collection of integer values.
// It can be multiple values, a range of values, or a composite of multiple sets.
class IntegerSet : public std::enable_shared_from_this<IntegerSet>
{
public:
    explicit IntegerSet(SetType type_)
        : type(type_)
    {}

    virtual ~IntegerSet() = default;

    virtual SetType getType() const = 0;

    // intersect the other set with this set.
    // return the result set.
    virtual IntegerSetPtr intersectWith(const IntegerSetPtr & other) = 0;

    // union the other set with this set.
    // return the result set.
    virtual IntegerSetPtr unionWith(const IntegerSetPtr & other) = 0;

    // invert the set.
    // return the result set.
    virtual IntegerSetPtr invert() const = 0;

    // search the inverted index with the set.
    // return the bitmap filter.
    virtual BitmapFilterPtr search(InvertedIndexViewerPtr inverted_index, size_t size) = 0;

    // return a string representation of the set.
    // Only used in tests.
    virtual String toDebugString() = 0;

    static IntegerSetPtr createValueSet(TypeIndex type_index, const Fields & values);
    static IntegerSetPtr createLessRangeSet(TypeIndex type_index, Field max, bool not_included = true);
    static IntegerSetPtr createGreaterRangeSet(TypeIndex type_index, Field min, bool not_included = true);

protected:
    SetType type;
};

class EmptySet final : public IntegerSet
{
public:
    static IntegerSetPtr instance()
    {
        static IntegerSetPtr instance = std::make_shared<EmptySet>();
        return instance;
    }

    EmptySet()
        : IntegerSet(SetType::Empty)
    {}

    ~EmptySet() override = default;

    SetType getType() const override { return SetType::Empty; }

    IntegerSetPtr intersectWith(const IntegerSetPtr &) override { return instance(); }

    IntegerSetPtr unionWith(const IntegerSetPtr & other) override { return other; }

    IntegerSetPtr invert() const override;

    BitmapFilterPtr search(InvertedIndexViewerPtr, size_t size) override
    {
        return std::make_shared<BitmapFilter>(size, false);
    }

    String toDebugString() override { return "EMPTY"; }
};

class AllSet final : public IntegerSet
{
public:
    static IntegerSetPtr instance()
    {
        static IntegerSetPtr instance = std::make_shared<AllSet>();
        return instance;
    }

    AllSet()
        : IntegerSet(SetType::All)
    {}

    ~AllSet() override = default;

    SetType getType() const override { return SetType::All; }

    IntegerSetPtr intersectWith(const IntegerSetPtr & other) override { return other; }

    IntegerSetPtr unionWith(const IntegerSetPtr &) override { return instance(); }

    IntegerSetPtr invert() const override { return EmptySet::instance(); }

    BitmapFilterPtr search(InvertedIndexViewerPtr, size_t size) override
    {
        return std::make_shared<BitmapFilter>(size, true);
    }

    String toDebugString() override { return "ALL"; }
};

// {x1, x2, x3, ...}
template <typename T>
class ValueSet final : public IntegerSet
{
public:
    explicit ValueSet(std::set<T> values_)
        : IntegerSet(SetType::Value)
        , values(values_)
    {}

    ValueSet()
        : ValueSet(std::set<T>{})
    {}

    explicit ValueSet(const Fields & values_)
        : IntegerSet(SetType::Value)
    {
        for (const auto & value : values_)
            values.insert(value.get<T>());
    }

    ~ValueSet() override = default;

    SetType getType() const override { return SetType::Value; }

    IntegerSetPtr intersectWith(const IntegerSetPtr & other) override;

    static IntegerSetPtr intersectWithValueSet(const IntegerSetPtr & lhs, const IntegerSetPtr & rhs);
    static IntegerSetPtr intersectWithRangeSet(const IntegerSetPtr & range_set, const IntegerSetPtr & value_set);
    static IntegerSetPtr intersectWithCompositeSet(
        const IntegerSetPtr & composite_set,
        const IntegerSetPtr & value_set);

    IntegerSetPtr unionWith(const IntegerSetPtr & other) override;

    static IntegerSetPtr unionWithValueSet(const IntegerSetPtr & lhs, const IntegerSetPtr & rhs);
    static IntegerSetPtr unionWithRangeSet(const IntegerSetPtr & range_set, const IntegerSetPtr & value_set);
    static IntegerSetPtr unionWithCompositeSet(const IntegerSetPtr & composite_set, const IntegerSetPtr & value_set);

    IntegerSetPtr invert() const override;

    BitmapFilterPtr search(InvertedIndexViewerPtr inverted_index, size_t size) override;

    String toDebugString() override
    {
        FmtBuffer buf;
        buf.append("{");
        buf.joinStr(
            values.begin(),
            values.end(),
            [](const auto & value, FmtBuffer & fb) { fb.fmtAppend("{}", value); },
            ", ");
        buf.append("}");

        return buf.toString();
    }

private:
    std::set<T> values;
};

// [start, end]
template <typename T>
class RangeSet final : public IntegerSet
{
    friend class ValueSet<T>;

public:
    explicit RangeSet(T start_, T end_)
        : IntegerSet(SetType::Range)
        , start(start_)
        , end(end_)
    {}

    ~RangeSet() override = default;

    SetType getType() const override { return SetType::Range; }

    IntegerSetPtr intersectWith(const IntegerSetPtr & other) override;

    static IntegerSetPtr intersectWithRangeSet(const IntegerSetPtr & lhs, const IntegerSetPtr & rhs);
    static IntegerSetPtr intersectWithCompositeSet(
        const IntegerSetPtr & composite_set,
        const IntegerSetPtr & range_set);

    IntegerSetPtr unionWith(const IntegerSetPtr & other) override;

    static IntegerSetPtr unionWithRangeSet(const IntegerSetPtr & lhs, const IntegerSetPtr & rhs);
    static IntegerSetPtr unionWithCompositeSet(const IntegerSetPtr & composite_set, const IntegerSetPtr & range_set);

    IntegerSetPtr invert() const override;

    BitmapFilterPtr search(InvertedIndexViewerPtr inverted_index, size_t size) override;

    String toDebugString() override { return fmt::format("[{}, {}]", start, end); }

private:
    T start;
    T end;
};

// {x1, x2, x3, ...} U [start1, end1] U ...
template <typename T>
class CompositeSet : public IntegerSet
{
    friend class RangeSet<T>;
    friend class ValueSet<T>;

public:
    CompositeSet()
        : IntegerSet(SetType::Composite)
    {}

    explicit CompositeSet(std::vector<IntegerSetPtr> sets_)
        : IntegerSet(SetType::Composite)
        , sets(sets_)
    {}

    ~CompositeSet() override = default;

    SetType getType() const override { return SetType::Composite; }

    IntegerSetPtr intersectWith(const IntegerSetPtr & other) override;

    IntegerSetPtr unionWith(const IntegerSetPtr & other) override;

    IntegerSetPtr invert() const override;

    BitmapFilterPtr search(InvertedIndexViewerPtr inverted_index, size_t size) override;

    String toDebugString() override
    {
        FmtBuffer buf;
        buf.append("{");
        buf.joinStr(
            sets.begin(),
            sets.end(),
            [](const auto & set, FmtBuffer & fb) { fb.append(set->toDebugString()); },
            ", ");
        buf.append("}");

        return buf.toString();
    }

private:
    std::vector<IntegerSetPtr> sets;
};

} // namespace DB::DM
