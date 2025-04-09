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

#include <AggregateFunctions/AggregateFunctionMinMaxAny.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <common/StringRef.h>

#include <cassert>
#include <set>

namespace DB
{
template <typename T>
struct SingleValueDataFixedForWindow
{
private:
    using Self = SingleValueDataFixedForWindow<T>;
    using ColumnType = std::conditional_t<IsDecimal<T>, ColumnDecimal<T>, ColumnVector<T>>;

    mutable std::multiset<T> saved_values;

    template <bool is_min>
    void insertMinOrMaxResultInto(IColumn & to) const
    {
        insertBatchMinOrMaxResultInto<is_min>(to, 1);
    }

    template <bool is_min>
    void insertBatchMinOrMaxResultInto(IColumn & to, size_t num) const
    {
        if (!saved_values.empty())
        {
            if constexpr (is_min)
            {
                const auto & iter = saved_values.begin();
                auto & container = static_cast<ColumnType &>(to).getData();
                container.resize_fill(num + container.size(), *iter);
            }
            else
            {
                const auto & iter = saved_values.rbegin();
                auto & container = static_cast<ColumnType &>(to).getData();
                container.resize_fill(num + container.size(), *iter);
            }
        }
        else
        {
            static_cast<ColumnType &>(to).insertManyDefaults(num);
        }
    }

public:
    static bool needArena() { return false; }

    void insertMaxResultInto(IColumn & to) const { insertMinOrMaxResultInto<false>(to); }

    void insertMinResultInto(IColumn & to) const { insertMinOrMaxResultInto<true>(to); }

    void insertBatchMaxResultInto(IColumn & to, size_t num) const { insertBatchMinOrMaxResultInto<false>(to, num); }

    void insertBatchMinResultInto(IColumn & to, size_t num) const { insertBatchMinOrMaxResultInto<true>(to, num); }

    void reset() { saved_values.clear(); }

    void decrease(const IColumn & column, size_t row_num)
    {
        auto value = static_cast<const ColumnType &>(column).getData()[row_num];
        auto iter = saved_values.find(value);
        assert(iter != saved_values.end());
        saved_values.erase(iter);
    }

    void add(const IColumn & column, size_t row_num, Arena *)
    {
        auto to_value = static_cast<const ColumnType &>(column).getData()[row_num];
        saved_values.insert(to_value);
    }

    static void setCollators(const TiDB::TiDBCollators &) {}
    static void write(WriteBuffer &, const IDataType &) { throw Exception("Not implemented yet"); }
    static void read(ReadBuffer &, const IDataType &, Arena *) { throw Exception("Not implemented yet"); }
};

struct SingleValueDataStringForWindow
{
private:
    using Self = SingleValueDataStringForWindow;

    struct StringWithCollator
    {
        StringWithCollator(const StringRef & value_, TiDB::TiDBCollatorPtr collator_)
            : value(value_)
            , collator(collator_)
        {}

        StringRef value;
        TiDB::TiDBCollatorPtr collator;
    };

    struct Less
    {
        constexpr bool operator()(const StringWithCollator & left, const StringWithCollator & right) const
        {
            if unlikely (left.collator == nullptr)
                return left.value < right.value;
            return left.collator->compareFastPath(left.value.data, left.value.size, right.value.data, right.value.size);
        }
    };

    using multiset = std::multiset<StringWithCollator, Less>;

    mutable multiset saved_values;
    TiDB::TiDBCollatorPtr collator{};

    void saveValue(const StringRef & value) { saved_values.insert(StringWithCollator(value, collator)); }

    template <bool is_min>
    void insertMinOrMaxResultInto(IColumn & to) const
    {
        insertBatchMinOrMaxResultInto<is_min>(to, 1);
    }

    template <bool is_min>
    void insertBatchMinOrMaxResultInto(IColumn & to, size_t num) const
    {
        if (!saved_values.empty())
        {
            if constexpr (is_min)
            {
                const auto & iter = saved_values.begin();
                const char * data = iter->value.data;
                size_t size = iter->value.size;
                for (size_t i = 0; i < num; ++i)
                    static_cast<ColumnString &>(to).insertDataWithTerminatingZero(data, size);
            }
            else
            {
                const auto & iter = saved_values.rbegin();
                const char * data = iter->value.data;
                size_t size = iter->value.size;
                for (size_t i = 0; i < num; ++i)
                    static_cast<ColumnString &>(to).insertDataWithTerminatingZero(data, size);
            }
        }
        else
        {
            static_cast<ColumnString &>(to).insertManyDefaults(num);
        }
    }

public:
    static bool needArena() { return false; }

    void insertMaxResultInto(IColumn & to) const { insertMinOrMaxResultInto<false>(to); }

    void insertMinResultInto(IColumn & to) const { insertMinOrMaxResultInto<true>(to); }

    void insertBatchMaxResultInto(IColumn & to, size_t num) const { insertBatchMinOrMaxResultInto<false>(to, num); }

    void insertBatchMinResultInto(IColumn & to, size_t num) const { insertBatchMinOrMaxResultInto<true>(to, num); }

    void reset() { saved_values.clear(); }

    void decrease(const IColumn & column, size_t row_num)
    {
        auto str = static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num);
        auto iter = saved_values.find(StringWithCollator(str, collator));
        assert(iter != saved_values.end());
        saved_values.erase(iter);
    }

    void add(const IColumn & column, size_t row_num, Arena *)
    {
        saveValue(static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num));
    }

    void setCollators(const TiDB::TiDBCollators & collators_)
    {
        collator = !collators_.empty() ? collators_[0] : nullptr;
    }

    static void write(WriteBuffer &, const IDataType &) { throw Exception("Not implemented yet"); }
    static void read(ReadBuffer &, const IDataType &, Arena *) { throw Exception("Not implemented yet"); }
};

struct SingleValueDataGenericForWindow
{
private:
    using Self = SingleValueDataGenericForWindow;
    mutable std::multiset<Field> saved_values;

    template <bool is_min>
    void insertMinOrMaxResultInto(IColumn & to) const
    {
        insertBatchMinOrMaxResultInto<is_min>(to, 1);
    }

    template <bool is_min>
    void insertBatchMinOrMaxResultInto(IColumn & to, size_t num) const
    {
        if (!saved_values.empty())
        {
            if constexpr (is_min)
            {
                const auto & iter = saved_values.begin();
                to.insertMany(*iter, num);
            }
            else
            {
                const auto & iter = saved_values.rbegin();
                to.insertMany(*iter, num);
            }
        }
        else
        {
            to.insertManyDefaults(num);
        }
    }

public:
    static bool needArena() { return false; }

    void insertMaxResultInto(IColumn & to) const { insertMinOrMaxResultInto<false>(to); }

    void insertMinResultInto(IColumn & to) const { insertMinOrMaxResultInto<true>(to); }

    void insertBatchMaxResultInto(IColumn & to, size_t num) const { insertBatchMinOrMaxResultInto<false>(to, num); }

    void insertBatchMinResultInto(IColumn & to, size_t num) const { insertBatchMinOrMaxResultInto<true>(to, num); }

    void reset() { saved_values.clear(); }

    void decrease(const IColumn & column, size_t row_num)
    {
        Field value;
        column.get(row_num, value);
        auto iter = saved_values.find(value);
        assert(iter != saved_values.end());
        saved_values.erase(iter);
    }

    void add(const IColumn & column, size_t row_num, Arena *)
    {
        Field value;
        column.get(row_num, value);
        saved_values.insert(value);
    }

    static void setCollators(const TiDB::TiDBCollators &) {}
    static void write(WriteBuffer &, const IDataType &) { throw Exception("Not implemented yet"); }
    static void read(ReadBuffer &, const IDataType &, Arena *) { throw Exception("Not implemented yet"); }
};

template <typename Data>
struct AggregateFunctionMinDataForWindow : Data
{
    using Self = AggregateFunctionMinDataForWindow<Data>;

    void changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        return this->add(column, row_num, arena);
    }

    void changeIfBetter(const Self &, Arena *) { throw Exception("Not implemented yet"); }

    void insertResultInto(IColumn & to) const { Data::insertMinResultInto(to); }

    void batchInsertSameResultInto(IColumn & to, size_t num) const { Data::insertBatchMinResultInto(to, num); }

    static const char * name() { return "min_for_window"; }
};

template <typename Data>
struct AggregateFunctionMaxDataForWindow : Data
{
    using Self = AggregateFunctionMaxDataForWindow<Data>;

    void changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        return this->add(column, row_num, arena);
    }

    void changeIfBetter(const Self &, Arena *) { throw Exception("Not implemented yet"); }

    void insertResultInto(IColumn & to) const { Data::insertMaxResultInto(to); }

    void batchInsertSameResultInto(IColumn & to, size_t num) const { Data::insertBatchMaxResultInto(to, num); }

    static const char * name() { return "max_for_window"; }
};

} // namespace DB
