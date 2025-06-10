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
#include <deque>

namespace DB
{
template <typename T>
struct SingleValueDataFixedForWindow
{
private:
    using Self = SingleValueDataFixedForWindow<T>;
    using ColumnType = std::conditional_t<IsDecimal<T>, ColumnDecimal<T>, ColumnVector<T>>;

    mutable std::deque<T> queue;

public:
    static bool needArena() { return false; }

    void reset() { queue.clear(); }

    void insertResultInto(IColumn & to) const
    {
        if likely (!queue.empty())
            static_cast<ColumnType &>(to).getData().push_back(queue.front());
        else
            static_cast<ColumnType &>(to).insertDefault();
    }

    void insertBatchResultInto(IColumn & to, size_t num) const
    {
        if (!queue.empty())
        {
            auto & container = static_cast<ColumnType &>(to).getData();
            container.resize_fill(num + container.size(), queue.front());
        }
        else
            static_cast<ColumnType &>(to).insertManyDefaults(num);
    }

    void decrease(const IColumn & column, size_t row_num)
    {
        assert(!queue.empty());

        auto value = static_cast<const ColumnType &>(column).getData()[row_num];
        if (queue.front() == value)
            queue.pop_front();
    }

    template <bool is_min>
    void add(const IColumn & column, size_t row_num, Arena *) const
    {
        auto to_value = static_cast<const ColumnType &>(column).getData()[row_num];
        if constexpr (is_min)
        {
            while (!queue.empty() && to_value < queue.back())
                queue.pop_back();
        }
        else
        {
            while (!queue.empty() && queue.back() < to_value)
                queue.pop_back();
        }
        queue.push_back(to_value);
    }

    static void setCollators(const TiDB::TiDBCollators &) {}
    static void write(WriteBuffer &, const IDataType &) { throw Exception("Not implemented yet"); }
    static void read(ReadBuffer &, const IDataType &, Arena *) { throw Exception("Not implemented yet"); }
};

struct SingleValueDataStringForWindow
{
private:
    using Self = SingleValueDataStringForWindow;

    mutable std::deque<StringRef> queue;
    TiDB::TiDBCollatorPtr collator{};

public:
    static bool needArena() { return false; }

    void reset() { queue.clear(); }

    void insertResultInto(IColumn & to) const
    {
        if likely (!queue.empty())
            static_cast<ColumnString &>(to).insertDataWithTerminatingZero(queue.front().data, queue.front().size);
        else
            static_cast<ColumnString &>(to).insertDefault();
    }

    void insertBatchResultInto(IColumn & to, size_t num) const
    {
        if (!queue.empty())
            static_cast<ColumnString &>(to).batchInsertDataWithTerminatingZero(
                num,
                queue.front().data,
                queue.front().size);
        else
            static_cast<ColumnString &>(to).insertManyDefaults(num);
    }

    void decrease(const IColumn & column, size_t row_num)
    {
        assert(!queue.empty());

        auto str = static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num);
        if (str.compare(queue.front()) == 0)
            queue.pop_front();
    }

    template <bool is_min>
    void add(const IColumn & column, size_t row_num, Arena *) const
    {
        const StringRef & str = static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num);
        if (collator != nullptr)
        {
            if constexpr (is_min)
                while (!queue.empty()
                       && collator->compareFastPath(str.data, str.size, queue.back().data, queue.back().size) < 0)
                    queue.pop_back();
            else
                while (!queue.empty()
                       && collator->compareFastPath(str.data, str.size, queue.back().data, queue.back().size) > 0)
                    queue.pop_back();
        }
        else
        {
            if constexpr (is_min)
                while (!queue.empty() && str.compare(queue.back()) < 0)
                    queue.pop_back();
            else
                while (!queue.empty() && str.compare(queue.back()) > 0)
                    queue.pop_back();
        }

        queue.push_back(str);
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
    mutable std::deque<Field> queue;

public:
    static bool needArena() { return false; }

    void reset() { queue.clear(); }

    void insertResultInto(IColumn & to) const
    {
        if likely (!queue.empty())
            to.insert(queue.front());
        else
            to.insertDefault();
    }

    void insertBatchResultInto(IColumn & to, size_t num) const
    {
        if (!queue.empty())
            to.insertMany(queue.front(), num);
        else
            to.insertManyDefaults(num);
    }

    void decrease(const IColumn & column, size_t row_num)
    {
        assert(!queue.empty());

        Field value;
        column.get(row_num, value);
        if (value == queue.front())
            queue.pop_front();
    }

    template <bool is_min>
    void add(const IColumn & column, size_t row_num, Arena *) const
    {
        Field value;
        column.get(row_num, value);
        if constexpr (is_min)
            while (!queue.empty() && value < queue.back())
                queue.pop_back();
        else
            while (!queue.empty() && queue.back() < value)
                queue.pop_back();
        queue.push_back(value);
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
        return this->template add<true>(column, row_num, arena);
    }

    void changeIfBetter(const Self &, Arena *) { throw Exception("Not implemented yet"); }

    void insertResultInto(IColumn & to) const { Data::insertResultInto(to); }

    void batchInsertSameResultInto(IColumn & to, size_t num) const { Data::insertBatchResultInto(to, num); }

    static const char * name() { return "min_for_window"; }
};

template <typename Data>
struct AggregateFunctionMaxDataForWindow : Data
{
    using Self = AggregateFunctionMaxDataForWindow<Data>;

    void changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        return this->template add<false>(column, row_num, arena);
    }

    void changeIfBetter(const Self &, Arena *) { throw Exception("Not implemented yet"); }

    void insertResultInto(IColumn & to) const { Data::insertResultInto(to); }

    void batchInsertSameResultInto(IColumn & to, size_t num) const { Data::insertBatchResultInto(to, num); }

    static const char * name() { return "max_for_window"; }
};

} // namespace DB
