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

namespace DB
{
template <typename T>
struct SingleValueDataFixedForWindow : public SingleValueDataFixed<T>
{
private:
    using Self = SingleValueDataFixedForWindow<T>;
    using ColumnType = std::conditional_t<IsDecimal<T>, ColumnDecimal<T>, ColumnVector<T>>;

    mutable std::deque<T> * saved_values;

public:
    SingleValueDataFixedForWindow()
        : saved_values(nullptr)
    {}

    ~SingleValueDataFixedForWindow() { delete saved_values; }

    void insertMaxResultInto(IColumn & to) const { insertMinOrMaxResultInto<false>(to); }

    void insertMinResultInto(IColumn & to) const { insertMinOrMaxResultInto<true>(to); }

    template <bool is_min>
    void insertMinOrMaxResultInto(IColumn & to) const
    {
        if (this->has())
        {
            auto size = saved_values->size();
            T tmp = (*saved_values)[0];
            for (size_t i = 1; i < size; i++)
            {
                if constexpr (is_min)
                {
                    if ((*saved_values)[i] < tmp)
                        tmp = (*saved_values)[i];
                }
                else
                {
                    if (tmp < (*saved_values)[i])
                        tmp = (*saved_values)[i];
                }
            }
            static_cast<ColumnType &>(to).getData().push_back(tmp);
        }
        else
        {
            static_cast<ColumnType &>(to).insertDefault();
        }
    }

    void prepareWindow() { saved_values = new std::deque<T>(); }

    void reset()
    {
        this->has_value = false;
        saved_values->clear();
    }

    void decrease()
    {
        saved_values->pop_front();
        if unlikely (saved_values->empty())
            this->has_value = false;
    }

    bool changeIfLess(const IColumn & column, size_t row_num, Arena * arena)
    {
        auto to_value = static_cast<const ColumnType &>(column).getData()[row_num];
        if (saved_values != nullptr)
            saved_values->push_back(to_value);

        return SingleValueDataFixed<T>::changeIfLess(column, row_num, arena);
    }

    bool changeIfLess(const Self & to, Arena * arena)
    {
        if (saved_values != nullptr)
            saved_values->push_back(to.value);

        return SingleValueDataFixed<T>::changeIfLess(to, arena);
    }

    bool changeIfGreater(const IColumn & column, size_t row_num, Arena * arena)
    {
        auto to_value = static_cast<const ColumnType &>(column).getData()[row_num];
        if (saved_values != nullptr)
            saved_values->push_back(to_value);

        return SingleValueDataFixed<T>::changeIfGreater(column, row_num, arena);
    }

    bool changeIfGreater(const Self & to, Arena * arena)
    {
        if (saved_values != nullptr)
            saved_values->push_back(to.value);

        return SingleValueDataFixed<T>::changeIfGreater(to, arena);
    }
};

struct SingleValueDataStringForWindow : public SingleValueDataString
{
private:
    using Self = SingleValueDataStringForWindow;

    // TODO use std::string is inefficient
    mutable std::deque<std::string> * saved_values{};

public:
    SingleValueDataStringForWindow()
        : saved_values(nullptr)
    {}
    ~SingleValueDataStringForWindow() { delete saved_values; }

    void insertMaxResultInto(IColumn & to) const { insertMinOrMaxResultInto<false>(to); }

    void insertMinResultInto(IColumn & to) const { insertMinOrMaxResultInto<true>(to); }

    template <bool is_min>
    void insertMinOrMaxResultInto(IColumn & to) const
    {
        if (has())
        {
            auto elem_num = saved_values->size();
            StringRef value((*saved_values)[0].c_str(), (*saved_values)[0].size());
            for (size_t i = 1; i < elem_num; i++)
            {
                String cmp_value((*saved_values)[i].c_str(), (*saved_values)[i].size());
                if constexpr (is_min)
                {
                    if (less(cmp_value, value))
                        value = (*saved_values)[i];
                }
                else
                {
                    if (less(value, cmp_value))
                        value = (*saved_values)[i];
                }
            }

            static_cast<ColumnString &>(to).insertDataWithTerminatingZero(value.data, value.size);
        }
        else
        {
            static_cast<ColumnString &>(to).insertDefault();
        }
    }

    void prepareWindow() { saved_values = new std::deque<std::string>(); }

    void reset()
    {
        size = -1;
        saved_values->clear();
    }

    void decrease()
    {
        saved_values->pop_front();
        if unlikely (saved_values->empty())
            size = -1;
    }

    void saveValue(StringRef value) { saved_values->push_back(value.toString()); }

    bool changeIfLess(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (saved_values != nullptr)
            saveValue(static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num));

        return SingleValueDataString::changeIfLess(column, row_num, arena);
    }

    bool changeIfLess(const Self & to, Arena * arena)
    {
        if (saved_values != nullptr)
            saveValue(to.getStringRef());

        return SingleValueDataString::changeIfLess(to, arena);
    }

    bool changeIfGreater(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (saved_values != nullptr)
            saveValue(static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num));


        return SingleValueDataString::changeIfGreater(column, row_num, arena);
    }

    bool changeIfGreater(const Self & to, Arena * arena)
    {
        if (saved_values != nullptr)
            saveValue(to.getStringRef());

        return SingleValueDataString::changeIfGreater(to, arena);
    }

    static bool allocatesMemoryInArena() { return true; }
};

struct SingleValueDataGenericForWindow : public SingleValueDataGeneric
{
private:
    using Self = SingleValueDataGenericForWindow;
    mutable std::deque<Field> * saved_values;

public:
    SingleValueDataGenericForWindow()
        : saved_values(nullptr)
    {}
    ~SingleValueDataGenericForWindow() { delete saved_values; }

    void insertMaxResultInto(IColumn & to) const { insertMinOrMaxResultInto<false>(to); }

    void insertMinResultInto(IColumn & to) const { insertMinOrMaxResultInto<true>(to); }

    template <bool is_min>
    void insertMinOrMaxResultInto(IColumn & to) const
    {
        if (has())
        {
            auto size = saved_values->size();
            Field tmp = (*saved_values)[0];
            for (size_t i = 1; i < size; i++)
            {
                if constexpr (is_min)
                {
                    if ((*saved_values)[i] < tmp)
                        tmp = (*saved_values)[i];
                }
                else
                {
                    if (tmp < (*saved_values)[i])
                        tmp = (*saved_values)[i];
                }
            }
            to.insert(tmp);
        }
        else
        {
            to.insertDefault();
        }
    }

    void prepareWindow() { saved_values = new std::deque<Field>(); }

    void reset()
    {
        value = Field();
        saved_values->clear();
    }

    // Only used for window aggregation
    void decrease()
    {
        saved_values->pop_front();
        if unlikely (saved_values->empty())
            value = Field();
    }

    bool changeIfLess(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has())
        {
            change(column, row_num, arena);

            if (saved_values != nullptr)
                saved_values->push_back(value);
            return true;
        }
        else
        {
            Field new_value;
            column.get(row_num, new_value);

            if (saved_values != nullptr)
                saved_values->push_back(new_value);

            if (new_value < value)
            {
                value = new_value;
                return true;
            }
            else
                return false;
        }
    }

    bool changeIfLess(const Self & to, Arena * arena)
    {
        if (saved_values != nullptr)
            saved_values->push_back(to.value);

        return SingleValueDataGeneric::changeIfLess(to, arena);
    }

    bool changeIfGreater(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has())
        {
            change(column, row_num, arena);

            if (saved_values != nullptr)
                saved_values->push_back(value);
            return true;
        }
        else
        {
            Field new_value;
            column.get(row_num, new_value);

            if (saved_values != nullptr)
                saved_values->push_back(new_value);

            if (new_value > value)
            {
                value = new_value;
                return true;
            }
            else
                return false;
        }
    }

    bool changeIfGreater(const Self & to, Arena * arena)
    {
        if (saved_values != nullptr)
            saved_values->push_back(to.value);

        return SingleValueDataGeneric::changeIfGreater(to, arena);
    }
};

template <typename Data>
struct AggregateFunctionMinDataForWindow : Data
{
    using Self = AggregateFunctionMinDataForWindow<Data>;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        return this->changeIfLess(column, row_num, arena);
    }
    bool changeIfBetter(const Self & to, Arena * arena) { return this->changeIfLess(to, arena); }

    void insertResultInto(IColumn & to) const { Data::insertMinResultInto(to); }

    static const char * name() { return "min_for_window"; }
};

template <typename Data>
struct AggregateFunctionMaxDataForWindow : Data
{
    using Self = AggregateFunctionMaxDataForWindow<Data>;

    void insertResultInto(IColumn & to) const { Data::insertMaxResultInto(to); }

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        return this->changeIfGreater(column, row_num, arena);
    }

    bool changeIfBetter(const Self & to, Arena * arena) { return this->changeIfGreater(to, arena); }

    static const char * name() { return "max_for_window"; }
};

} // namespace DB
