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

#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <common/StringRef.h>

#include <deque>
#include <memory>


namespace DB
{
/** Aggregate functions that store one of passed values.
  * For example: min, max, any, anyLast.
  */

// TODO maybe we can create a new class to be inherited by SingleValueDataFixed, SingleValueDataString and SingleValueDataGeneric

/// For numeric values.
template <typename T>
struct SingleValueDataFixed
{
private:
    using Self = SingleValueDataFixed<T>;

    bool has_value
        = false; /// We need to remember if at least one value has been passed. This is necessary for AggregateFunctionIf.
    T value;

    // It's only used in window aggregation
    mutable std::deque<T> * saved_values;

    using ColumnType = std::conditional_t<IsDecimal<T>, ColumnDecimal<T>, ColumnVector<T>>;

public:
    ~SingleValueDataFixed() { delete saved_values; }

    bool has() const { return has_value; }

    void setCollators(const TiDB::TiDBCollators &) {}

    void insertResultInto(IColumn & to) const
    {
        if (has())
            static_cast<ColumnType &>(to).getData().push_back(value);
        else
            static_cast<ColumnType &>(to).insertDefault();
    }

    void write(WriteBuffer & buf, const IDataType & /*data_type*/) const
    {
        writeBinary(has(), buf);
        if (has())
            writeBinary(value, buf);
    }

    void read(ReadBuffer & buf, const IDataType & /*data_type*/, Arena *)
    {
        readBinary(has_value, buf);
        if (has())
            readBinary(value, buf);
    }

    void insertMaxResultInto(IColumn & to) const { insertMinOrMaxResultInto<false>(to); }

    void insertMinResultInto(IColumn & to) const { insertMinOrMaxResultInto<true>(to); }

    template <bool is_min>
    void insertMinOrMaxResultInto(IColumn & to) const
    {
        if (has())
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
        has_value = false;
        saved_values->clear();
    }

    // Only used for window aggregation
    void decrease()
    {
        saved_values->pop_front();
        if unlikely (saved_values->empty())
            has_value = false;
    }

    void change(const IColumn & column, size_t row_num, Arena *)
    {
        has_value = true;
        value = static_cast<const ColumnType &>(column).getData()[row_num];
    }

    /// Assuming to.has()
    void change(const Self & to, Arena *)
    {
        has_value = true;
        value = to.value;
    }

    bool changeFirstTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has())
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeFirstTime(const Self & to, Arena * arena)
    {
        if (!has() && to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeEveryTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        change(column, row_num, arena);
        return true;
    }

    bool changeEveryTime(const Self & to, Arena * arena)
    {
        if (to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfLess(const IColumn & column, size_t row_num, Arena * arena)
    {
        auto to_value = static_cast<const ColumnType &>(column).getData()[row_num];
        if (saved_values != nullptr)
            saved_values->push_back(to_value);

        if (!has() || to_value < value)
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfLess(const Self & to, Arena * arena)
    {
        if (saved_values != nullptr)
            saved_values->push_back(to.value);

        if (to.has() && (!has() || to.value < value))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfGreater(const IColumn & column, size_t row_num, Arena * arena)
    {
        auto to_value = static_cast<const ColumnType &>(column).getData()[row_num];
        if (saved_values != nullptr)
            saved_values->push_back(to_value);

        if (!has() || to_value > value)
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfGreater(const Self & to, Arena * arena)
    {
        if (saved_values != nullptr)
            saved_values->push_back(to.value);

        if (to.has() && (!has() || to.value > value))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool isEqualTo(const Self & to) const { return has() && to.value == value; }

    bool isEqualTo(const IColumn & column, size_t row_num) const
    {
        return has() && static_cast<const ColumnType &>(column).getData()[row_num] == value;
    }
};


/** For strings. Short strings are stored in the object itself, and long strings are allocated separately.
  * NOTE It could also be suitable for arrays of numbers.
  */
struct SingleValueDataString
{
private:
    using Self = SingleValueDataString;

    Int32 size = -1; /// -1 indicates that there is no value.
    Int32 capacity = 0; /// power of two or zero
    char * large_data{};
    TiDB::TiDBCollatorPtr collator{};

    // TODO use std::string is inefficient
    // It's only used in window aggregation
    mutable std::deque<std::string> * saved_values{};

    bool less(const StringRef & a, const StringRef & b) const
    {
        if (unlikely(collator == nullptr))
            return a < b;
        return collator->compareFastPath(a.data, a.size, b.data, b.size) < 0;
    }

    bool greater(const StringRef & a, const StringRef & b) const
    {
        if (unlikely(collator == nullptr))
            return a > b;
        return collator->compareFastPath(a.data, a.size, b.data, b.size) > 0;
    }

    bool equalTo(const StringRef & a, const StringRef & b) const
    {
        if (unlikely(collator == nullptr))
            return a == b;
        return collator->compareFastPath(a.data, a.size, b.data, b.size) == 0;
    }

public:
    static constexpr Int32 AUTOMATIC_STORAGE_SIZE = 64;
    static constexpr Int32 MAX_SMALL_STRING_SIZE = AUTOMATIC_STORAGE_SIZE - sizeof(size) - sizeof(capacity)
        - sizeof(large_data) - sizeof(TiDB::TiDBCollatorPtr) - sizeof(std::unique_ptr<std::deque<std::string>>);

private:
    char small_data[MAX_SMALL_STRING_SIZE]{}; /// Including the terminating zero.

public:
    ~SingleValueDataString() { delete saved_values; }

    bool has() const { return size >= 0; }

    const char * getData() const { return size <= MAX_SMALL_STRING_SIZE ? small_data : large_data; }

    StringRef getStringRef() const { return StringRef(getData(), size); }

    void insertResultInto(IColumn & to) const
    {
        if (has())
            static_cast<ColumnString &>(to).insertDataWithTerminatingZero(getData(), size);
        else
            static_cast<ColumnString &>(to).insertDefault();
    }

    void setCollators(const TiDB::TiDBCollators & collators_)
    {
        collator = !collators_.empty() ? collators_[0] : nullptr;
    }

    void write(WriteBuffer & buf, const IDataType & /*data_type*/) const
    {
        writeBinary(size, buf);
        writeBinary(collator == nullptr ? 0 : collator->getCollatorId(), buf);
        if (has())
            buf.write(getData(), size);
    }

    void read(ReadBuffer & buf, const IDataType & /*data_type*/, Arena * arena)
    {
        Int32 rhs_size;
        readBinary(rhs_size, buf);
        Int32 collator_id;
        readBinary(collator_id, buf);
        if (collator_id != 0)
            collator = TiDB::ITiDBCollator::getCollator(collator_id);
        else
            collator = nullptr;

        if (rhs_size >= 0)
        {
            if (rhs_size <= MAX_SMALL_STRING_SIZE)
            {
                /// Don't free large_data here.

                size = rhs_size;

                if (size > 0)
                    buf.read(small_data, size);
            }
            else
            {
                if (capacity < rhs_size)
                {
                    capacity = static_cast<UInt32>(roundUpToPowerOfTwoOrZero(rhs_size));
                    /// Don't free large_data here.
                    large_data = arena->alloc(capacity);
                }

                size = rhs_size;
                buf.read(large_data, size);
            }
        }
        else
        {
            /// Don't free large_data here.
            size = rhs_size;
        }
    }

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

    // Only used for window aggregation
    void decrease()
    {
        saved_values->pop_front();
        if unlikely (saved_values->empty())
            size = -1;
    }

    void saveValue(StringRef value) { saved_values->push_back(value.toString()); }

    /// Assuming to.has()
    void changeImpl(StringRef value, Arena * arena)
    {
        Int32 value_size = value.size;

        if (value_size <= MAX_SMALL_STRING_SIZE)
        {
            /// Don't free large_data here.
            size = value_size;

            if (size > 0)
                memcpy(small_data, value.data, size);
        }
        else
        {
            if (capacity < value_size)
            {
                /// Don't free large_data here.
                capacity = roundUpToPowerOfTwoOrZero(value_size);
                large_data = arena->alloc(capacity);
            }

            size = value_size;
            memcpy(large_data, value.data, size);
        }
    }

    void change(const IColumn & column, size_t row_num, Arena * arena)
    {
        changeImpl(static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num), arena);
    }

    void change(const Self & to, Arena * arena) { changeImpl(to.getStringRef(), arena); }

    bool changeFirstTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has())
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeFirstTime(const Self & to, Arena * arena)
    {
        if (!has() && to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeEveryTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        change(column, row_num, arena);
        return true;
    }

    bool changeEveryTime(const Self & to, Arena * arena)
    {
        if (to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfLess(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (saved_values != nullptr)
            saveValue(static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num));

        if (!has()
            || less(static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num), getStringRef()))
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfLess(const Self & to, Arena * arena)
    {
        if (saved_values != nullptr)
            saveValue(to.getStringRef());

        // todo should check the collator in `to` and `this`
        if (to.has() && (!has() || less(to.getStringRef(), getStringRef())))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfGreater(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (saved_values != nullptr)
            saveValue(static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num));

        if (!has()
            || greater(static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num), getStringRef()))
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeIfGreater(const Self & to, Arena * arena)
    {
        if (saved_values != nullptr)
            saveValue(to.getStringRef());

        if (to.has() && (!has() || greater(to.getStringRef(), getStringRef())))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool isEqualTo(const Self & to) const { return has() && equalTo(to.getStringRef(), getStringRef()); }

    bool isEqualTo(const IColumn & column, size_t row_num) const
    {
        return has()
            && equalTo(static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num), getStringRef());
    }
};

static_assert(
    sizeof(SingleValueDataString) == SingleValueDataString::AUTOMATIC_STORAGE_SIZE,
    "Incorrect size of SingleValueDataString struct");


/// For any other value types.
struct SingleValueDataGeneric
{
private:
    using Self = SingleValueDataGeneric;

    Field value;

    // It's only used in window aggregation
    mutable std::deque<Field> * saved_values;

public:
    ~SingleValueDataGeneric() { delete saved_values; }

    bool has() const { return !value.isNull(); }

    void setCollators(const TiDB::TiDBCollators &) {}

    void insertResultInto(IColumn & to) const
    {
        if (has())
            to.insert(value);
        else
            to.insertDefault();
    }

    void write(WriteBuffer & buf, const IDataType & data_type) const
    {
        if (!value.isNull())
        {
            writeBinary(true, buf);
            data_type.serializeBinary(value, buf);
        }
        else
            writeBinary(false, buf);
    }

    void read(ReadBuffer & buf, const IDataType & data_type, Arena *)
    {
        bool is_not_null;
        readBinary(is_not_null, buf);

        if (is_not_null)
            data_type.deserializeBinary(value, buf);
    }

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

    void change(const IColumn & column, size_t row_num, Arena *) { column.get(row_num, value); }

    void change(const Self & to, Arena *) { value = to.value; }

    bool changeFirstTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has())
        {
            change(column, row_num, arena);
            return true;
        }
        else
            return false;
    }

    bool changeFirstTime(const Self & to, Arena * arena)
    {
        if (!has() && to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool changeEveryTime(const IColumn & column, size_t row_num, Arena * arena)
    {
        change(column, row_num, arena);
        return true;
    }

    bool changeEveryTime(const Self & to, Arena * arena)
    {
        if (to.has())
        {
            change(to, arena);
            return true;
        }
        else
            return false;
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

        if (to.has() && (!has() || to.value < value))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
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

        if (to.has() && (!has() || to.value > value))
        {
            change(to, arena);
            return true;
        }
        else
            return false;
    }

    bool isEqualTo(const IColumn & column, size_t row_num) const { return has() && value == column[row_num]; }

    bool isEqualTo(const Self & to) const { return has() && to.value == value; }
};


/** What is the difference between the aggregate functions min, max, any, anyLast
  *  (the condition that the stored value is replaced by a new one,
  *   as well as, of course, the name).
  */

template <typename Data>
struct AggregateFunctionMinData : Data
{
    using Self = AggregateFunctionMinData<Data>;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        return this->changeIfLess(column, row_num, arena);
    }
    bool changeIfBetter(const Self & to, Arena * arena) { return this->changeIfLess(to, arena); }

    void prepareWindow()
    {
        is_in_window = true;
        Data::prepareWindow();
    }

    void insertResultInto(IColumn & to) const
    {
        if (is_in_window)
            Data::insertMinResultInto(to);
        else
            Data::insertResultInto(to);
    }

    static const char * name() { return "min"; }

    bool is_in_window = false;
};

template <typename Data>
struct AggregateFunctionMaxData : Data
{
    using Self = AggregateFunctionMaxData<Data>;

    void prepareWindow()
    {
        is_in_window = true;
        Data::prepareWindow();
    }

    void insertResultInto(IColumn & to) const
    {
        if (is_in_window)
            Data::insertMaxResultInto(to);
        else
            Data::insertResultInto(to);
    }

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        return this->changeIfGreater(column, row_num, arena);
    }
    bool changeIfBetter(const Self & to, Arena * arena) { return this->changeIfGreater(to, arena); }

    static const char * name() { return "max"; }

    bool is_in_window = false;
};

template <typename Data>
struct AggregateFunctionAnyData : Data
{
    using Self = AggregateFunctionAnyData<Data>;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        return this->changeFirstTime(column, row_num, arena);
    }
    bool changeIfBetter(const Self & to, Arena * arena) { return this->changeFirstTime(to, arena); }

    static const char * name() { return "any"; }
};

template <typename Data>
struct AggregateFunctionFirstRowData : Data
{
    using Self = AggregateFunctionFirstRowData<Data>;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        return this->changeFirstTime(column, row_num, arena);
    }
    bool changeIfBetter(const Self & to, Arena * arena) { return this->changeFirstTime(to, arena); }

    static const char * name() { return "first_row"; }
};

template <typename Data>
struct AggregateFunctionAnyLastData : Data
{
    using Self = AggregateFunctionAnyLastData<Data>;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        return this->changeEveryTime(column, row_num, arena);
    }
    bool changeIfBetter(const Self & to, Arena * arena) { return this->changeEveryTime(to, arena); }

    static const char * name() { return "anyLast"; }
};


/** Implement 'heavy hitters' algorithm.
  * Selects most frequent value if its frequency is more than 50% in each thread of execution.
  * Otherwise, selects some arbitary value.
  * http://www.cs.umd.edu/~samir/498/karp.pdf
  */
template <typename Data>
struct AggregateFunctionAnyHeavyData : Data
{
    size_t counter = 0;

    using Self = AggregateFunctionAnyHeavyData<Data>;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (this->isEqualTo(column, row_num))
        {
            ++counter;
        }
        else
        {
            if (counter == 0)
            {
                this->change(column, row_num, arena);
                ++counter;
                return true;
            }
            else
                --counter;
        }
        return false;
    }

    bool changeIfBetter(const Self & to, Arena * arena)
    {
        if (this->isEqualTo(to))
        {
            counter += to.counter;
        }
        else
        {
            if (counter < to.counter)
            {
                this->change(to, arena);
                return true;
            }
            else
                counter -= to.counter;
        }
        return false;
    }

    void write(WriteBuffer & buf, const IDataType & data_type) const
    {
        Data::write(buf, data_type);
        writeBinary(counter, buf);
    }

    void read(ReadBuffer & buf, const IDataType & data_type, Arena * arena)
    {
        Data::read(buf, data_type, arena);
        readBinary(counter, buf);
    }

    static const char * name() { return "anyHeavy"; }
};


template <typename Data>
class AggregateFunctionsSingleValue final
    : public IAggregateFunctionDataHelper<Data, AggregateFunctionsSingleValue<Data>, true>
{
private:
    DataTypePtr type;

public:
    explicit AggregateFunctionsSingleValue(const DataTypePtr & type)
        : type(type)
    {
        if (StringRef(Data::name()) == StringRef("min") || StringRef(Data::name()) == StringRef("max"))
        {
            if (!type->isComparable())
                throw Exception(
                    "Illegal type " + type->getName() + " of argument of aggregate function " + getName()
                        + " because the values of that data type are not comparable",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    String getName() const override { return Data::name(); }

    DataTypePtr getReturnType() const override { return type; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).changeIfBetter(*columns[0], row_num, arena);
    }

    void decrease(AggregateDataPtr __restrict place, const IColumn **, size_t, Arena *) const override
    {
        this->data(place).decrease();
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void prepareWindow(AggregateDataPtr __restrict place) const override { this->data(place).prepareWindow(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).changeIfBetter(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf, *type.get());
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        this->data(place).read(buf, *type.get(), arena);
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

} // namespace DB
