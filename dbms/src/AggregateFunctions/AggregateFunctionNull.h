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
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/countBytesInFilter.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/SetVariants.h>
#include <Interpreters/sortBlock.h>
#include <common/mem_utils.h>

#include <array>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes


/// This class implements a wrapper around an aggregate function. Despite its name,
/// this is an adapter. It is used to handle aggregate functions that are called with
/// at least one nullable argument. It implements the logic according to which any
/// row that contains at least one NULL is skipped.

/// If all rows had NULL, the behaviour is determined by "result_is_nullable" template parameter.
///  true - return NULL; false - return value from empty aggregation state of nested function.

template <bool result_is_nullable, typename Derived>
class AggregateFunctionNullBase : public IAggregateFunctionHelper<Derived>
{
protected:
    AggregateFunctionPtr nested_function;
    size_t prefix_size;

    /** In addition to data for nested aggregate function, we keep a flag
      *  indicating - was there at least one non-NULL value accumulated.
      * In case of no not-NULL values, the function will return NULL.
      *
      * We use prefix_size bytes for flag to satisfy the alignment requirement of nested state.
      */

    AggregateDataPtr nestedPlace(AggregateDataPtr __restrict place) const noexcept { return place + prefix_size; }

    ConstAggregateDataPtr nestedPlace(ConstAggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    static void initFlag(AggregateDataPtr __restrict place) noexcept
    {
        if (result_is_nullable)
            place[0] = 0;
    }

    static void setFlag(AggregateDataPtr __restrict place) noexcept
    {
        if (result_is_nullable)
            place[0] = 1;
    }

    static bool getFlag(ConstAggregateDataPtr __restrict place) noexcept
    {
        return result_is_nullable ? place[0] : true;
    }

public:
    explicit AggregateFunctionNullBase(AggregateFunctionPtr nested_function_, bool need_counter = false)
        : nested_function{nested_function_}
    {
        if (result_is_nullable)
            prefix_size = nested_function->alignOfData();
        else
            prefix_size = 0;

        if (result_is_nullable && need_counter)
        {
            auto tmp = prefix_size;
            prefix_size += sizeof(Int64);
            if ((prefix_size % tmp) != 0)
                prefix_size += tmp - (prefix_size % tmp);
            assert((prefix_size % tmp) == 0);
            assert(prefix_size >= (tmp + sizeof(Int64)));
        }
    }

    String getName() const override
    {
        /// This is just a wrapper. The function for Nullable arguments is named the same as the nested function itself.
        return nested_function->getName();
    }

    void setCollators(TiDB::TiDBCollators & collators) override { nested_function->setCollators(collators); }

    DataTypePtr getReturnType() const override
    {
        return result_is_nullable ? makeNullable(nested_function->getReturnType()) : nested_function->getReturnType();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        initFlag(place);
        nested_function->create(nestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_function->destroy(nestedPlace(place));
    }

    bool hasTrivialDestructor() const override { return nested_function->hasTrivialDestructor(); }

    size_t sizeOfData() const override { return prefix_size + nested_function->sizeOfData(); }

    size_t alignOfData() const override { return nested_function->alignOfData(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if (result_is_nullable && getFlag(rhs))
            setFlag(place);

        nested_function->merge(nestedPlace(place), nestedPlace(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        bool flag = getFlag(place);
        if (result_is_nullable)
            writeBinary(flag, buf);
        if (flag)
            nested_function->serialize(nestedPlace(place), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        bool flag = true;
        if (result_is_nullable)
            readBinary(flag, buf);
        if (flag)
        {
            setFlag(place);
            nested_function->deserialize(nestedPlace(place), buf, arena);
        }
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        if constexpr (result_is_nullable)
        {
            auto & to_concrete = static_cast<ColumnNullable &>(to);
            if (getFlag(place))
            {
                nested_function->insertResultInto(nestedPlace(place), to_concrete.getNestedColumn(), arena);
                to_concrete.getNullMapData().push_back(0);
            }
            else
            {
                to_concrete.insertDefault();
            }
        }
        else
        {
            nested_function->insertResultInto(nestedPlace(place), to, arena);
        }
    }

    bool allocatesMemoryInArena() const override { return nested_function->allocatesMemoryInArena(); }

    bool isState() const override { return nested_function->isState(); }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

template <bool result_is_nullable, bool input_is_nullable>
class AggregateFunctionFirstRowNull
    : public IAggregateFunctionHelper<AggregateFunctionFirstRowNull<result_is_nullable, input_is_nullable>>
{
protected:
    AggregateFunctionPtr nested_function;
    size_t prefix_size;

    /** In addition to data for nested aggregate function, we keep a flag
      *  indicating - was there at least one non-NULL value accumulated.
      * In case of no not-NULL values, the function will return NULL.
      *
      * We use prefix_size bytes for flag to satisfy the alignment requirement of nested state.
      */

    AggregateDataPtr nestedPlace(AggregateDataPtr __restrict place) const noexcept { return place + prefix_size; }

    ConstAggregateDataPtr nestedPlace(ConstAggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    static void initFlag(AggregateDataPtr __restrict place) noexcept
    {
        if (result_is_nullable)
            place[0] = 0;
    }

    static void setFlag(AggregateDataPtr __restrict place, UInt8 status) noexcept
    {
        if (result_is_nullable)
            place[0] = status;
    }

    /// 0 means there is no input yet
    /// 1 meas there is a not-null input
    /// 2 means there is a null input
    static UInt8 getFlag(ConstAggregateDataPtr __restrict place) noexcept { return result_is_nullable ? place[0] : 1; }

public:
    explicit AggregateFunctionFirstRowNull(AggregateFunctionPtr nested_function_)
        : nested_function{nested_function_}
    {
        if (result_is_nullable)
            prefix_size = nested_function->alignOfData();
        else
            prefix_size = 0;
    }

    String getName() const override
    {
        /// This is just a wrapper. The function for Nullable arguments is named the same as the nested function itself.
        return nested_function->getName();
    }

    void setCollators(TiDB::TiDBCollators & collators) override { nested_function->setCollators(collators); }

    DataTypePtr getReturnType() const override
    {
        return result_is_nullable ? makeNullable(nested_function->getReturnType()) : nested_function->getReturnType();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        initFlag(place);
        nested_function->create(nestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_function->destroy(nestedPlace(place));
    }

    bool hasTrivialDestructor() const override { return nested_function->hasTrivialDestructor(); }

    size_t sizeOfData() const override { return prefix_size + nested_function->sizeOfData(); }

    size_t alignOfData() const override { return nested_function->alignOfData(); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if constexpr (input_is_nullable)
        {
            if (this->getFlag(place) == 0)
            {
                const auto * column = static_cast<const ColumnNullable *>(columns[0]);
                bool is_null = column->isNullAt(row_num);
                this->setFlag(place, is_null ? 2 : 1);
                if (!is_null)
                {
                    const IColumn * nested_column = &column->getNestedColumn();
                    this->nested_function->add(this->nestedPlace(place), &nested_column, row_num, arena);
                }
            }
        }
        else
        {
            this->setFlag(place, 1);
            this->nested_function->add(this->nestedPlace(place), columns, row_num, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if constexpr (result_is_nullable)
        {
            if (getFlag(place) == 0)
            {
                if (getFlag(rhs) > 0)
                {
                    setFlag(place, getFlag(rhs));
                }
                if (getFlag(rhs) == 1)
                    nested_function->merge(nestedPlace(place), nestedPlace(rhs), arena);
            }
        }
        else
        {
            nested_function->merge(nestedPlace(place), nestedPlace(rhs), arena);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        UInt8 flag = getFlag(place);
        if (result_is_nullable)
            writeBinary(flag, buf);
        if (flag == 1)
            nested_function->serialize(nestedPlace(place), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        UInt8 flag = 1;
        if (result_is_nullable)
            readBinary(flag, buf);
        if (flag == 1)
        {
            setFlag(place, 1);
            nested_function->deserialize(nestedPlace(place), buf, arena);
        }
        else if (flag == 2)
        {
            setFlag(place, 2);
        }
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        if constexpr (result_is_nullable)
        {
            auto & to_concrete = static_cast<ColumnNullable &>(to);
            UInt8 flag = getFlag(place);
            if (flag == 1)
            {
                nested_function->insertResultInto(nestedPlace(place), to_concrete.getNestedColumn(), arena);
                to_concrete.getNullMapData().push_back(0);
            }
            else
            {
                to_concrete.insertDefault();
            }
        }
        else
        {
            nested_function->insertResultInto(nestedPlace(place), to, arena);
        }
    }

    bool allocatesMemoryInArena() const override { return nested_function->allocatesMemoryInArena(); }

    bool isState() const override { return nested_function->isState(); }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

/** There are two cases: for single argument and variadic.
  * Code for single argument is much more efficient.
  */
template <bool result_is_nullable, bool input_is_nullable>
class AggregateFunctionNullUnary final
    : public AggregateFunctionNullBase<
          result_is_nullable,
          AggregateFunctionNullUnary<result_is_nullable, input_is_nullable>>
{
public:
    explicit AggregateFunctionNullUnary(AggregateFunctionPtr nested_function)
        : AggregateFunctionNullBase<
            result_is_nullable,
            AggregateFunctionNullUnary<result_is_nullable, input_is_nullable>>(nested_function)
    {}

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if constexpr (input_is_nullable)
        {
            const auto * column = static_cast<const ColumnNullable *>(columns[0]);
            if (!column->isNullAt(row_num))
            {
                this->setFlag(place);
                const IColumn * nested_column = &column->getNestedColumn();
                this->nested_function->add(this->nestedPlace(place), &nested_column, row_num, arena);
            }
        }
        else
        {
            this->setFlag(place);
            this->nested_function->add(this->nestedPlace(place), columns, row_num, arena);
        }
    }

    void addBatchSinglePlace( // NOLINT(google-default-arguments)
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        if (batch_size == 0)
            return;

        if constexpr (input_is_nullable)
        {
            const auto * column = assert_cast<const ColumnNullable *>(columns[0]);
            const IColumn * nested_column = &column->getNestedColumn();
            const UInt8 * null_map = column->getNullMapData().data();

            this->nested_function->addBatchSinglePlaceNotNull(
                start_offset,
                batch_size,
                this->nestedPlace(place),
                &nested_column,
                null_map,
                arena,
                if_argument_pos);

            if constexpr (result_is_nullable)
                if (!mem_utils::memoryIsByte(null_map + start_offset, batch_size, std::byte{1}))
                    this->setFlag(place);
        }
        else
        {
            this->nested_function->addBatchSinglePlace(
                start_offset,
                batch_size,
                this->nestedPlace(place),
                columns,
                arena,
                if_argument_pos);
            this->setFlag(place);
        }
    }
};


template <bool result_is_nullable>
class AggregateFunctionNullVariadic final
    : public AggregateFunctionNullBase<result_is_nullable, AggregateFunctionNullVariadic<result_is_nullable>>
{
public:
    AggregateFunctionNullVariadic(AggregateFunctionPtr nested_function, const DataTypes & arguments)
        : AggregateFunctionNullBase<result_is_nullable, AggregateFunctionNullVariadic<result_is_nullable>>(
            nested_function)
        , number_of_arguments(arguments.size())
    {
        if (number_of_arguments == 1)
            throw Exception(
                "Logical error: single argument is passed to AggregateFunctionNullVariadic",
                ErrorCodes::LOGICAL_ERROR);

        if (number_of_arguments > MAX_ARGS)
            throw Exception(
                "Maximum number of arguments for aggregate function with Nullable types is "
                    + toString(size_t(MAX_ARGS)),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < number_of_arguments; ++i)
            is_nullable[i] = arguments[i]->isNullable();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        /// This container stores the columns we really pass to the nested function.
        const IColumn * nested_columns[number_of_arguments];

        for (size_t i = 0; i < number_of_arguments; ++i)
        {
            if (is_nullable[i])
            {
                const auto & nullable_col = static_cast<const ColumnNullable &>(*columns[i]);
                if (nullable_col.isNullAt(row_num))
                {
                    /// If at least one column has a null value in the current row,
                    /// we don't process this row.
                    return;
                }
                nested_columns[i] = &nullable_col.getNestedColumn();
            }
            else
                nested_columns[i] = columns[i];
        }

        this->setFlag(place);
        this->nested_function->add(this->nestedPlace(place), nested_columns, row_num, arena);
    }

    bool allocatesMemoryInArena() const override { return this->nested_function->allocatesMemoryInArena(); }

private:
    enum
    {
        MAX_ARGS = 8
    };
    size_t number_of_arguments = 0;
    std::array<char, MAX_ARGS> is_nullable; /// Plain array is better than std::vector due to one indirection less.
};

template <bool input_is_nullable>
class AggregateFunctionNullUnaryForWindow
    : public IAggregateFunctionHelper<AggregateFunctionNullUnaryForWindow<input_is_nullable>>
{
protected:
    AggregateFunctionPtr nested_function;
    size_t prefix_size;

    AggregateDataPtr nestedPlace(AggregateDataPtr __restrict place) const noexcept { return place + prefix_size; }

    ConstAggregateDataPtr nestedPlace(ConstAggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

public:
    explicit AggregateFunctionNullUnaryForWindow(AggregateFunctionPtr nested_function_)
        : nested_function(nested_function_)
    {
        prefix_size = nested_function->alignOfData();
        if (prefix_size < sizeof(Int64))
        {
            auto tmp = prefix_size;
            prefix_size += sizeof(Int64);
            if ((prefix_size % tmp) != 0)
                prefix_size += tmp - (prefix_size % tmp);
            assert((prefix_size % tmp) == 0);
            assert(prefix_size >= (tmp + sizeof(Int64)));
        }
    }

    String getName() const override
    {
        /// This is just a wrapper. The function for Nullable arguments is named the same as the nested function itself.
        return nested_function->getName();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        addOrDecrease<true>(place, columns, row_num, arena);
    }

    void decrease(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena)
        const override
    {
        addOrDecrease<false>(place, columns, row_num, arena);
    }

    template <bool is_add>
    void addOrDecreaseImpl(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena)
        const
    {
        if constexpr (is_add)
        {
            this->addCounter(place);
            this->nested_function->add(this->nestedPlace(place), columns, row_num, arena);
        }
        else
        {
            this->decreaseCounter(place);
            this->nested_function->decrease(this->nestedPlace(place), columns, row_num, arena);
        }
    }

    template <bool is_add>
    void addOrDecrease(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const
    {
        if constexpr (input_is_nullable)
        {
            const auto * column = static_cast<const ColumnNullable *>(columns[0]);
            if (!column->isNullAt(row_num))
            {
                const IColumn * nested_column = &column->getNestedColumn();
                addOrDecreaseImpl<is_add>(place, &nested_column, row_num, arena);
            }
        }
        else
        {
            addOrDecreaseImpl<is_add>(place, columns, row_num, arena);
        }
    }

    void reset(AggregateDataPtr __restrict place) const override
    {
        this->resetCounter(place);
        this->nested_function->reset(this->nestedPlace(place));
    }

    void setCollators(TiDB::TiDBCollators & collators) override { nested_function->setCollators(collators); }

    DataTypePtr getReturnType() const override { return makeNullable(nested_function->getReturnType()); }

    void create(AggregateDataPtr __restrict place) const override
    {
        resetCounter(place);
        nested_function->create(nestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_function->destroy(nestedPlace(place));
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        auto & to_concrete = static_cast<ColumnNullable &>(to);
        if (getCounter(place) > 0)
        {
            nested_function->insertResultInto(nestedPlace(place), to_concrete.getNestedColumn(), arena);
            to_concrete.getNullMapData().push_back(0);
        }
        else
        {
            to_concrete.insertDefault();
        }
    }

    bool hasTrivialDestructor() const override { return nested_function->hasTrivialDestructor(); }

    size_t sizeOfData() const override { return prefix_size + nested_function->sizeOfData(); }

    size_t alignOfData() const override { return nested_function->alignOfData(); }

    bool allocatesMemoryInArena() const override { return nested_function->allocatesMemoryInArena(); }

    bool isState() const override { return nested_function->isState(); }

    const char * getHeaderFilePath() const override { return __FILE__; }

    void merge(AggregateDataPtr __restrict, ConstAggregateDataPtr, Arena *) const override
    {
        throw Exception("Not implemented yet");
    }
    void serialize(ConstAggregateDataPtr __restrict, WriteBuffer &) const override
    {
        throw Exception("Not implemented yet");
    }
    void deserialize(AggregateDataPtr __restrict, ReadBuffer &, Arena *) const override
    {
        throw Exception("Not implemented yet");
    }

private:
    inline void addCounter(AggregateDataPtr __restrict place) const noexcept
    {
        auto * counter = reinterpret_cast<Int64 *>(place);
        ++(*counter);
    }

    inline void decreaseCounter(AggregateDataPtr __restrict place) const noexcept
    {
        auto * counter = reinterpret_cast<Int64 *>(place);
        --(*counter);
        assert((*counter) >= 0);
    }

    inline Int64 getCounter(ConstAggregateDataPtr __restrict place) const noexcept
    {
        const auto * counter = reinterpret_cast<const Int64 *>(place);
        return *counter;
    }

    inline void resetCounter(AggregateDataPtr __restrict place) const noexcept
    {
        auto * counter = reinterpret_cast<Int64 *>(place);
        (*counter) = 0;
    }
};

} // namespace DB
