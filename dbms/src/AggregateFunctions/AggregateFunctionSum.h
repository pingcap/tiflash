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
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <type_traits>


namespace DB
{
template <typename T>
struct AggregateFunctionSumAddImpl
{
    static void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(T & lhs, const T & rhs) { lhs += rhs; }
};

template <typename T>
struct AggregateFunctionSumAddImpl<Decimal<T>>
{
    template <typename U>
    static void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(Decimal<T> & lhs, const Decimal<U> & rhs)
    {
        lhs.value += static_cast<T>(rhs.value);
    }
};

template <typename T>
struct AggregateFunctionSumMinusImpl
{
    static void NO_SANITIZE_UNDEFINED ALWAYS_INLINE decrease(T & lhs, const T & rhs) { lhs -= rhs; }
};

template <typename T>
struct AggregateFunctionSumMinusImpl<Decimal<T>>
{
    template <typename U>
    static void NO_SANITIZE_UNDEFINED ALWAYS_INLINE decrease(Decimal<T> & lhs, const Decimal<U> & rhs)
    {
        lhs.value -= static_cast<T>(rhs.value);
    }
};

template <typename T>
struct AggregateFunctionSumData
{
    using Impl = AggregateFunctionSumAddImpl<T>;
    using DescreaseImpl = AggregateFunctionSumMinusImpl<T>;
    T sum{};

    AggregateFunctionSumData() = default;

    template <typename U>
    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(U value)
    {
        Impl::add(sum, value);
    }

    template <typename U>
    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE decrease(U value)
    {
        DescreaseImpl::decrease(sum, value);
    }

    template <typename U>
    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE reset()
    {
        sum = 0;
    }

    /// Vectorized version
    template <typename Value>
    void NO_SANITIZE_UNDEFINED NO_INLINE addMany(const Value * __restrict ptr, size_t count)
    {
        const auto * end = ptr + count;

        if constexpr (std::is_floating_point_v<T>)
        {
            /// Compiler cannot unroll this loop, do it manually.
            /// (at least for floats, most likely due to the lack of -fassociative-math)

            /// Something around the number of SSE registers * the number of elements fit in register.
            constexpr size_t unroll_count = 128 / sizeof(T);
            T partial_sums[unroll_count]{};

            const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

            while (ptr < unrolled_end)
            {
                for (size_t i = 0; i < unroll_count; ++i)
                    Impl::add(partial_sums[i], ptr[i]);
                ptr += unroll_count;
            }

            for (size_t i = 0; i < unroll_count; ++i)
                Impl::add(sum, partial_sums[i]);
        }

        /// clang cannot vectorize the loop if accumulator is class member instead of local variable.
        T local_sum{};
        while (ptr < end)
        {
            Impl::add(local_sum, *ptr);
            ++ptr;
        }
        Impl::add(sum, local_sum);
    }

    template <typename Value>
    void NO_SANITIZE_UNDEFINED NO_INLINE
    addManyNotNull(const Value * __restrict ptr, const UInt8 * __restrict null_map, size_t count)
    {
        const auto * end = ptr + count;

        if constexpr (std::is_floating_point_v<T>)
        {
            constexpr size_t unroll_count = 128 / sizeof(T);
            T partial_sums[unroll_count]{};

            const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

            while (ptr < unrolled_end)
            {
                for (size_t i = 0; i < unroll_count; ++i)
                {
                    if (!null_map[i])
                    {
                        Impl::add(partial_sums[i], ptr[i]);
                    }
                }
                ptr += unroll_count;
                null_map += unroll_count;
            }

            for (size_t i = 0; i < unroll_count; ++i)
                Impl::add(sum, partial_sums[i]);
        }

        T local_sum{};
        while (ptr < end)
        {
            if (!*null_map)
                Impl::add(local_sum, *ptr);
            ++ptr;
            ++null_map;
        }
        Impl::add(sum, local_sum);
    }

    void merge(const AggregateFunctionSumData & rhs) { Impl::add(sum, rhs.sum); }

    void write(WriteBuffer & buf) const { writeBinary(sum, buf); }

    void read(ReadBuffer & buf) { readBinary(sum, buf); }

    T get() const { return sum; }
};

template <typename T>
struct AggregateFunctionSumKahanData
{
    static_assert(
        std::is_floating_point_v<T>,
        "It doesn't make sense to use Kahan Summation algorithm for non floating point types");

    T sum{};
    T compensation{};

    template <typename Value>
    void ALWAYS_INLINE addImpl(Value value, T & out_sum, T & out_compensation)
    {
        auto compensated_value = static_cast<T>(value) - out_compensation;
        auto new_sum = out_sum + compensated_value;
        out_compensation = (new_sum - out_sum) - compensated_value;
        out_sum = new_sum;
    }

    void ALWAYS_INLINE add(T value) { addImpl(value, sum, compensation); }

    void ALWAYS_INLINE decrease(T) { throw Exception("`decrease` function is not implemented in AggregateFunctionSumKahanData"); }

    /// Vectorized version
    template <typename Value>
    void NO_INLINE addMany(const Value * __restrict ptr, size_t count)
    {
        /// Less than in ordinary sum, because the algorithm is more complicated and too large loop unrolling is questionable.
        /// But this is just a guess.
        constexpr size_t unroll_count = 4;
        T partial_sums[unroll_count]{};
        T partial_compensations[unroll_count]{};

        const auto * end = ptr + count;
        const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

        while (ptr < unrolled_end)
        {
            for (size_t i = 0; i < unroll_count; ++i)
                addImpl(ptr[i], partial_sums[i], partial_compensations[i]);
            ptr += unroll_count;
        }

        for (size_t i = 0; i < unroll_count; ++i)
            mergeImpl(sum, compensation, partial_sums[i], partial_compensations[i]);

        while (ptr < end)
        {
            addImpl(*ptr, sum, compensation);
            ++ptr;
        }
    }

    template <typename Value>
    void NO_INLINE addManyNotNull(const Value * __restrict ptr, const UInt8 * __restrict null_map, size_t count)
    {
        constexpr size_t unroll_count = 4;
        T partial_sums[unroll_count]{};
        T partial_compensations[unroll_count]{};

        const auto * end = ptr + count;
        const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

        while (ptr < unrolled_end)
        {
            for (size_t i = 0; i < unroll_count; ++i)
                if (!null_map[i])
                    addImpl(ptr[i], partial_sums[i], partial_compensations[i]);
            ptr += unroll_count;
            null_map += unroll_count;
        }

        for (size_t i = 0; i < unroll_count; ++i)
            mergeImpl(sum, compensation, partial_sums[i], partial_compensations[i]);

        while (ptr < end)
        {
            if (!*null_map)
                addImpl(*ptr, sum, compensation);
            ++ptr;
            ++null_map;
        }
    }

    void ALWAYS_INLINE mergeImpl(T & to_sum, T & to_compensation, T from_sum, T from_compensation)
    {
        auto raw_sum = to_sum + from_sum;
        auto rhs_compensated = raw_sum - to_sum;
        /// Kahan summation is tricky because it depends on non-associativity of float arithmetic.
        /// Do not simplify this expression if you are not sure.
        auto compensations = ((from_sum - rhs_compensated) + (to_sum - (raw_sum - rhs_compensated))) + compensation
            + from_compensation;
        to_sum = raw_sum + compensations;
        to_compensation = compensations - (to_sum - raw_sum);
    }

    void merge(const AggregateFunctionSumKahanData & rhs) { mergeImpl(sum, compensation, rhs.sum, rhs.compensation); }

    void write(WriteBuffer & buf) const
    {
        writeBinary(sum, buf);
        writeBinary(compensation, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinary(sum, buf);
        readBinary(compensation, buf);
    }

    T get() const { return sum; }
};


#define DEFAULT_DECIMAL_INFER                                                           \
    static std::tuple<PrecType, ScaleType> decimalInfer(PrecType prec, ScaleType scale) \
    {                                                                                   \
        return SumDecimalInferer::infer(prec, scale);                                   \
    }


struct NameSum
{
    static constexpr auto name = "sum";
    DEFAULT_DECIMAL_INFER
};

struct NameSumWithOverFlow
{
    static constexpr auto name = "sumWithOverflow";
    static std::tuple<PrecType, ScaleType> decimalInfer(PrecType prec, ScaleType scale) { return {prec, scale}; }
};

using NameSumOnPartialResult = NameSumWithOverFlow;

struct NameCountSecondStage
{
    static constexpr auto name = "countSecondStage";
    DEFAULT_DECIMAL_INFER
};

struct NameSumKahan
{
    static constexpr auto name = "sumKahan";
    DEFAULT_DECIMAL_INFER
};

/// Counts the sum of the numbers.
template <typename T, typename TResult, typename Data, typename Name = NameSum>
class AggregateFunctionSum final
    : public IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data, Name>>
{
    static_assert(IsDecimal<T> == IsDecimal<TResult>);

public:
    using ResultDataType = std::conditional_t<IsDecimal<T>, DataTypeDecimal<TResult>, DataTypeNumber<TResult>>;
    using ColVecType = std::conditional_t<IsDecimal<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult = std::conditional_t<IsDecimal<T>, ColumnDecimal<TResult>, ColumnVector<TResult>>;

    String getName() const override { return Name::name; }

    ScaleType result_scale{};
    PrecType result_prec{};

    AggregateFunctionSum() = default;

    AggregateFunctionSum(PrecType prec, ScaleType scale)
    {
        std::tie(result_prec, result_scale) = Name::decimalInfer(prec, scale);
    };

    DataTypePtr getReturnType() const override
    {
        if constexpr (IsDecimal<TResult>)
        {
            return std::make_shared<ResultDataType>(result_prec, result_scale);
        }
        else
        {
            return std::make_shared<ResultDataType>();
        }
    }

    void create(AggregateDataPtr __restrict place) const override // NOLINT(readability-non-const-parameter)
    {
        new (place) Data;
    }

    void prepareWindow(AggregateDataPtr __restrict) const override {}

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        this->data(place).add(column.getData()[row_num]);
    }

    void decrease(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        this->data(place).decrease(column.getData()[row_num]);
    }

    void reset(AggregateDataPtr __restrict place) const override
    {
        this->data(place).reset();
    }

    /// Vectorized version when there is no GROUP BY keys.
    void addBatchSinglePlace(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = start_offset; i < start_offset + batch_size; ++i)
            {
                if (flags[i])
                    add(place, columns, i, arena);
            }
        }
        else
        {
            const auto & column = assert_cast<const ColVecType &>(*columns[0]);
            this->data(place).addMany(column.getData().data() + start_offset, batch_size);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = start_offset; i < start_offset + batch_size; ++i)
                if (!null_map[i] && flags[i])
                    add(place, columns, i, arena);
        }
        else
        {
            const auto & column = assert_cast<const ColVecType &>(*columns[0]);
            this->data(place).addManyNotNull(
                column.getData().data() + start_offset,
                null_map + start_offset,
                batch_size);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        if constexpr (IsDecimal<TResult>)
        {
            static_cast<ColumnDecimal<TResult> &>(to).getData().push_back(this->data(place).get(), result_scale);
        }
        else
            static_cast<ColumnVector<TResult> &>(to).getData().push_back(this->data(place).get());
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

} // namespace DB
