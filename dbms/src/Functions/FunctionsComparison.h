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

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Core/DecimalComparison.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeMyTimeBase.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/CollationStringComparision.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunction.h>
#include <Functions/StringUtil.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <fmt/core.h>

#include <limits>
#include <type_traits>


namespace DB
{
/** Comparison functions: ==, !=, <, >, <=, >=.
  * The comparison functions always return 0 or 1 (UInt8).
  *
  * You can compare the following types:
  * - numbers;
  * - strings and fixed strings;
  * - dates;
  * - datetimes;
  *   within each group, but not from different groups;
  * - tuples (lexicographic comparison).
  *
  * Exception: You can compare the date and datetime with a constant string. Example: EventDate = '2015-01-01'.
  *
  * TODO Arrays.
  */

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename A, typename B, typename Op>
struct NumComparisonImpl
{
    /// If you don't specify NO_INLINE, the compiler will inline this function, but we don't need this as this function contains tight loop inside.
    static void NO_INLINE
    vectorVector(const PaddedPODArray<A> & a, const PaddedPODArray<B> & b, PaddedPODArray<UInt8> & c)
    {
        /** GCC 4.8.2 vectorizes a loop only if it is written in this form.
          * In this case, if you loop through the array index (the code will look simpler),
          *  the loop will not be vectorized.
          */

        size_t size = a.size();
        const A * __restrict a_pos = &a[0];
        const B * __restrict b_pos = &b[0];
        UInt8 * __restrict c_pos = &c[0];
        const A * a_end = a_pos + size;

        while (a_pos < a_end)
        {
            *c_pos = Op::apply(*a_pos, *b_pos);
            ++a_pos;
            ++b_pos;
            ++c_pos;
        }
    }

    static void NO_INLINE vectorConstant(const PaddedPODArray<A> & a, B b, PaddedPODArray<UInt8> & c)
    {
        size_t size = a.size();
        const A * __restrict a_pos = &a[0];
        UInt8 * __restrict c_pos = &c[0];
        const A * a_end = a_pos + size;

        while (a_pos < a_end)
        {
            *c_pos = Op::apply(*a_pos, b);
            ++a_pos;
            ++c_pos;
        }
    }

    static void constantVector(A a, const PaddedPODArray<B> & b, PaddedPODArray<UInt8> & c)
    {
        NumComparisonImpl<B, A, typename Op::SymmetricOp>::vectorConstant(b, a, c);
    }

    static void constantConstant(A a, B b, UInt8 & c) { c = Op::apply(a, b); }
};


inline int memcmp16(const void * a, const void * b)
{
    /// Assuming little endian.

    UInt64 a_hi = __builtin_bswap64(unalignedLoad<UInt64>(a));
    UInt64 b_hi = __builtin_bswap64(unalignedLoad<UInt64>(b));

    if (a_hi < b_hi)
        return -1;
    if (a_hi > b_hi)
        return 1;

    UInt64 a_lo = __builtin_bswap64(unalignedLoad<UInt64>(reinterpret_cast<const char *>(a) + 8));
    UInt64 b_lo = __builtin_bswap64(unalignedLoad<UInt64>(reinterpret_cast<const char *>(b) + 8));

    if (a_lo < b_lo)
        return -1;
    if (a_lo > b_lo)
        return 1;

    return 0;
}


inline time_t dateToDateTime(UInt32 date_data)
{
    DayNum day_num(date_data);
    LocalDate local_date(day_num);
    // todo use timezone info
    return DateLUT::instance().makeDateTime(local_date.year(), local_date.month(), local_date.day(), 0, 0, 0);
}

inline std::tuple<DayNum, bool> dateTimeToDate(time_t time_data)
{
    // todo use timezone info
    const auto & date_lut = DateLUT::instance();
    auto truncated
        = date_lut.toHour(time_data) != 0 || date_lut.toMinute(time_data) != 0 || date_lut.toSecond(time_data) != 0;
    auto values = date_lut.getValues(time_data);
    auto day_num = date_lut.makeDayNum(values.year, values.month, values.day_of_month);
    return std::make_tuple(static_cast<DayNum>(day_num), truncated);
}


template <typename A, typename B, template <typename, typename> class Op, bool is_left_date>
struct DateDateTimeComparisonImpl
{
    static void NO_INLINE
    vectorVector(const PaddedPODArray<A> & a, const PaddedPODArray<B> & b, PaddedPODArray<UInt8> & c)
    {
        size_t size = a.size();
        const A * a_pos = &a[0];
        const B * b_pos = &b[0];
        UInt8 * c_pos = &c[0];
        const A * a_end = a_pos + size;
        while (a_pos < a_end)
        {
            if (is_left_date)
            {
                using OpType = B;
                time_t date_time = dateToDateTime(*a_pos);
                *c_pos = Op<OpType, OpType>::apply(static_cast<OpType>(date_time), *b_pos);
            }
            else
            {
                using OpType = A;
                time_t date_time = dateToDateTime(*b_pos);
                *c_pos = Op<OpType, OpType>::apply(*a_pos, static_cast<OpType>(date_time));
            }
            ++a_pos;
            ++b_pos;
            ++c_pos;
        }
    }

    static void NO_INLINE vectorConstant(const PaddedPODArray<A> & a, B b, PaddedPODArray<UInt8> & c)
    {
        if (!is_left_date)
        {
            // datetime vector with date constant
            using OpType = A;
            time_t date_time = dateToDateTime(b);
            NumComparisonImpl<OpType, OpType, Op<OpType, OpType>>::vectorConstant(a, static_cast<OpType>(date_time), c);
        }
        else
        {
            // date vector with datetime constant
            // first check if datetime constant can be convert to date constant
            bool truncated;
            DayNum date_num;
            std::tie(date_num, truncated) = dateTimeToDate(static_cast<time_t>(b));
            if (!truncated)
            {
                using OpType = A;
                NumComparisonImpl<OpType, OpType, Op<OpType, OpType>>::vectorConstant(
                    a,
                    static_cast<OpType>(date_num),
                    c);
            }
            else
            {
                using OpType = B;
                size_t size = a.size();
                const A * a_pos = &a[0];
                UInt8 * c_pos = &c[0];
                const A * a_end = a_pos + size;

                while (a_pos < a_end)
                {
                    time_t date_time = dateToDateTime(*a_pos);
                    *c_pos = Op<OpType, OpType>::apply(static_cast<OpType>(date_time), b);
                    ++a_pos;
                    ++c_pos;
                }
            }
        }
    }

    static void constantVector(A a, const PaddedPODArray<B> & b, PaddedPODArray<UInt8> & c)
    {
        if (is_left_date)
        {
            // date constant with datetime vector
            using OpType = B;
            time_t date_time = dateToDateTime(a);
            NumComparisonImpl<OpType, OpType, Op<OpType, OpType>>::constantVector(static_cast<OpType>(date_time), b, c);
        }
        else
        {
            // datetime constant with date vector
            bool truncated;
            DayNum date_num;
            std::tie(date_num, truncated) = dateTimeToDate(static_cast<time_t>(a));
            if (!truncated)
            {
                using OpType = B;
                NumComparisonImpl<OpType, OpType, Op<OpType, OpType>>::vectorConstant(
                    static_cast<OpType>(a),
                    date_num,
                    c);
            }
            else
            {
                using OpType = A;
                size_t size = b.size();
                const B * b_pos = &b[0];
                UInt8 * c_pos = &c[0];
                const B * b_end = b_pos + size;

                while (b_pos < b_end)
                {
                    time_t date_time = dateToDateTime(*b_pos);
                    *c_pos = Op<OpType, OpType>::apply(a, static_cast<OpType>(date_time));
                    ++b_pos;
                    ++c_pos;
                }
            }
        }
    }

    static void constantConstant(A a, B b, UInt8 & c)
    {
        if (is_left_date)
        {
            using OpType = B;
            time_t date_time = dateToDateTime(a);
            NumComparisonImpl<OpType, OpType, Op<OpType, OpType>>::constantConstant(
                static_cast<OpType>(date_time),
                b,
                c);
        }
        else
        {
            using OpType = A;
            time_t date_time = dateToDateTime(b);
            NumComparisonImpl<OpType, OpType, Op<OpType, OpType>>::constantConstant(
                a,
                static_cast<OpType>(date_time),
                c);
        }
    }
};

template <typename Op, typename ResultType>
struct StringComparisonWithCollatorImpl
{
    static void NO_INLINE stringVectorStringVector(
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        const TiDB::TiDBCollatorPtr & collator,
        PaddedPODArray<ResultType> & c)
    {
        bool optimized_path = CompareStringVectorStringVector<Op>(a_data, a_offsets, b_data, b_offsets, collator, c);
        if (optimized_path)
        {
            return;
        }

        size_t size = a_offsets.size();

        for (size_t i = 0; i < size; ++i)
        {
            size_t a_size = StringUtil::sizeAt(a_offsets, i) - 1;
            size_t b_size = StringUtil::sizeAt(b_offsets, i) - 1;
            size_t a_offset = StringUtil::offsetAt(a_offsets, i);
            size_t b_offset = StringUtil::offsetAt(b_offsets, i);

            c[i] = Op::apply(
                collator->compare(
                    reinterpret_cast<const char *>(&a_data[a_offset]),
                    a_size,
                    reinterpret_cast<const char *>(&b_data[b_offset]),
                    b_size),
                0);
        }
    }

    static void NO_INLINE stringVectorConstant(
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const std::string_view & b,
        const TiDB::TiDBCollatorPtr & collator,
        PaddedPODArray<ResultType> & c)
    {
        bool optimized_path = CompareStringVectorConstant<Op>(a_data, a_offsets, b, collator, c);

        if (optimized_path)
        {
            return;
        }

        size_t size = a_offsets.size();
        ColumnString::Offset b_size = b.size();
        const char * b_data = reinterpret_cast<const char *>(b.data());
        for (size_t i = 0; i < size; ++i)
        {
            /// Trailing zero byte of the smaller string is included in the comparison.
            c[i] = Op::apply(
                collator->compare(
                    reinterpret_cast<const char *>(&a_data[StringUtil::offsetAt(a_offsets, i)]),
                    StringUtil::sizeAt(a_offsets, i) - 1,
                    b_data,
                    b_size),
                0);
        }
    }

    static void constantStringVector(
        const std::string_view & a,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        const TiDB::TiDBCollatorPtr & collator,
        PaddedPODArray<ResultType> & c)
    {
        StringComparisonWithCollatorImpl<typename Op::SymmetricOp, ResultType>::stringVectorConstant(
            b_data,
            b_offsets,
            a,
            collator,
            c);
    }

    static void constantConstant(
        const std::string_view & a,
        const std::string_view & b,
        const TiDB::TiDBCollatorPtr & collator,
        ResultType & c)
    {
        size_t a_n = a.size();
        size_t b_n = b.size();

        int res = collator->compareFastPath(
            reinterpret_cast<const char *>(a.data()),
            a_n,
            reinterpret_cast<const char *>(b.data()),
            b_n);
        c = Op::apply(res, 0);
    }
};

template <typename Op, typename ResultType>
struct StringComparisonImpl
{
    static void NO_INLINE stringVectorStringVector(
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        PaddedPODArray<ResultType> & c)
    {
        size_t size = a_offsets.size();

        for (size_t i = 0; i < size; ++i)
        {
            /// Trailing zero byte of the smaller string is included in the comparison.
            size_t a_size = StringUtil::sizeAt(a_offsets, i);
            size_t b_size = StringUtil::sizeAt(b_offsets, i);
            size_t a_offset = StringUtil::offsetAt(a_offsets, i);
            size_t b_offset = StringUtil::offsetAt(b_offsets, i);
            int res = memcmp(&a_data[a_offset], &b_data[b_offset], std::min(a_size, b_size));
            /// if partial compare result is 0, it means the common part of the two strings are exactly the same, then need to
            /// further compare the string length, otherwise we can get the compare result from partial compare result.
            c[i] = res == 0 ? Op::apply(a_size, b_size) : Op::apply(res, 0);
        }
    }

    static void NO_INLINE stringVectorConstant(
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const std::string & b,
        PaddedPODArray<ResultType> & c)
    {
        size_t size = a_offsets.size();
        ColumnString::Offset b_size = b.size() + 1;
        const UInt8 * b_data = reinterpret_cast<const UInt8 *>(b.data());
        for (size_t i = 0; i < size; ++i)
        {
            /// Trailing zero byte of the smaller string is included in the comparison.
            size_t a_size = StringUtil::sizeAt(a_offsets, i);
            size_t a_offset = StringUtil::offsetAt(a_offsets, i);

            int res = memcmp(&a_data[a_offset], b_data, std::min(a_size, b_size));
            c[i] = res == 0 ? Op::apply(a_size, b_size) : Op::apply(res, 0);
        }
    }

    static void constantStringVector(
        const std::string & a,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        PaddedPODArray<ResultType> & c)
    {
        StringComparisonImpl<typename Op::SymmetricOp, ResultType>::stringVectorConstant(b_data, b_offsets, a, c);
    }

    static void constantConstant(const std::string & a, const std::string & b, ResultType & c)
    {
        size_t a_n = a.size();
        size_t b_n = b.size();

        int res = memcmp(a.data(), b.data(), std::min(a_n, b_n));
        c = res == 0 ? Op::apply(a_n, b_n) : Op::apply(res, 0);
    }
};


/// Comparisons for equality/inequality are implemented slightly more efficient.
template <bool positive>
struct StringEqualsImpl
{
    static void NO_INLINE stringVectorStringVector(
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = positive
                == ((i == 0) ? (a_offsets[0] == b_offsets[0] && !memcmp(&a_data[0], &b_data[0], a_offsets[0] - 1))
                             : (a_offsets[i] - a_offsets[i - 1] == b_offsets[i] - b_offsets[i - 1]
                                && !memcmp(
                                    &a_data[a_offsets[i - 1]],
                                    &b_data[b_offsets[i - 1]],
                                    a_offsets[i] - a_offsets[i - 1] - 1)));
    }

    static void NO_INLINE stringVectorConstant(
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const std::string & b,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        ColumnString::Offset b_n = b.size();
        const UInt8 * b_data = reinterpret_cast<const UInt8 *>(b.data());
        for (size_t i = 0; i < size; ++i)
            c[i] = positive
                == ((i == 0) ? (a_offsets[0] == b_n + 1 && !memcmp(&a_data[0], b_data, b_n))
                             : (a_offsets[i] - a_offsets[i - 1] == b_n + 1
                                && !memcmp(&a_data[a_offsets[i - 1]], b_data, b_n)));
    }

    static void constantStringVector(
        const std::string & a,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        stringVectorConstant(b_data, b_offsets, a, c);
    }

    static void constantConstant(const std::string & a, const std::string & b, UInt8 & c) { c = positive == (a == b); }
};


template <typename A, typename B>
struct StringComparisonImpl<EqualsOp<A, B>, UInt8> : StringEqualsImpl<true>
{
};

template <typename A, typename B>
struct StringComparisonImpl<NotEqualsOp<A, B>, UInt8> : StringEqualsImpl<false>
{
};


/// Generic version, implemented for columns of same type.
template <typename Op>
struct GenericComparisonImpl
{
    static void NO_INLINE vectorVector(const IColumn & a, const IColumn & b, PaddedPODArray<UInt8> & c)
    {
        for (size_t i = 0, size = a.size(); i < size; ++i)
            c[i] = Op::apply(a.compareAt(i, i, b, 1), 0);
    }

    static void NO_INLINE vectorConstant(const IColumn & a, const IColumn & b, PaddedPODArray<UInt8> & c)
    {
        auto b_materialized = b.cloneResized(1)->convertToFullColumnIfConst();
        for (size_t i = 0, size = a.size(); i < size; ++i)
            c[i] = Op::apply(a.compareAt(i, 0, *b_materialized, 1), 0);
    }

    static void constantVector(const IColumn & a, const IColumn & b, PaddedPODArray<UInt8> & c)
    {
        GenericComparisonImpl<typename Op::SymmetricOp>::vectorConstant(b, a, c);
    }

    static void constantConstant(const IColumn & a, const IColumn & b, UInt8 & c)
    {
        c = Op::apply(a.compareAt(0, 0, b, 1), 0);
    }
};


struct NameEquals
{
    static constexpr auto name = "equals";
};
struct NameNotEquals
{
    static constexpr auto name = "notEquals";
};
struct NameLess
{
    static constexpr auto name = "less";
};
struct NameGreater
{
    static constexpr auto name = "greater";
};
struct NameLessOrEquals
{
    static constexpr auto name = "lessOrEquals";
};
struct NameGreaterOrEquals
{
    static constexpr auto name = "greaterOrEquals";
};
struct NameStrcmp
{
    static constexpr auto name = "strcmp";
};

template <template <typename, typename> class Op, typename Name, typename StrCmpRetColType = ColumnUInt8>
class FunctionComparison : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionComparison>(); };

private:
    template <typename T0, typename T1>
    bool executeNumRightType(
        Block & block,
        size_t result,
        const ColumnVector<T0> * col_left,
        const IColumn * col_right_untyped) const
    {
        if (const ColumnVector<T1> * col_right = checkAndGetColumn<ColumnVector<T1>>(col_right_untyped))
        {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container & vec_res = col_res->getData();
            vec_res.resize(col_left->getData().size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::vectorVector(col_left->getData(), col_right->getData(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }
        else if (auto col_right = checkAndGetColumnConst<ColumnVector<T1>>(col_right_untyped))
        {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container & vec_res = col_res->getData();
            vec_res.resize(col_left->size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::vectorConstant(
                col_left->getData(),
                col_right->template getValue<T1>(),
                vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }

        return false;
    }

    template <typename T0, typename T1>
    bool executeNumConstRightType(
        Block & block,
        size_t result,
        const ColumnConst * col_left,
        const IColumn * col_right_untyped) const
    {
        if (const ColumnVector<T1> * col_right = checkAndGetColumn<ColumnVector<T1>>(col_right_untyped))
        {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container & vec_res = col_res->getData();
            vec_res.resize(col_left->size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::constantVector(
                col_left->template getValue<T0>(),
                col_right->getData(),
                vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }
        else if (auto col_right = checkAndGetColumnConst<ColumnVector<T1>>(col_right_untyped))
        {
            UInt8 res = 0;
            NumComparisonImpl<T0, T1, Op<T0, T1>>::constantConstant(
                col_left->template getValue<T0>(),
                col_right->template getValue<T1>(),
                res);

            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(col_left->size(), toField(res));
            return true;
        }

        return false;
    }

    template <typename T0>
    bool executeNumLeftType(
        Block & block,
        size_t result,
        const IColumn * col_left_untyped,
        const IColumn * col_right_untyped) const
    {
        if (const ColumnVector<T0> * col_left = checkAndGetColumn<ColumnVector<T0>>(col_left_untyped))
        {
            if (executeNumRightType<T0, UInt8>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, UInt16>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, UInt32>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, UInt64>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, Int8>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, Int16>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, Int32>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, Int64>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, Float32>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, Float64>(block, result, col_left, col_right_untyped))
                return true;
            else
                throw Exception(
                    fmt::format(
                        "Illegal column {} of second argument of function {}",
                        col_right_untyped->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (auto col_left = checkAndGetColumnConst<ColumnVector<T0>>(col_left_untyped))
        {
            if (executeNumConstRightType<T0, UInt8>(block, result, col_left, col_right_untyped)
                || executeNumConstRightType<T0, UInt16>(block, result, col_left, col_right_untyped)
                || executeNumConstRightType<T0, UInt32>(block, result, col_left, col_right_untyped)
                || executeNumConstRightType<T0, UInt64>(block, result, col_left, col_right_untyped)
                || executeNumConstRightType<T0, Int8>(block, result, col_left, col_right_untyped)
                || executeNumConstRightType<T0, Int16>(block, result, col_left, col_right_untyped)
                || executeNumConstRightType<T0, Int32>(block, result, col_left, col_right_untyped)
                || executeNumConstRightType<T0, Int64>(block, result, col_left, col_right_untyped)
                || executeNumConstRightType<T0, Float32>(block, result, col_left, col_right_untyped)
                || executeNumConstRightType<T0, Float64>(block, result, col_left, col_right_untyped))
                return true;
            else
                throw Exception(
                    fmt::format(
                        "Illegal column {} of second argument of function {}",
                        col_right_untyped->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        return false;
    }

    template <typename ResultColumnType>
    bool executeStringWithoutCollator(
        Block & block,
        size_t result,
        const IColumn * c0,
        const IColumn * c1,
        const ColumnString * c0_string,
        const ColumnString * c1_string,
        const ColumnConst * c0_const,
        const ColumnConst * c1_const) const
    {
        using ResultType = typename ResultColumnType::value_type;
        using StringImpl = StringComparisonImpl<Op<int, int>, ResultType>;

        if (c0_const && c1_const)
        {
            ResultType res = 0;
            StringImpl::constantConstant(c0_const->getValue<String>(), c1_const->getValue<String>(), res);
            block.getByPosition(result).column
                = block.getByPosition(result).type->createColumnConst(c0_const->size(), toField(res));
            return true;
        }
        else
        {
            auto c_res = ResultColumnType::create();
            typename ResultColumnType::Container & vec_res = c_res->getData();
            vec_res.resize(c0->size());

            if (c0_string && c1_string)
                StringImpl::stringVectorStringVector(
                    c0_string->getChars(),
                    c0_string->getOffsets(),
                    c1_string->getChars(),
                    c1_string->getOffsets(),
                    c_res->getData());
            else if (c0_string && c1_const)
                StringImpl::stringVectorConstant(
                    c0_string->getChars(),
                    c0_string->getOffsets(),
                    c1_const->getValue<String>(),
                    c_res->getData());
            else if (c0_const && c1_string)
                StringImpl::constantStringVector(
                    c0_const->getValue<String>(),
                    c1_string->getChars(),
                    c1_string->getOffsets(),
                    c_res->getData());
            else
                throw Exception(
                    fmt::format(
                        "Illegal columns {} and {} of arguments of function {}",
                        c0->getName(),
                        c1->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_COLUMN);

            block.getByPosition(result).column = std::move(c_res);
            return true;
        }
    }

    static inline std::string_view genConstStrRef(const ColumnConst * c0_const)
    {
        std::string_view c0_const_str_ref{};
        if (c0_const)
        {
            if (const auto * c0_const_string = checkAndGetColumn<ColumnString>(&c0_const->getDataColumn());
                c0_const_string)
            {
                c0_const_str_ref = std::string_view(c0_const_string->getDataAt(0));
            }
            else if (const auto * c0_const_fixed_string
                     = checkAndGetColumn<ColumnFixedString>(&c0_const->getDataColumn());
                     c0_const_fixed_string)
            {
                c0_const_str_ref = std::string_view(c0_const_fixed_string->getDataAt(0));
            }
            else
                throw Exception(
                    "Logical error: ColumnConst contains not String nor FixedString column",
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        return c0_const_str_ref;
    }

    template <typename ResultColumnType>
    bool executeStringWithCollator(
        Block & block,
        size_t result,
        const IColumn * c0,
        const IColumn * c1,
        const ColumnString * c0_string,
        const ColumnString * c1_string,
        const ColumnConst * c0_const,
        const ColumnConst * c1_const) const
    {
        using ResultType = typename ResultColumnType::value_type;
        using StringImpl = StringComparisonWithCollatorImpl<Op<int, int>, ResultType>;

        std::string_view c0_const_str_ref = genConstStrRef(c0_const);
        std::string_view c1_const_str_ref = genConstStrRef(c1_const);

        if (c0_const && c1_const)
        {
            ResultType res = 0;
            StringImpl::constantConstant(c0_const_str_ref, c1_const_str_ref, collator, res);
            block.getByPosition(result).column
                = block.getByPosition(result).type->createColumnConst(c0_const->size(), toField(res));
            return true;
        }
        else
        {
            auto c_res = ResultColumnType::create();
            typename ResultColumnType::Container & vec_res = c_res->getData();
            vec_res.resize(c0->size());

            if (c0_string && c1_string)
                StringImpl::stringVectorStringVector(
                    c0_string->getChars(),
                    c0_string->getOffsets(),
                    c1_string->getChars(),
                    c1_string->getOffsets(),
                    collator,
                    c_res->getData());
            else if (c0_string && c1_const)
                StringImpl::stringVectorConstant(
                    c0_string->getChars(),
                    c0_string->getOffsets(),
                    c1_const_str_ref,
                    collator,
                    c_res->getData());
            else if (c0_const && c1_string)
                StringImpl::constantStringVector(
                    c0_const_str_ref,
                    c1_string->getChars(),
                    c1_string->getOffsets(),
                    collator,
                    c_res->getData());
            else
                throw Exception(
                    fmt::format(
                        "Illegal columns {} and {} of arguments of function {}",
                        c0->getName(),
                        c1->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_COLUMN);

            block.getByPosition(result).column = std::move(c_res);
            return true;
        }
    }

    friend class FunctionStrcmp;

    bool executeString(Block & block, size_t result, const IColumn * c0, const IColumn * c1) const
    {
        const auto * c0_string = checkAndGetColumn<ColumnString>(c0);
        const auto * c1_string = checkAndGetColumn<ColumnString>(c1);
        const ColumnConst * c0_const = checkAndGetColumnConstStringOrFixedString(c0);
        const ColumnConst * c1_const = checkAndGetColumnConstStringOrFixedString(c1);

        if (!((c0_string || c0_const) && (c1_string || c1_const)))
            return false;

        if (collator != nullptr)
            return executeStringWithCollator<
                StrCmpRetColType>(block, result, c0, c1, c0_string, c1_string, c0_const, c1_const);
        else
            return executeStringWithoutCollator<
                StrCmpRetColType>(block, result, c0, c1, c0_string, c1_string, c0_const, c1_const);
    }

    void executeDateOrDateTimeOrEnumWithConstString(
        Block & block,
        size_t result,
        const IColumn * col_left_untyped,
        const IColumn * col_right_untyped,
        const DataTypePtr & left_type,
        const DataTypePtr & right_type,
        bool left_is_num) const
    {
        /// This is no longer very special case - comparing dates, datetimes, and enumerations with a string constant.
        const IColumn * column_string_untyped = !left_is_num ? col_left_untyped : col_right_untyped;
        const IColumn * column_number = left_is_num ? col_left_untyped : col_right_untyped;
        const IDataType * number_type = left_is_num ? left_type.get() : right_type.get();

        bool is_date = false;
        bool is_date_time = false;
        bool is_my_date = false;
        bool is_my_datetime = false;
        bool is_enum8 = false;
        bool is_enum16 = false;

        const auto legal_types = (is_date = checkAndGetDataType<DataTypeDate>(number_type))
            || (is_date_time = checkAndGetDataType<DataTypeDateTime>(number_type))
            || (is_my_datetime = checkAndGetDataType<DataTypeMyDateTime>(number_type))
            || (is_my_date = checkAndGetDataType<DataTypeMyDate>(number_type))
            || (is_enum8 = checkAndGetDataType<DataTypeEnum8>(number_type))
            || (is_enum16 = checkAndGetDataType<DataTypeEnum16>(number_type));

        const auto * column_string = checkAndGetColumnConst<ColumnString>(column_string_untyped);
        if (!column_string || !legal_types)
            throw Exception(
                fmt::format(
                    "Illegal columns {} and {} of arguments of function {}",
                    col_left_untyped->getName(),
                    col_right_untyped->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_COLUMN);

        StringRef string_value = column_string->getDataAt(0);

        if (is_date)
        {
            DayNum date;
            ReadBufferFromMemory in(string_value.data, string_value.size);
            readDateText(date, in);
            if (!in.eof())
                throw Exception(fmt::format("String is too long for Date: {}", string_value));

            ColumnPtr parsed_const_date_holder = DataTypeDate().createColumnConst(block.rows(), UInt64(date));
            const ColumnConst * parsed_const_date = static_cast<const ColumnConst *>(parsed_const_date_holder.get());
            executeNumLeftType<DataTypeDate::FieldType>(
                block,
                result,
                left_is_num ? col_left_untyped : parsed_const_date,
                left_is_num ? parsed_const_date : col_right_untyped);
        }
        else if (is_my_date || is_my_datetime)
        {
            Field parsed_time = parseMyDateTime(string_value.toString());
            const DataTypePtr & time_type = left_is_num ? left_type : right_type;
            ColumnPtr parsed_const_date_holder = time_type->createColumnConst(block.rows(), parsed_time);
            const ColumnConst * parsed_const_date = static_cast<const ColumnConst *>(parsed_const_date_holder.get());
            executeNumLeftType<DataTypeMyTimeBase::FieldType>(
                block,
                result,
                left_is_num ? col_left_untyped : parsed_const_date,
                left_is_num ? parsed_const_date : col_right_untyped);
        }
        else if (is_date_time)
        {
            time_t date_time;
            ReadBufferFromMemory in(string_value.data, string_value.size);
            readDateTimeText(date_time, in);
            if (!in.eof())
                throw Exception(fmt::format("String is too long for DateTime: {}", string_value));

            ColumnPtr parsed_const_date_time_holder
                = DataTypeDateTime().createColumnConst(block.rows(), UInt64(date_time));
            const ColumnConst * parsed_const_date_time
                = static_cast<const ColumnConst *>(parsed_const_date_time_holder.get());
            executeNumLeftType<DataTypeDateTime::FieldType>(
                block,
                result,
                left_is_num ? col_left_untyped : parsed_const_date_time,
                left_is_num ? parsed_const_date_time : col_right_untyped);
        }

        else if (is_enum8)
            executeEnumWithConstString<DataTypeEnum8>(
                block,
                result,
                column_number,
                column_string,
                number_type,
                left_is_num);
        else if (is_enum16)
            executeEnumWithConstString<DataTypeEnum16>(
                block,
                result,
                column_number,
                column_string,
                number_type,
                left_is_num);
    }

    /// Comparison between DataTypeEnum<T> and string constant containing the name of an enum element
    template <typename EnumType>
    void executeEnumWithConstString(
        Block & block,
        const size_t result,
        const IColumn * column_number,
        const ColumnConst * column_string,
        const IDataType * type_untyped,
        const bool left_is_num) const
    {
        const auto type = static_cast<const EnumType *>(type_untyped);

        const Field x = nearestFieldType(type->getValue(column_string->getValue<String>()));
        const auto enum_col = type->createColumnConst(block.rows(), x);

        executeNumLeftType<typename EnumType::FieldType>(
            block,
            result,
            left_is_num ? column_number : enum_col.get(),
            left_is_num ? enum_col.get() : column_number);
    }

    void executeTuple(Block & block, size_t result, const ColumnWithTypeAndName & c0, const ColumnWithTypeAndName & c1)
        const
    {
        /** We will lexicographically compare the tuples. This is done as follows:
          * x == y : x1 == y1 && x2 == y2 ...
          * x != y : x1 != y1 || x2 != y2 ...
          *
          * x < y:   x1 < y1 || (x1 == y1 && (x2 < y2 || (x2 == y2 ... && xn < yn))
          * x > y:   x1 > y1 || (x1 == y1 && (x2 > y2 || (x2 == y2 ... && xn > yn))
          * x <= y:  x1 < y1 || (x1 == y1 && (x2 < y2 || (x2 == y2 ... && xn <= yn))
          *
          * Recursive form:
          * x <= y:  x1 < y1 || (x1 == y1 && x_tail <= y_tail)
          *
          * x >= y:  x1 > y1 || (x1 == y1 && (x2 > y2 || (x2 == y2 ... && xn >= yn))
          */

        const size_t tuple_size = typeid_cast<const DataTypeTuple &>(*c0.type).getElements().size();

        if (0 == tuple_size)
            throw Exception("Comparison of zero-sized tuples is not implemented.", ErrorCodes::NOT_IMPLEMENTED);

        ColumnsWithTypeAndName x(tuple_size);
        ColumnsWithTypeAndName y(tuple_size);

        const auto * x_const = checkAndGetColumnConst<ColumnTuple>(c0.column.get());
        const auto * y_const = checkAndGetColumnConst<ColumnTuple>(c1.column.get());

        Columns x_columns;
        Columns y_columns;

        if (x_const)
            x_columns = convertConstTupleToConstantElements(*x_const);
        else
            x_columns = static_cast<const ColumnTuple &>(*c0.column).getColumns();

        if (y_const)
            y_columns = convertConstTupleToConstantElements(*y_const);
        else
            y_columns = static_cast<const ColumnTuple &>(*c1.column).getColumns();

        for (size_t i = 0; i < tuple_size; ++i)
        {
            x[i].type = static_cast<const DataTypeTuple &>(*c0.type).getElements()[i];
            y[i].type = static_cast<const DataTypeTuple &>(*c1.type).getElements()[i];

            x[i].column = x_columns[i];
            y[i].column = y_columns[i];
        }

        executeTupleImpl(block, result, x, y, tuple_size);
    }

    void executeTupleImpl(
        Block & block,
        size_t result,
        const ColumnsWithTypeAndName & x,
        const ColumnsWithTypeAndName & y,
        size_t tuple_size) const;

    template <typename ComparisonFunction, typename ConvolutionFunction>
    void executeTupleEqualityImpl(
        Block & block,
        size_t result,
        const ColumnsWithTypeAndName & x,
        const ColumnsWithTypeAndName & y,
        size_t tuple_size) const
    {
        DefaultExecutable func_compare(std::make_shared<ComparisonFunction>());
        DefaultExecutable func_convolution(std::make_shared<ConvolutionFunction>());

        Block tmp_block;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            tmp_block.insert(x[i]);
            tmp_block.insert(y[i]);

            /// Comparison of the elements.
            tmp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});
            func_compare.execute(tmp_block, {i * 3, i * 3 + 1}, i * 3 + 2);
        }

        /// Logical convolution.
        tmp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});

        ColumnNumbers convolution_args(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
            convolution_args[i] = i * 3 + 2;

        func_convolution.execute(tmp_block, convolution_args, tuple_size * 3);
        block.getByPosition(result).column = tmp_block.getByPosition(tuple_size * 3).column;
    }

    template <typename HeadComparisonFunction, typename TailComparisonFunction>
    void executeTupleLessGreaterImpl(
        Block & block,
        size_t result,
        const ColumnsWithTypeAndName & x,
        const ColumnsWithTypeAndName & y,
        size_t tuple_size) const
    {
        DefaultExecutable func_compare_head(std::make_shared<HeadComparisonFunction>());
        DefaultExecutable func_compare_tail(std::make_shared<TailComparisonFunction>());
        DefaultExecutable func_and(std::make_shared<FunctionAnd>());
        DefaultExecutable func_or(std::make_shared<FunctionOr>());
        DefaultExecutable func_equals(std::make_shared<FunctionComparison<EqualsOp, NameEquals>>());

        Block tmp_block;

        /// Pairwise comparison of the inequality of all elements; on the equality of all elements except the last.
        for (size_t i = 0; i < tuple_size; ++i)
        {
            tmp_block.insert(x[i]);
            tmp_block.insert(y[i]);

            tmp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});

            if (i + 1 != tuple_size)
            {
                func_compare_head.execute(tmp_block, {i * 4, i * 4 + 1}, i * 4 + 2);

                tmp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});
                func_equals.execute(tmp_block, {i * 4, i * 4 + 1}, i * 4 + 3);
            }
            else
                func_compare_tail.execute(tmp_block, {i * 4, i * 4 + 1}, i * 4 + 2);
        }

        /// Combination. Complex code - make a drawing. It can be replaced by a recursive comparison of tuples.
        size_t i = tuple_size - 1;
        while (i > 0)
        {
            tmp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});
            func_and.execute(tmp_block, {tmp_block.columns() - 2, (i - 1) * 4 + 3}, tmp_block.columns() - 1);
            tmp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});
            func_or.execute(tmp_block, {tmp_block.columns() - 2, (i - 1) * 4 + 2}, tmp_block.columns() - 1);
            --i;
        }

        block.getByPosition(result).column = tmp_block.getByPosition(tmp_block.columns() - 1).column;
    }

    void executeGeneric(Block & block, size_t result, const IColumn * c0, const IColumn * c1) const
    {
        bool c0_const = c0->isColumnConst();
        bool c1_const = c1->isColumnConst();

        if (c0_const && c1_const)
        {
            UInt8 res = 0;
            GenericComparisonImpl<Op<int, int>>::constantConstant(*c0, *c1, res);
            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(c0->size(), toField(res));
        }
        else
        {
            auto c_res = ColumnUInt8::create();
            ColumnUInt8::Container & vec_res = c_res->getData();
            vec_res.resize(c0->size());

            if (c0_const)
                GenericComparisonImpl<Op<int, int>>::constantVector(*c0, *c1, vec_res);
            else if (c1_const)
                GenericComparisonImpl<Op<int, int>>::vectorConstant(*c0, *c1, vec_res);
            else
                GenericComparisonImpl<Op<int, int>>::vectorVector(*c0, *c1, vec_res);

            block.getByPosition(result).column = std::move(c_res);
        }
    }

    bool executeDateWithDateTimeOrDateTimeWithDate(
        Block & block,
        size_t result,
        const IColumn * col_left_untyped,
        const IColumn * col_right_untyped,
        const DataTypePtr & left_type,
        const DataTypePtr & right_type) const
    {
        if ((checkDataType<DataTypeDate>(left_type.get()) && checkDataType<DataTypeDateTime>(right_type.get()))
            || (checkDataType<DataTypeDateTime>(left_type.get()) && checkDataType<DataTypeDate>(right_type.get())))
        {
            bool is_left_date = checkDataType<DataTypeDate>(left_type.get());
            if (is_left_date)
            {
                return executeDateAndDateTimeCompare<UInt32, Int64, true>(
                    block,
                    result,
                    col_left_untyped,
                    col_right_untyped);
            }
            else
            {
                return executeDateAndDateTimeCompare<Int64, UInt32, false>(
                    block,
                    result,
                    col_left_untyped,
                    col_right_untyped);
            }
        }
        return false;
    }

    template <typename T0, typename T1, bool is_left_date>
    bool executeDateAndDateTimeCompare(Block & block, size_t result, const IColumn * c0, const IColumn * c1) const
    {
        bool c0_const = c0->isColumnConst();
        bool c1_const = c1->isColumnConst();

        if (c0_const && c1_const)
        {
            UInt8 res = 0;
            DateDateTimeComparisonImpl<T0, T1, Op, is_left_date>::constantConstant(
                checkAndGetColumnConst<ColumnVector<T0>>(c0)->template getValue<T0>(),
                checkAndGetColumnConst<ColumnVector<T1>>(c1)->template getValue<T1>(),
                res);
            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(c0->size(), toField(res));
        }
        else
        {
            auto c_res = ColumnUInt8::create();
            ColumnUInt8::Container & vec_res = c_res->getData();
            vec_res.resize(c0->size());
            if (c0_const)
            {
                DateDateTimeComparisonImpl<T0, T1, Op, is_left_date>::constantVector(
                    checkAndGetColumnConst<ColumnVector<T0>>(c0)->template getValue<T0>(),
                    checkAndGetColumn<ColumnVector<T1>>(c1)->getData(),
                    vec_res);
            }
            else if (c1_const)
            {
                DateDateTimeComparisonImpl<T0, T1, Op, is_left_date>::vectorConstant(
                    checkAndGetColumn<ColumnVector<T0>>(c0)->getData(),
                    checkAndGetColumnConst<ColumnVector<T1>>(c1)->template getValue<T1>(),
                    vec_res);
            }
            else
            {
                DateDateTimeComparisonImpl<T0, T1, Op, true>::vectorVector(
                    checkAndGetColumn<ColumnVector<T0>>(c0)->getData(),
                    checkAndGetColumn<ColumnVector<T1>>(c1)->getData(),
                    vec_res);
            }
            block.getByPosition(result).column = std::move(c_res);
        }
        return true;
    }

    TiDB::TiDBCollatorPtr collator = nullptr;

public:
    String getName() const override { return name; }

    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { collator = collator_; }

    size_t getNumberOfArguments() const override { return 2; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        bool left_is_date = false;
        bool left_is_date_time = false;
        bool left_is_enum8 = false;
        bool left_is_enum16 = false;
        bool left_is_string = false;
        bool left_is_fixed_string = false;
        const DataTypeTuple * left_tuple = nullptr;

        (left_is_date = checkAndGetDataType<DataTypeDate>(arguments[0].get())
             || checkAndGetDataType<DataTypeMyDate>(arguments[0].get()))
            || (left_is_date_time = checkAndGetDataType<DataTypeDateTime>(arguments[0].get())
                    || checkAndGetDataType<DataTypeMyDateTime>(arguments[0].get()))
            || (left_is_enum8 = checkAndGetDataType<DataTypeEnum8>(arguments[0].get()))
            || (left_is_enum16 = checkAndGetDataType<DataTypeEnum16>(arguments[0].get()))
            || (left_is_string = checkAndGetDataType<DataTypeString>(arguments[0].get()))
            || (left_is_fixed_string = checkAndGetDataType<DataTypeFixedString>(arguments[0].get()))
            || (left_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].get()));

        const bool left_is_enum = left_is_enum8 || left_is_enum16;

        bool right_is_date = false;
        bool right_is_date_time = false;
        bool right_is_enum8 = false;
        bool right_is_enum16 = false;
        bool right_is_string = false;
        bool right_is_fixed_string = false;
        const DataTypeTuple * right_tuple = nullptr;

        (right_is_date = checkAndGetDataType<DataTypeDate>(arguments[1].get())
             || checkAndGetDataType<DataTypeMyDate>(arguments[1].get()))
            || (right_is_date_time = checkAndGetDataType<DataTypeDateTime>(arguments[1].get())
                    || checkAndGetDataType<DataTypeMyDateTime>(arguments[1].get()))
            || (right_is_enum8 = checkAndGetDataType<DataTypeEnum8>(arguments[1].get()))
            || (right_is_enum16 = checkAndGetDataType<DataTypeEnum16>(arguments[1].get()))
            || (right_is_string = checkAndGetDataType<DataTypeString>(arguments[1].get()))
            || (right_is_fixed_string = checkAndGetDataType<DataTypeFixedString>(arguments[1].get()))
            || (right_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].get()));

        const bool right_is_enum = right_is_enum8 || right_is_enum16;

        if (!((arguments[0]->isValueRepresentedByNumber() && arguments[1]->isValueRepresentedByNumber())
              || ((left_is_string || left_is_fixed_string) && (right_is_string || right_is_fixed_string))
              || (left_is_date && right_is_date)
              || (left_is_date
                  && right_is_string) /// You can compare the date, datetime and an enumeration with a constant string.
              || (left_is_string && right_is_date) || (left_is_date_time && right_is_date_time)
              || (left_is_date_time && right_is_string) || (left_is_string && right_is_date_time)
              || (left_is_enum && right_is_enum
                  && arguments[0]->getName()
                      == arguments[1]->getName()) /// only equivalent enum type values can be compared against
              || (left_is_enum && right_is_string) || (left_is_string && right_is_enum)
              || (left_tuple && right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
              || (arguments[0]->equals(*arguments[1]))))
            throw Exception(
                fmt::format(
                    "Illegal types of arguments ({}, {}) of function {}",
                    arguments[0]->getName(),
                    arguments[1]->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (left_tuple && right_tuple)
        {
            size_t size = left_tuple->getElements().size();
            for (size_t i = 0; i < size; ++i)
            {
                ColumnsWithTypeAndName args
                    = {{nullptr, left_tuple->getElements()[i], ""}, {nullptr, right_tuple->getElements()[i], ""}};
                IFunction::getReturnTypeImpl(args);
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeDecimal(
        Block & block,
        size_t result,
        const ColumnWithTypeAndName & col_left,
        const ColumnWithTypeAndName & col_right) const
    {
        TypeIndex left_number = col_left.type->getTypeId();
        TypeIndex right_number = col_right.type->getTypeId();

        auto call = [&](const auto & types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;

            DecimalComparison<LeftDataType, RightDataType, Op, true>(block, result, col_left, col_right);
            return true;
        };

        if (!callOnBasicTypes<true, true, true, false>(left_number, right_number, call))
            throw Exception(
                fmt::format(
                    "Wrong call for {} with {} and {}",
                    getName(),
                    col_left.type->getName(),
                    col_right.type->getName()),
                ErrorCodes::LOGICAL_ERROR);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto & col_with_type_and_name_left = block.getByPosition(arguments[0]);
        const auto & col_with_type_and_name_right = block.getByPosition(arguments[1]);
        const IColumn * col_left_untyped = col_with_type_and_name_left.column.get();
        const IColumn * col_right_untyped = col_with_type_and_name_right.column.get();

        const bool left_is_num = col_left_untyped->isNumeric();
        const bool right_is_num = col_right_untyped->isNumeric();

        if (left_is_num && right_is_num)
        {
            if (!(executeDateWithDateTimeOrDateTimeWithDate(
                      block,
                      result,
                      col_left_untyped,
                      col_right_untyped,
                      col_with_type_and_name_left.type,
                      col_with_type_and_name_right.type)
                  || executeNumLeftType<UInt8>(block, result, col_left_untyped, col_right_untyped)
                  || executeNumLeftType<UInt16>(block, result, col_left_untyped, col_right_untyped)
                  || executeNumLeftType<UInt32>(block, result, col_left_untyped, col_right_untyped)
                  || executeNumLeftType<UInt64>(block, result, col_left_untyped, col_right_untyped)
                  || executeNumLeftType<Int8>(block, result, col_left_untyped, col_right_untyped)
                  || executeNumLeftType<Int16>(block, result, col_left_untyped, col_right_untyped)
                  || executeNumLeftType<Int32>(block, result, col_left_untyped, col_right_untyped)
                  || executeNumLeftType<Int64>(block, result, col_left_untyped, col_right_untyped)
                  || executeNumLeftType<Float32>(block, result, col_left_untyped, col_right_untyped)
                  || executeNumLeftType<Float64>(block, result, col_left_untyped, col_right_untyped)))
                throw Exception(
                    fmt::format(
                        "Illegal column {} of first argument of function {}",
                        col_left_untyped->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (
            IsDecimalDataType(col_with_type_and_name_left.type) || IsDecimalDataType(col_with_type_and_name_right.type))
            executeDecimal(block, result, col_with_type_and_name_left, col_with_type_and_name_right);
        else if (checkAndGetDataType<DataTypeTuple>(col_with_type_and_name_left.type.get()))
            executeTuple(block, result, col_with_type_and_name_left, col_with_type_and_name_right);
        else if (!left_is_num && !right_is_num && executeString(block, result, col_left_untyped, col_right_untyped))
            ;
        else if (col_with_type_and_name_left.type->equals(*col_with_type_and_name_right.type))
            executeGeneric(block, result, col_left_untyped, col_right_untyped);
        else
            executeDateOrDateTimeOrEnumWithConstString(
                block,
                result,
                col_left_untyped,
                col_right_untyped,
                col_with_type_and_name_left.type,
                col_with_type_and_name_right.type,
                left_is_num);
    }
};

using StrcmpReturnColumnType = ColumnInt8;
class FunctionStrcmp : public FunctionComparison<CmpOp, NameStrcmp, StrcmpReturnColumnType>
{
public:
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionStrcmp>(); };

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const IColumn * col_left_untyped = block.getByPosition(arguments[0]).column.get();
        const IColumn * col_right_untyped = block.getByPosition(arguments[1]).column.get();

        bool success = executeString(block, result, col_left_untyped, col_right_untyped);
        if (!success)
        {
            throw Exception(fmt::format(
                "Function {} executed on invalid arguments [col_left={}] [col_right={}]",
                getName(),
                col_left_untyped->getName(),
                col_right_untyped->getName()));
        }
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeInt8>(); }
};

template <bool with_null_>
struct IsTrueTrait
{
    static constexpr bool with_null = with_null_;
    static constexpr auto name = with_null ? "isTrueWithNull" : "isTrue";

    template <typename T>
    static constexpr Int64 apply(T value)
    {
        if constexpr (IsDecimal<T>)
            return value.value == 0 ? 0 : 1;
        else
            return value == 0 ? 0 : 1;
    }
};

template <bool with_null_>
struct IsFalseTrait
{
    static constexpr bool with_null = with_null_;
    static constexpr auto name = with_null ? "isFalseWithNull" : "isFalse";

    template <typename T>
    static constexpr Int64 apply(T value)
    {
        if constexpr (IsDecimal<T>)
            return value.value == 0 ? 1 : 0;
        else
            return value == 0 ? 1 : 0;
    }
};

/// Implements the function isTrue/isTrueWithNull/isFalse/isFalseWithNull.
template <typename Trait>
class FunctionIsTrueFalse : public IFunction
{
public:
    static constexpr auto name = Trait::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIsTrueFalse<Trait>>(); };

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto type = std::make_shared<DataTypeInt64>();
        if (Trait::with_null && arguments[0]->isNullable())
            return makeNullable(type);
        return type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnWithTypeAndName & src = block.getByPosition(arguments[0]);

        auto & result_col = block.getByPosition(result);
        if (src.column->onlyNull())
        {
            if (result_col.type->isNullable())
                result_col.column = result_col.type->createColumnConst(src.column->size(), Null());
            else
                result_col.column = result_col.type->createColumnConst(src.column->size(), Int64(0));
            return;
        }

        auto src_type = removeNullable(src.type);
        ColumnPtr res_col;
        switch (src_type->getTypeId())
        {
        case TypeIndex::UInt8:
            res_col = executeVec<ColumnVector<UInt8>>(src.column);
            break;
        case TypeIndex::Int8:
            res_col = executeVec<ColumnVector<Int8>>(src.column);
            break;
        case TypeIndex::UInt16:
            res_col = executeVec<ColumnVector<UInt16>>(src.column);
            break;
        case TypeIndex::Int16:
            res_col = executeVec<ColumnVector<Int16>>(src.column);
            break;
        case TypeIndex::UInt32:
            res_col = executeVec<ColumnVector<UInt32>>(src.column);
            break;
        case TypeIndex::Int32:
            res_col = executeVec<ColumnVector<Int32>>(src.column);
            break;
        case TypeIndex::UInt64:
            res_col = executeVec<ColumnVector<UInt64>>(src.column);
            break;
        case TypeIndex::Int64:
            res_col = executeVec<ColumnVector<Int64>>(src.column);
            break;
        case TypeIndex::Float32:
            res_col = executeVec<ColumnVector<Float32>>(src.column);
            break;
        case TypeIndex::Float64:
            res_col = executeVec<ColumnVector<Float64>>(src.column);
            break;
        case TypeIndex::Decimal32:
            res_col = executeVec<ColumnDecimal<Decimal32>>(src.column);
            break;
        case TypeIndex::Decimal64:
            res_col = executeVec<ColumnDecimal<Decimal64>>(src.column);
            break;
        case TypeIndex::Decimal128:
            res_col = executeVec<ColumnDecimal<Decimal128>>(src.column);
            break;
        case TypeIndex::Decimal256:
            res_col = executeVec<ColumnDecimal<Decimal256>>(src.column);
            break;
        default:
            throw Exception(
                fmt::format("Illegal type {} of function {}", src.type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (result_col.type->isNullable())
        {
            res_col = ColumnNullable::create(
                res_col,
                static_cast<const ColumnNullable &>(*src.column).getNullMapColumnPtr());
        }

        result_col.column = res_col;
    }

private:
    template <typename VecT>
    static ColumnPtr executeVec(const ColumnPtr & src)
    {
        auto [src_col, src_nullmap] = removeNullable(src.get());
        const auto & src_vec_col = assert_cast<const VecT &>(*src_col);
        const auto & src_vec = src_vec_col.getData();
        size_t rows = src_vec.size();

        auto res_col = ColumnInt64::create();
        ColumnInt64::Container & res_vec = res_col->getData();
        res_vec.resize(rows);

        for (size_t i = 0; i < rows; ++i)
            res_vec[i] = Trait::apply(src_vec[i]);

        if (src_nullmap)
        {
            assert(src_nullmap->size() == rows);
            for (size_t i = 0; i < rows; ++i)
                res_vec[i] &= !(*src_nullmap)[i];
        }

        return res_col;
    }
};

using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;
using FunctionNotEquals = FunctionComparison<NotEqualsOp, NameNotEquals>;
using FunctionLess = FunctionComparison<LessOp, NameLess>;
using FunctionGreater = FunctionComparison<GreaterOp, NameGreater>;
using FunctionLessOrEquals = FunctionComparison<LessOrEqualsOp, NameLessOrEquals>;
using FunctionGreaterOrEquals = FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>;
using FunctionIsTrue = FunctionIsTrueFalse<IsTrueTrait<false>>;
using FunctionIsTrueWithNull = FunctionIsTrueFalse<IsTrueTrait<true>>;
using FunctionIsFalse = FunctionIsTrueFalse<IsFalseTrait<false>>;
using FunctionIsFalseWithNull = FunctionIsTrueFalse<IsFalseTrait<true>>;

} // namespace DB
