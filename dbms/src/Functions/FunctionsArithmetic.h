#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitors.h>
#include <Common/typeid_cast.h>
#include <Core/AccurateComparison.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <common/intExp.h>

#include <boost/math/common_factor.hpp>
#include <ext/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_DIVISION;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
    extern const int DECIMAL_OVERFLOW;
}

//
/// this one is just for convenience
template <bool B, typename T1, typename T2> using If = std::conditional_t<B, T1, T2>;

/** Arithmetic operations: +, -, *, /, %,
  * intDiv (integer division), unary minus.
  * Bitwise operations: |, &, ^, ~.
  * Etc.
  */

template <typename A, typename B, typename Op, typename ResultType_ = typename Op::ResultType>
struct BinaryOperationImplBase
{
    using ResultType = ResultType_;
    using ColVecA = std::conditional_t<IsDecimal<A>, ColumnDecimal<A>, ColumnVector<A>>;
    using ColVecB = std::conditional_t<IsDecimal<B>, ColumnDecimal<B>, ColumnVector<B>>;
    using ArrayA = typename ColVecA::Container;
    using ArrayB = typename ColVecB::Container;

    static void NO_INLINE vector_vector(const ArrayA & a, const ArrayB & b, PaddedPODArray<ResultType> & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            if constexpr (IsDecimal<A> && IsDecimal<B>)
                c[i] = Op::template apply<ResultType>(DecimalField<A>(a[i], a.getScale()), DecimalField<B>(b[i], b.getScale()));
            else if constexpr (IsDecimal<A>)
                c[i] = Op::template apply<ResultType>(DecimalField<A>(a[i], a.getScale()), b[i]);
            else if constexpr (IsDecimal<B>)
                c[i] = Op::template apply<ResultType>(a[i], DecimalField<B>(b[i], b.getScale()));
            else
                c[i] = Op::template apply<ResultType>(a[i], b[i]);
    }

    static void NO_INLINE vector_vector_nullable(const ArrayA & a, const ColumnUInt8 * a_nullmap, const ArrayB & b, const ColumnUInt8 * b_nullmap,
        PaddedPODArray<ResultType> & c, typename ColumnUInt8::Container & res_null)
    {
        size_t size = a.size();
        if (a_nullmap != nullptr && b_nullmap != nullptr)
        {
            auto & a_nullmap_data = a_nullmap->getData();
            auto & b_nullmap_data = b_nullmap->getData();
            for (size_t i = 0; i < size; i++)
                res_null[i] = a_nullmap_data[i] || b_nullmap_data[i];
        }
        else if (a_nullmap != nullptr || b_nullmap != nullptr)
        {
            auto & nullmap_data = a_nullmap != nullptr ? a_nullmap->getData() : b_nullmap->getData();
            for (size_t i = 0; i < size; i++)
                res_null[i] = nullmap_data[i];
        }
        for (size_t i = 0; i < size; ++i)
        {
            if constexpr (IsDecimal<A> && IsDecimal<B>)
                c[i] = Op::template apply<ResultType>(DecimalField<A>(a[i], a.getScale()), DecimalField<B>(b[i], b.getScale()), res_null[i]);
            else if constexpr (IsDecimal<A>)
                c[i] = Op::template apply<ResultType>(DecimalField<A>(a[i], a.getScale()), b[i], res_null[i]);
            else if constexpr (IsDecimal<B>)
                c[i] = Op::template apply<ResultType>(a[i], DecimalField<B>(b[i], b.getScale()), res_null[i]);
            else
                c[i] = Op::template apply<ResultType>(a[i], b[i], res_null[i]);
        }
    }

    static void NO_INLINE vector_constant(const ArrayA & a, typename NearestFieldType<B>::Type b, PaddedPODArray<ResultType> & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            if constexpr(IsDecimal<A>)
                c[i] = Op::template apply<ResultType>(DecimalField<A>(a[i], a.getScale()), b);
            else
                c[i] = Op::template apply<ResultType>(a[i], b);
    }

    static void NO_INLINE vector_constant_nullable(const ArrayA & a, const ColumnUInt8 * a_nullmap, typename NearestFieldType<B>::Type b,
        PaddedPODArray<ResultType> & c, typename ColumnUInt8::Container & res_null)
    {
        size_t size = a.size();
        if (a_nullmap != nullptr)
        {
            auto & nullmap_data = a_nullmap->getData();
            for (size_t i = 0; i < size; ++i)
                res_null[i] = nullmap_data[i];
        }
        for (size_t i = 0; i < size; ++i)
            if constexpr(IsDecimal<A>)
                c[i] = Op::template apply<ResultType>(DecimalField<A>(a[i], a.getScale()), b, res_null[i]);
            else
                c[i] = Op::template apply<ResultType>(a[i], b, res_null[i]);
    }

    static void NO_INLINE constant_vector(typename NearestFieldType<A>::Type a, const ArrayB & b, PaddedPODArray<ResultType> & c)
    {
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i) {
            if constexpr(IsDecimal<B>)
                c[i] = Op::template apply<ResultType>(a, DecimalField<B>(b[i], b.getScale()));
            else
                c[i] = Op::template apply<ResultType>(a, b[i]);
        }
    }

    static void NO_INLINE constant_vector_nullable(typename NearestFieldType<A>::Type a, const ArrayB & b, const ColumnUInt8 * b_nullmap,
        PaddedPODArray<ResultType> & c, typename ColumnUInt8::Container & res_null)
    {
        size_t size = b.size();
        if (b_nullmap != nullptr)
        {
            auto & nullmap_data = b_nullmap->getData();
            for (size_t i = 0; i < size; i++)
                res_null[i] = nullmap_data[i];
        }
        for (size_t i = 0; i < size; ++i) {
            if constexpr(IsDecimal<B>)
                c[i] = Op::template apply<ResultType>(a, DecimalField<B>(b[i], b.getScale()), res_null[i]);
            else
                c[i] = Op::template apply<ResultType>(a, b[i], res_null[i]);
        }
    }

    static ResultType constant_constant(typename NearestFieldType<A>::Type a, typename NearestFieldType<B>::Type b)
    {
        return Op::template apply<ResultType>(a, b);
    }
    static ResultType constant_constant_nullable(typename NearestFieldType<A>::Type a, typename NearestFieldType<B>::Type b, UInt8 & res_null)
    {
        return Op::template apply<ResultType>(a, b, res_null);
    }
};

template <typename A, typename B, typename Op, typename ResultType = typename Op::ResultType>
struct BinaryOperationImpl : BinaryOperationImplBase<A, B, Op, ResultType>
{
};

template <typename A, typename Op>
struct UnaryOperationImpl
{
    using ResultType = typename Op::ResultType;

    static void NO_INLINE vector(const PaddedPODArray<A> & a, PaddedPODArray<ResultType> & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::apply(a[i]);
    }

    static void constant(A a, ResultType & c)
    {
        c = Op::apply(a);
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct PlusImpl;
template <typename A, typename B>
struct PlusImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        /// Next everywhere, static_cast - so that there is no wrong result in expressions of the form Int64 c = UInt32(a) * Int32(-1).
        return static_cast<Result>(a) + b;
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct PlusImpl<A,B,true>
{
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;
    using ResultPrecInferer = PlusDecimalInferer;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) + static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct MultiplyImpl;

template <typename A, typename B>
struct MultiplyImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) * b;
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct MultiplyImpl<A,B,true>
{
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;

    using ResultPrecInferer = MulDecimalInferer;
    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) * static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct MinusImpl;

template <typename A, typename B>
struct MinusImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfSubtraction<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) - b;
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct MinusImpl<A,B,true>
{
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;
    using ResultPrecInferer = PlusDecimalInferer;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) - static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct DivideFloatingImpl;
template <typename A, typename B>
struct DivideFloatingImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) / b;
    }

    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct DivideFloatingImpl<A,B,true>
{
    using ResultPrecInferer = DivDecimalInferer;
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) / static_cast<Result>(b);
    }

    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct TiDBDivideFloatingImpl;
template <typename A, typename B>
struct TiDBDivideFloatingImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) / b;
    }
    template <typename Result = ResultType>
    static inline Result apply(A a, B b, UInt8 & res_null)
    {
        if (b == 0)
        {
            /// we can check res_null to see if it is DIVISION_BY_ZERO or DIVISION_BY_NULL, when sql mode is ERROR_FOR_DIVISION_BY_ZERO,
            /// inserts and updates involving expressions that perform division by zero should be treated as errors, now only read-only
            /// statement will send to TiFlash, so just return NULL here
            res_null = 1;
            return static_cast<Result>(0);
        }
        return static_cast<Result>(a) / b;
    }
};

template <typename A, typename B>
struct TiDBDivideFloatingImpl<A,B,true>
{
    using ResultPrecInferer = DivDecimalInferer;
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) / static_cast<Result>(b);
    }

    template <typename Result = ResultType>
    static inline Result apply(A a, B b, UInt8 & res_null)
    {
        if (static_cast<Result>(b) == static_cast<Result>(0))
        {
            /// we can check res_null to see if it is DIVISION_BY_ZERO or DIVISION_BY_NULL, when sql mode is ERROR_FOR_DIVISION_BY_ZERO,
            /// inserts and updates involving expressions that perform division by zero should be treated as errors, now only read-only
            /// statement will send to TiFlash, so just return NULL here
            res_null = 1;
            return static_cast<Result>(0);
        }
        return static_cast<Result>(a) / static_cast<Result>(b);
    }
};

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

template <typename A, typename B>
inline void throwIfDivisionLeadsToFPE(A a, B b)
{
    /// Is it better to use siglongjmp instead of checks?

    if (unlikely(b == 0))
        throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

    /// http://avva.livejournal.com/2548306.html
    if (unlikely(std::is_signed_v<A> && std::is_signed_v<B> && a == std::numeric_limits<A>::min() && b == -1))
        throw Exception("Division of minimal signed number by minus one", ErrorCodes::ILLEGAL_DIVISION);
}

template <typename A, typename B>
inline bool divisionLeadsToFPE(A a, B b)
{
    if (unlikely(b == 0))
        return true;

    if (unlikely(std::is_signed_v<A> && std::is_signed_v<B> && a == std::numeric_limits<A>::min() && b == -1))
        return true;

    return false;
}


#pragma GCC diagnostic pop


template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct DivideIntegralImpl;
template <typename A, typename B>
struct DivideIntegralImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(a, b);
        return static_cast<Result>(a) / static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct DivideIntegralImpl<A,B,true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        Result x, y;
        if constexpr (IsDecimal<A>) {
            x = static_cast<Result>(a.value);
        } else {
            x = static_cast<Result>(a);
        }
        if constexpr (IsDecimal<B>) {
            y = static_cast<Result>(b.value);
        } else {
            y = static_cast<Result>(b);
        }
        throwIfDivisionLeadsToFPE(x, y);
        return x / y;
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct DivideIntegralOrZeroImpl;
template <typename A, typename B>
struct DivideIntegralOrZeroImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(unlikely(divisionLeadsToFPE(a, b)) ? 0 : static_cast<Result>(a) / static_cast<Result>(b));
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct DivideIntegralOrZeroImpl<A,B,true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        Result x, y;
        if constexpr (IsDecimal<A>) {
            x = static_cast<Result>(a.value);
        } else {
            x = static_cast<Result>(a);
        }
        if constexpr (IsDecimal<B>) {
            y = static_cast<Result>(b.value);
        } else {
            y = static_cast<Result>(b);
        }
        throwIfDivisionLeadsToFPE(x, y);
        return unlikely(divisionLeadsToFPE(x, y)) ? 0 : x / y;
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct ModuloImpl;
template <typename A, typename B>
struct ModuloImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
        return static_cast<Result>( typename NumberTraits::ToInteger<A>::Type(a)
            % typename NumberTraits::ToInteger<B>::Type(b));
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct ModuloImpl<A,B,true>
{
    using ResultPrecInferer = ModDecimalInferer;
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        Result x, y;
        if constexpr (IsDecimal<A>) {
            x = static_cast<Result>(a.value);
        } else {
            x = static_cast<Result>(a);
        }
        if constexpr (IsDecimal<B>) {
            y = static_cast<Result>(b.value);
        } else {
            y = static_cast<Result>(b);
        }
        return ModuloImpl<Result, Result>::apply(x, y);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct BitAndImpl;
template <typename A, typename B>
struct BitAndImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a)
            & static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BitAndImpl<A,B,true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        Result x, y;
        if constexpr (IsDecimal<A>) {
            x = static_cast<Result>(a.value);
        } else {
            x = static_cast<Result>(a);
        }
        if constexpr (IsDecimal<B>) {
            y = static_cast<Result>(b.value);
        } else {
            y = static_cast<Result>(b);
        }
        return BitAndImpl<Result, Result>::apply(x, y);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct BitOrImpl;
template <typename A, typename B>
struct BitOrImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a)
            | static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BitOrImpl<A,B,true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        Result x, y;
        if constexpr (IsDecimal<A>) {
            x = static_cast<Result>(a.value);
        } else {
            x = static_cast<Result>(a);
        }
        if constexpr (IsDecimal<B>) {
            y = static_cast<Result>(b.value);
        } else {
            y = static_cast<Result>(b);
        }
        return BitOrImpl<Result, Result>::apply(x, y);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct BitXorImpl;
template <typename A, typename B>
struct BitXorImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a)
            ^ static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BitXorImpl<A,B,true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        Result x, y;
        if constexpr (IsDecimal<A>) {
            x = static_cast<Result>(a.value);
        } else {
            x = static_cast<Result>(a);
        }
        if constexpr (IsDecimal<B>) {
            y = static_cast<Result>(b.value);
        } else {
            y = static_cast<Result>(b);
        }
        return BitXorImpl<Result, Result>::apply(x, y);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct BitShiftLeftImpl;
template <typename A, typename B>
struct BitShiftLeftImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a)
            << static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BitShiftLeftImpl<A,B,true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        Result x, y;
        if constexpr (IsDecimal<A>) {
            x = static_cast<Result>(a.value);
        } else {
            x = static_cast<Result>(a);
        }
        if constexpr (IsDecimal<B>) {
            y = static_cast<Result>(b.value);
        } else {
            y = static_cast<Result>(b);
        }
        return BitShiftLeftImpl<Result, Result>::apply(x, y);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct BitShiftRightImpl;
template <typename A, typename B>
struct BitShiftRightImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a)
            >> static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BitShiftRightImpl<A,B,true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        Result x, y;
        if constexpr (IsDecimal<A>) {
            x = static_cast<Result>(a.value);
        } else {
            x = static_cast<Result>(a);
        }
        if constexpr (IsDecimal<B>) {
            y = static_cast<Result>(b.value);
        } else {
            y = static_cast<Result>(b);
        }
        return BitShiftRightImpl<Result, Result>::apply(x, y);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct BitRotateLeftImpl;
template <typename A, typename B>
struct BitRotateLeftImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return (static_cast<Result>(a) << static_cast<Result>(b))
            | (static_cast<Result>(a) >> ((sizeof(Result) * 8) - static_cast<Result>(b)));
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BitRotateLeftImpl<A,B,true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        Result x, y;
        if constexpr (IsDecimal<A>) {
            x = static_cast<Result>(a.value);
        } else {
            x = static_cast<Result>(a);
        }
        if constexpr (IsDecimal<B>) {
            y = static_cast<Result>(b.value);
        } else {
            y = static_cast<Result>(b);
        }
        return BitRotateLeftImpl<Result, Result>::apply(x, y);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct BitRotateRightImpl;
template <typename A, typename B>
struct BitRotateRightImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return (static_cast<Result>(a) >> static_cast<Result>(b))
            | (static_cast<Result>(a) << ((sizeof(Result) * 8) - static_cast<Result>(b)));
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BitRotateRightImpl<A,B,true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        Result x, y;
        if constexpr (IsDecimal<A>) {
            x = static_cast<Result>(a.value);
        } else {
            x = static_cast<Result>(a);
        }
        if constexpr (IsDecimal<B>) {
            y = static_cast<Result>(b.value);
        } else {
            y = static_cast<Result>(b);
        }
        return BitRotateRightImpl<Result, Result>::apply(x, y);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename T>
std::enable_if_t<std::is_integral_v<T> || std::is_same_v<T, Int128> || std::is_same_v<T, Int256>, T> toInteger(T x) { return x; }

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, Int64> toInteger(T x) { return Int64(x); }

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct BitTestImpl;
template <typename A, typename B>
struct BitTestImpl<A,B,false>
{
    using ResultType = UInt8;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b) { return static_cast<Result>( (toInteger(a) >> static_cast<int64_t>(toInteger(b))) & 1 ); };
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BitTestImpl<A,B,true>
{
    using ResultType = UInt8;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        if constexpr(!IsDecimal<B>) {
            return BitTestImpl<Result, Result>::apply(static_cast<int64_t>(a.value), b);
        }
        else if constexpr(!IsDecimal<A>) {
            return BitTestImpl<Result, Result>::apply(a, static_cast<int64_t>(b.value));
        }
        else
            return BitTestImpl<Result, Result>::apply(static_cast<int64_t>(a.value), static_cast<int64_t>(b.value));
        return {};
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct LeastBaseImpl;
template <typename A, typename B>
struct LeastBaseImpl<A,B,false>
{
    using ResultType = NumberTraits::ResultOfLeast<A, B>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        /** gcc 4.9.2 successfully vectorizes a loop from this function. */
        return static_cast<Result>(a) < static_cast<Result>(b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template<typename A, typename B>
struct LeastBaseImpl<A,B,true>
{
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;
    using ResultPrecInferer = PlusDecimalInferer;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) < static_cast<Result>(b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct LeastSpecialImpl
{
    using ResultType = std::make_signed_t<A>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        static_assert(std::is_same_v<Result, ResultType>, "ResultType != Result");
        return accurate::lessOp(a, b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
using LeastImpl = std::conditional_t<!NumberTraits::LeastGreatestSpecialCase<A, B>, LeastBaseImpl<A, B>, LeastSpecialImpl<A, B>>;


template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct GreatestBaseImpl;
template<typename A, typename B>
struct GreatestBaseImpl<A,B,false>
{
    using ResultType = NumberTraits::ResultOfGreatest<A, B>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) > static_cast<Result>(b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template<typename A, typename B>
struct GreatestBaseImpl<A,B,true>
{
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;
    using ResultPrecInferer = PlusDecimalInferer;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) > static_cast<Result>(b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct GreatestSpecialImpl
{
    using ResultType = std::make_unsigned_t<A>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        static_assert(std::is_same_v<Result, ResultType>, "ResultType != Result");
        return accurate::greaterOp(a, b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
using GreatestImpl = std::conditional_t<!NumberTraits::LeastGreatestSpecialCase<A, B>, GreatestBaseImpl<A, B>, GreatestSpecialImpl<A, B>>;


template <typename A>
struct NegateImpl
{
    using ResultType = typename NumberTraits::ResultOfNegate<A>::Type;

    static inline ResultType apply(A a)
    {
        if constexpr (IsDecimal<A>) {
            return static_cast<ResultType>(-a.value);
        } else {
            return -static_cast<ResultType>(a);
        }
    }
};

template <typename A>
struct BitNotImpl
{
    using ResultType = typename NumberTraits::ResultOfBitNot<A>::Type;

    static inline ResultType apply(A a [[maybe_unused]])
    {
        if constexpr (IsDecimal<A>)
            throw Exception("unimplement");
        else
            return ~static_cast<ResultType>(a);
    }
};

template <typename A>
struct AbsImpl
{
    using ResultType = typename NumberTraits::ResultOfAbs<A>::Type;

    static inline ResultType apply(A a)
    {
        if constexpr (std::is_integral_v<A> && std::is_signed_v<A>)
        {
            // keep the same behavior as mysql and tidb, even though error no is not the same.
            if unlikely(a == INT64_MIN)
            {
                throw Exception("BIGINT value is out of range in 'abs(-9223372036854775808)'");
            }
            return a < 0 ? static_cast<ResultType>(~a) + 1 : a;
        }
        else if constexpr (std::is_integral_v<A> && std::is_unsigned_v<A>)
            return static_cast<ResultType>(a);
        else if constexpr (std::is_floating_point_v<A>)
            return static_cast<ResultType>(std::abs(a));
        else if constexpr (IsDecimal<A>)
            return a.value < 0 ? -a.value : a.value;
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct GCDImpl;
template <typename A, typename B>
struct GCDImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<B>::Type(b), typename NumberTraits::ToInteger<A>::Type(a));
        return boost::math::gcd(
            typename NumberTraits::ToInteger<Result>::Type(a),
            typename NumberTraits::ToInteger<Result>::Type(b));
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct GCDImpl<A,B,true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return GCDImpl<Result, Result>::apply(static_cast<Result>(a), static_cast<Result>(b));
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B> > struct LCMImpl;
template <typename A, typename B>
struct LCMImpl<A,B,false>
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<B>::Type(b), typename NumberTraits::ToInteger<A>::Type(a));
        return boost::math::lcm(
            typename NumberTraits::ToInteger<Result>::Type(a),
            typename NumberTraits::ToInteger<Result>::Type(b));
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct LCMImpl<A,B,true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return LCMImpl<Result, Result>::apply(static_cast<Result>(a), static_cast<Result>(b));
    }
    template <typename Result = ResultType>
    static inline Result apply(A , B , UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A>
struct IntExp2Impl
{
    using ResultType = UInt64;

    static inline ResultType apply(A a)
    {
        return intExp2(a);
    }
};

template <typename A>
struct IntExp2Impl<Decimal<A>>
{
    using ResultType = UInt64;

    static inline ResultType apply(Decimal<A>)
    {
        return 0;
    }
};

template <typename A>
struct IntExp10Impl
{
    using ResultType = UInt64;

    static inline ResultType apply(A a)
    {
        return intExp10(a);
    }
};

template <typename A>
struct IntExp10Impl<Decimal<A>>
{
    using ResultType = UInt64;

    static inline ResultType apply(Decimal<A> a)
    {
        return intExp10(a);
    }
};

/// these ones for better semantics
template <typename T> using Then = T;
template <typename T> using Else = T;

/// Used to indicate undefined operation
struct InvalidType;

template <typename T>
struct DataTypeFromFieldType
{
    using Type = DataTypeNumber<T>;
};

template <>
struct DataTypeFromFieldType<NumberTraits::Error>
{
    using Type = InvalidType;
};

template<typename T>
struct DataTypeFromFieldType<Decimal<T>>
{
    using Type = DataTypeDecimal<T>;
};

/// Binary operations for Decimals need scale args
/// +|- scale one of args (which scale factor is not 1). ScaleR = oneof(Scale1, Scale2);
/// *   no agrs scale. ScaleR = Scale1 + Scale2;
/// /   first arg scale. ScaleR = Scale1 (scale_a = DecimalType<B>::getScale()).
template <typename A, typename B, template <typename, typename> typename Operation, typename ResultType_>
struct DecimalBinaryOperation
{
    //static_assert((IsDecimal<A> || IsDecimal<B>) && IsDecimal<ResultType_>);
    //static_assert(IsDecimal<A> || std::is_integral_v<A>);
    //static_assert(IsDecimal<B> || std::is_integral_v<B>);

    static constexpr bool is_plus_minus =   std::is_same_v<Operation<Int32, Int32>, PlusImpl<Int32, Int32>> ||
                                            std::is_same_v<Operation<Int32, Int32>, MinusImpl<Int32, Int32>>;
    static constexpr bool is_multiply =     std::is_same_v<Operation<Int32, Int32>, MultiplyImpl<Int32, Int32>>;
    static constexpr bool is_mod =     std::is_same_v<Operation<Int32, Int32>, ModuloImpl<Int32, Int32>>;
    static constexpr bool is_float_division = std::is_same_v<Operation<Int32, Int32>, DivideFloatingImpl<Int32, Int32>> ||
                                              std::is_same_v<Operation<Int32, Int32>, TiDBDivideFloatingImpl<Int32, Int32>>;
    static constexpr bool is_int_division = std::is_same_v<Operation<Int32, Int32>, DivideIntegralImpl<Int32, Int32>> ||
                                            std::is_same_v<Operation<Int32, Int32>, DivideIntegralOrZeroImpl<Int32, Int32>>;
    static constexpr bool is_division = is_float_division || is_int_division;
    static constexpr bool is_compare =      std::is_same_v<Operation<Int32, Int32>, LeastBaseImpl<Int32, Int32>> ||
                                            std::is_same_v<Operation<Int32, Int32>, GreatestBaseImpl<Int32, Int32>>;
    static constexpr bool is_plus_minus_compare = is_plus_minus || is_compare;
    static constexpr bool can_overflow = is_plus_minus || is_multiply;

    static constexpr bool need_promote_type = (std::is_same_v<ResultType_, A> || std::is_same_v<ResultType_, B>) && (is_plus_minus_compare || is_division || is_multiply) ; // And is multiple / division
    static constexpr bool check_overflow = need_promote_type && std::is_same_v<ResultType_, Decimal256>; // Check if exceeds 10 * 66;

    using ResultType = ResultType_;
    using NativeResultType = typename ResultType::NativeType;
    using ColVecA = std::conditional_t<IsDecimal<A>, ColumnDecimal<A>, ColumnVector<A>>;
    using ColVecB = std::conditional_t<IsDecimal<B>, ColumnDecimal<B>, ColumnVector<B>>;
    using ArrayA = typename ColVecA::Container;
    using ArrayB = typename ColVecB::Container;
    using ArrayC = typename ColumnDecimal<ResultType>::Container;
    using PromoteResultType = typename PromoteType<NativeResultType>::Type;
    using InputType = std::conditional_t<need_promote_type, PromoteResultType, NativeResultType>;
    using Op = Operation<InputType, InputType>;

    static void NO_INLINE vector_vector(const ArrayA & a, const ArrayB & b, ArrayC & c,
                                        NativeResultType scale_a [[maybe_unused]], NativeResultType scale_b [[maybe_unused]], NativeResultType scale_result [[maybe_unused]])
    {
        size_t size = a.size();
        if constexpr (is_plus_minus_compare || is_mod)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(a[i], b[i], scale_a);
                return;
            }
            else if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(a[i], b[i], scale_b);
                return;
            }
        }
        else if constexpr (is_multiply)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaledMul(a[i], b[i], scale_result);
            return;
        }
        else if constexpr (is_division)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaled<true>(a[i], b[i], scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a[i], b[i]);
    }

    static void NO_INLINE vector_vector_nullable(const ArrayA & a, const ColumnUInt8 * a_nullmap, const ArrayB & b, const ColumnUInt8 * b_nullmap,
        ArrayC & c, typename ColumnUInt8::Container & res_null, NativeResultType scale_a [[maybe_unused]], NativeResultType scale_b [[maybe_unused]], NativeResultType scale_result [[maybe_unused]])
    {
        size_t size = a.size();
        if constexpr (is_division)
        {
            if (a_nullmap != nullptr && b_nullmap != nullptr)
            {
                auto & a_nullmap_data = a_nullmap->getData();
                auto & b_nullmap_data = b_nullmap->getData();
                for (size_t i = 0; i < size; ++i)
                {
                    res_null[i] = a_nullmap_data[i] || b_nullmap_data[i];
                }
            }
            else if (a_nullmap != nullptr || b_nullmap != nullptr)
            {
                auto & nullmap_data = a_nullmap != nullptr ? a_nullmap->getData() : b_nullmap->getData();
                for (size_t i = 0; i < size; ++i)
                {
                    res_null[i] = nullmap_data[i];
                }
            }

            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaled<true>(a[i], b[i], scale_a, res_null[i]);
            return;
        }
        throw Exception("Should not reach here");
    }

    static void NO_INLINE vector_constant(const ArrayA & a, B b, ArrayC & c,
                                        NativeResultType scale_a [[maybe_unused]], NativeResultType scale_b [[maybe_unused]], NativeResultType scale_result [[maybe_unused]])
    {
        size_t size = a.size();
        if constexpr (is_plus_minus_compare || is_mod)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(a[i], b, scale_a);
                return;
            }
            else if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(a[i], b, scale_b);
                return;
            }
        }
        else if constexpr (is_multiply)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaledMul(a[i], b, scale_result);
            return;
        }
        else if constexpr (is_division)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaled<true>(a[i], b, scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a[i], b);
    }

    static void NO_INLINE vector_constant_nullable(const ArrayA & a, const ColumnUInt8 * a_nullmap, B b, ArrayC & c, typename ColumnUInt8::Container & res_null,
                                          NativeResultType scale_a [[maybe_unused]], NativeResultType scale_b [[maybe_unused]], NativeResultType scale_result [[maybe_unused]])
    {
        size_t size = a.size();
        if constexpr (is_division)
        {
            if (a_nullmap != nullptr)
            {
                auto & nullmap_data = a_nullmap->getData();
                for (size_t i = 0; i < size; ++i)
                {
                    res_null[i] = nullmap_data[i];
                }
            }
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaled<true>(a[i], b, scale_a, res_null[i]);
            return;
        }
        throw Exception("Should not reach here");
    }

    static void NO_INLINE constant_vector(A a, const ArrayB & b, ArrayC & c,
                                        NativeResultType scale_a [[maybe_unused]], NativeResultType scale_b [[maybe_unused]], NativeResultType scale_result [[maybe_unused]])
    {
        size_t size = b.size();
        if constexpr (is_plus_minus_compare || is_mod)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(a, b[i], scale_a);
                return;
            }
            else if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(a, b[i], scale_b);
                return;
            }
        }
        else if constexpr (is_multiply) {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaledMul(a, b[i], scale_result);
            return;
        }
        else if constexpr (is_division)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaled<true>(a, b[i], scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a, b[i]);
    }

    static void NO_INLINE constant_vector_nullable(A a, const ArrayB & b, const ColumnUInt8 * b_nullmap, ArrayC & c, typename ColumnUInt8::Container & res_null,
                                          NativeResultType scale_a [[maybe_unused]], NativeResultType scale_b [[maybe_unused]], NativeResultType scale_result [[maybe_unused]])
    {
        size_t size = b.size();
        if constexpr (is_division)
        {
            if (b_nullmap != nullptr)
            {
                auto & nullmap_data = b_nullmap->getData();
                for (size_t i = 0; i < size; ++i)
                {
                    res_null[i] = nullmap_data[i];
                }
            }

            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaled<true>(a, b[i], scale_a, res_null[i]);
            return;
        }
        throw Exception("Should not reach here");
    }

    static ResultType constant_constant(A a, B b, NativeResultType scale_a [[maybe_unused]], NativeResultType scale_b [[maybe_unused]], NativeResultType scale_result [[maybe_unused]])
    {
        if constexpr (is_plus_minus_compare || is_mod)
        {
            if (scale_a != 1)
                return applyScaled<true>(a, b, scale_a);
            else if (scale_b != 1)
                return applyScaled<false>(a, b, scale_b);
        }
        else if constexpr (is_multiply) {
            return applyScaledMul(a, b, scale_result);
        }
        else if constexpr (is_division)
            return applyScaled<true>(a, b, scale_a);
        return apply(a, b);
    }

    static ResultType constant_constant_nullable(A a, B b, NativeResultType scale_a [[maybe_unused]], NativeResultType scale_b [[maybe_unused]],
        NativeResultType scale_result [[maybe_unused]], UInt8 & res_null)
    {
        if constexpr (is_division)
        {
            return applyScaled<true>(a, b, scale_a, res_null);
        }
        else
        {
            throw Exception("Should not reach here");
        }
    }

private:
    static NativeResultType applyScaledMul(NativeResultType a, NativeResultType b, NativeResultType scale) {
        if constexpr (is_multiply) {
            if constexpr (need_promote_type) {
                PromoteResultType res = Op::template apply<PromoteResultType>(a, b);
                res = res / scale;
                if constexpr (check_overflow){
                    if (res > DecimalMaxValue::MaxValue()) {
                        throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
                    }
                }
                return static_cast<NativeResultType>(res);
            } else {
                NativeResultType res = Op::template apply<NativeResultType>(a, b);
                res = res / scale;
                return res;
            }
        }
    }

    /// there's implicit type convertion here
    static NativeResultType apply(NativeResultType a, NativeResultType b)
    {
        if constexpr (need_promote_type)
        {
            auto res = Op::template apply<PromoteResultType>(a, b);
            if constexpr (check_overflow){
                if (res > DecimalMaxValue::MaxValue()) {
                    throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
                }
            }
            return static_cast<NativeResultType>(res);
        }
        else
        {
            return Op::template apply<NativeResultType>(a, b);
        }
    }

    template <bool scale_left>
    static NativeResultType applyScaled(InputType a, InputType b, InputType scale)
    {
        if constexpr (is_plus_minus_compare || is_division || is_mod)
        {
            InputType res;

            if constexpr (scale_left)
                a = a * scale;
            else
                b = b * scale;

            res = Op::template apply<InputType>(a, b);

            if constexpr (check_overflow) {
                if (res > DecimalMaxValue::MaxValue()) {
                    throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
                }
            }

            return static_cast<NativeResultType>(res);
        }
    }

    template <bool scale_left>
    static NativeResultType applyScaled(InputType a, InputType b, InputType scale, UInt8 & res_null)
    {
        if constexpr (is_division)
        {
            InputType res;

            if constexpr (scale_left)
                a = a * scale;
            else
                b = b * scale;

            res = Op::template apply<InputType>(a, b, res_null);

            if constexpr (check_overflow) {
                if (res > DecimalMaxValue::MaxValue()) {
                    throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
                }
            }

            return static_cast<NativeResultType>(res);
        }
        throw Exception("Should not reach here");
    }
};

template <typename DataType> constexpr bool IsIntegral = false;
template <> inline constexpr bool IsIntegral<DataTypeUInt8> = true;
template <> inline constexpr bool IsIntegral<DataTypeUInt16> = true;
template <> inline constexpr bool IsIntegral<DataTypeUInt32> = true;
template <> inline constexpr bool IsIntegral<DataTypeUInt64> = true;
template <> inline constexpr bool IsIntegral<DataTypeInt8> = true;
template <> inline constexpr bool IsIntegral<DataTypeInt16> = true;
template <> inline constexpr bool IsIntegral<DataTypeInt32> = true;
template <> inline constexpr bool IsIntegral<DataTypeInt64> = true;

template <typename DataType> constexpr bool IsDateOrDateTime = false;
template <> inline constexpr bool IsDateOrDateTime<DataTypeDate> = true;
template <> inline constexpr bool IsDateOrDateTime<DataTypeDateTime> = true;

/** Returns appropriate result type for binary operator on dates (or datetimes):
 *  Date + Integral -> Date
 *  Integral + Date -> Date
 *  Date - Date     -> Int32
 *  Date - Integral -> Date
 *  least(Date, Date) -> Date
 *  greatest(Date, Date) -> Date
 *  All other operations are not defined and return InvalidType, operations on
 *  distinct date types are also undefined (e.g. DataTypeDate - DataTypeDateTime)
 */
template <template <typename, typename> class Operation, typename LeftDataType, typename RightDataType>
struct DateBinaryOperationTraits
{
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using Op = Operation<T0, T1>;

    using ResultDataType =
        If<std::is_same_v<Op, PlusImpl<T0, T1>>,
            Then<
                If<IsDateOrDateTime<LeftDataType> && IsIntegral<RightDataType>,
                    Then<LeftDataType>,
                    Else<
                        If<IsIntegral<LeftDataType> && IsDateOrDateTime<RightDataType>,
                            Then<RightDataType>,
                            Else<InvalidType>>>>>,
            Else<
                If<std::is_same_v<Op, MinusImpl<T0, T1>>,
                    Then<
                        If<IsDateOrDateTime<LeftDataType>,
                            Then<
                                If<std::is_same_v<LeftDataType, RightDataType>,
                                    Then<DataTypeInt32>,
                                    Else<
                                        If<IsIntegral<RightDataType>,
                                            Then<LeftDataType>,
                                            Else<InvalidType>>>>>,
                            Else<InvalidType>>>,
                    Else<
                        If<std::is_same_v<T0, T1>
                            && (std::is_same_v<Op, LeastImpl<T0, T1>> || std::is_same_v<Op, GreatestImpl<T0, T1>>),
                            Then<LeftDataType>,
                            Else<InvalidType>>>>>>;
};


/// Decides among date and numeric operations
template <template <typename, typename> class Operation, typename LeftDataType, typename RightDataType>
struct BinaryOperationTraits
{
    using ResultDataType =
        If<IsDateOrDateTime<LeftDataType> || IsDateOrDateTime<RightDataType>,
            Then<
                typename DateBinaryOperationTraits<
                    Operation, LeftDataType, RightDataType>::ResultDataType>,
            Else<
                typename DataTypeFromFieldType<
                    typename Operation<
                        typename LeftDataType::FieldType,
                        typename RightDataType::FieldType>::ResultType>::Type>>;
};


template <template <typename, typename> class Op, typename Name, bool default_impl_for_nulls = true>
class FunctionBinaryArithmetic : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionBinaryArithmetic>(context); }

    FunctionBinaryArithmetic(const Context & context) : context(context) {}

    bool useDefaultImplementationForNulls() const override { return default_impl_for_nulls; }

private:
    const Context & context;

    template <typename ResultDataType>
    bool checkRightTypeImpl(DataTypePtr & type_res) const
    {
        /// Overload for InvalidType
        if constexpr (std::is_same_v<ResultDataType, InvalidType>)
            return false;
        else
        {
            type_res = std::make_shared<ResultDataType>();
            return true;
        }
    }

    std::pair<PrecType, ScaleType> getPrecAndScale(const IDataType* input_type) const {
        const IDataType * type = input_type;
        if constexpr (!default_impl_for_nulls)
        {
            if (auto ptr = typeid_cast<const DataTypeNullable *>(input_type))
            {
                type = ptr->getNestedType().get();
            }
        }
        if (auto ptr = typeid_cast<const DataTypeDecimal32 *>(type)) {
            return std::make_pair(ptr->getPrec(), ptr->getScale());
        }
        if (auto ptr = typeid_cast<const DataTypeDecimal64 *>(type)) {
            return std::make_pair(ptr->getPrec(), ptr->getScale());
        }
        if (auto ptr = typeid_cast<const DataTypeDecimal128 *>(type)) {
            return std::make_pair(ptr->getPrec(), ptr->getScale());
        }
        auto ptr = typeid_cast<const DataTypeDecimal256 *>(type);
        return std::make_pair(ptr->getPrec(), ptr->getScale());
    }

    template <typename LeftDataType, typename RightDataType>
    DataTypePtr getDecimalReturnType(const DataTypes & arguments) const {
        using LeftFieldType = typename LeftDataType::FieldType;
        using RightFieldType = typename RightDataType::FieldType;
        if constexpr (!IsDecimal<typename Op<LeftFieldType, RightFieldType>::ResultType>)
        {
            return std::make_shared<typename DataTypeFromFieldType<typename Op<LeftFieldType, RightFieldType>::ResultType>::Type>();
        }
        else
        {
            PrecType result_prec = 0;
            ScaleType result_scale = 0;
            // Treat integer as a kind of decimal;
            if constexpr (std::is_integral_v<LeftFieldType>) {
                PrecType leftPrec = IntPrec<LeftFieldType>::prec;
                auto [rightPrec, rightScale] = getPrecAndScale(arguments[1].get());
                Op<LeftFieldType, RightFieldType>::ResultPrecInferer::infer(leftPrec, 0, rightPrec, rightScale, result_prec, result_scale);
                return createDecimal(result_prec, result_scale);
            } else if constexpr (std::is_integral_v<RightFieldType>) {
                ScaleType rightPrec = IntPrec<RightFieldType>::prec;
                auto [leftPrec, leftScale] = getPrecAndScale(arguments[0].get());
                Op<LeftFieldType, RightFieldType>::ResultPrecInferer::infer(leftPrec, leftScale, rightPrec, 0, result_prec, result_scale);
                return createDecimal(result_prec, result_scale);
            }
            auto [leftPrec, leftScale] = getPrecAndScale(arguments[0].get());
            auto [rightPrec, rightScale] = getPrecAndScale(arguments[1].get());
            Op<LeftFieldType, RightFieldType>::ResultPrecInferer::infer(leftPrec, leftScale, rightPrec, rightScale, result_prec, result_scale);

            return createDecimal(result_prec, result_scale);
        }
    }

    template <typename LeftDataType, typename RightDataType>
    bool checkRightType(const DataTypes & arguments, DataTypePtr & type_res) const
    {
        auto right_type = arguments[1];
        if constexpr (!default_impl_for_nulls)
        {
            right_type = removeNullable(right_type);
        }
        if constexpr (IsDecimal<typename LeftDataType::FieldType> || IsDecimal<typename RightDataType::FieldType>) {
            if (typeid_cast<const RightDataType *>(right_type.get())){
                type_res = getDecimalReturnType<LeftDataType, RightDataType>(arguments);
                return true;
            }
            return false;
        } else {
            using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;

            if (typeid_cast<const RightDataType *>(right_type.get()))
                return checkRightTypeImpl<ResultDataType>(type_res);

            return false;
        }
    }

    template <typename T0>
    bool checkLeftType(const DataTypes & arguments, DataTypePtr & type_res) const
    {
        auto left_type = arguments[0];
        if constexpr (!default_impl_for_nulls)
        {
            left_type = removeNullable(left_type);
        }
        if (typeid_cast<const T0 *>(left_type.get()))
        {
            if (   checkRightType<T0, DataTypeDate>(arguments, type_res)
                || checkRightType<T0, DataTypeDateTime>(arguments, type_res)
                || checkRightType<T0, DataTypeUInt8>(arguments, type_res)
                || checkRightType<T0, DataTypeUInt16>(arguments, type_res)
                || checkRightType<T0, DataTypeUInt32>(arguments, type_res)
                || checkRightType<T0, DataTypeUInt64>(arguments, type_res)
                || checkRightType<T0, DataTypeInt8>(arguments, type_res)
                || checkRightType<T0, DataTypeInt16>(arguments, type_res)
                || checkRightType<T0, DataTypeInt32>(arguments, type_res)
                || checkRightType<T0, DataTypeInt64>(arguments, type_res)
                || checkRightType<T0, DataTypeFloat32>(arguments, type_res)
                || checkRightType<T0, DataTypeDecimal32>(arguments, type_res)
                || checkRightType<T0, DataTypeDecimal64>(arguments, type_res)
                || checkRightType<T0, DataTypeDecimal128>(arguments, type_res)
                || checkRightType<T0, DataTypeDecimal256>(arguments, type_res)
                || checkRightType<T0, DataTypeFloat64>(arguments, type_res))
                return true;
        }
        return false;
    }

    FunctionBuilderPtr getFunctionForIntervalArithmetic(const DataTypePtr & type0, const DataTypePtr & type1) const
    {
        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Interval.
        /// We construct another function (example: addMonths) and call it.

        bool function_is_plus = std::is_same_v<Op<UInt8, UInt8>, PlusImpl<UInt8, UInt8>>;
        bool function_is_minus = std::is_same_v<Op<UInt8, UInt8>, MinusImpl<UInt8, UInt8>>;

        if (!function_is_plus && !function_is_minus)
            return {};

        int interval_arg = 1;
        /// do not check null type because only divide op may use non-default-impl for nulls
        const DataTypeInterval * interval_data_type = checkAndGetDataType<DataTypeInterval>(type1.get());
        if (!interval_data_type)
        {
            interval_arg = 0;
            interval_data_type = checkAndGetDataType<DataTypeInterval>(type0.get());
        }
        if (!interval_data_type)
            return {};

        if (interval_arg == 0 && function_is_minus)
            throw Exception("Wrong order of arguments for function " + getName() + ": argument of type Interval cannot be first.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypeDate * date_data_type = checkAndGetDataType<DataTypeDate>(interval_arg == 0 ? type1.get() : type0.get());
        const DataTypeDateTime * date_time_data_type = nullptr;
        if (!date_data_type)
        {
            date_time_data_type = checkAndGetDataType<DataTypeDateTime>(interval_arg == 0 ? type1.get() : type0.get());
            if (!date_time_data_type)
                throw Exception("Wrong argument types for function " + getName() + ": if one argument is Interval, then another must be Date or DateTime.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        std::stringstream function_name;
        function_name << (function_is_plus ? "add" : "subtract") << interval_data_type->kindToString() << 's';

        return FunctionFactory::instance().get(function_name.str(), context);
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Interval.
        if (auto function_builder = getFunctionForIntervalArithmetic(arguments[0], arguments[1]))
        {
            ColumnsWithTypeAndName new_arguments(2);

            for (size_t i = 0; i < 2; ++i)
                new_arguments[i].type = arguments[i];

            /// Interval argument must be second.
            if (checkDataType<DataTypeInterval>(new_arguments[0].type.get()))
                std::swap(new_arguments[0], new_arguments[1]);

            /// Change interval argument to its representation
            new_arguments[1].type = std::make_shared<typename DataTypeFromFieldType<DataTypeInterval::FieldType>::Type>();

            auto function = function_builder->build(new_arguments);
            return function->getReturnType();
        }

        DataTypePtr type_res;
        if constexpr (!default_impl_for_nulls)
        {
            /// if one of the input is null constant, return null constant
            auto * left_null_type = typeid_cast<const DataTypeNullable *>(arguments[0].get());
            bool left_null_const = left_null_type != nullptr && left_null_type->onlyNull();
            if (left_null_const)
                type_res = arguments[0];
            auto * right_null_type = typeid_cast<const DataTypeNullable *>(arguments[1].get());
            bool right_null_const = right_null_type != nullptr && right_null_type->onlyNull();
            if (right_null_const)
                type_res = arguments[1];
            if (left_null_const || right_null_const)
                return type_res;
        }

        if (!( checkLeftType<DataTypeDate>(arguments, type_res)
            || checkLeftType<DataTypeDateTime>(arguments, type_res)
            || checkLeftType<DataTypeUInt8>(arguments, type_res)
            || checkLeftType<DataTypeUInt16>(arguments, type_res)
            || checkLeftType<DataTypeUInt32>(arguments, type_res)
            || checkLeftType<DataTypeUInt64>(arguments, type_res)
            || checkLeftType<DataTypeInt8>(arguments, type_res)
            || checkLeftType<DataTypeInt16>(arguments, type_res)
            || checkLeftType<DataTypeInt32>(arguments, type_res)
            || checkLeftType<DataTypeInt64>(arguments, type_res)
            || checkLeftType<DataTypeDecimal<Decimal32>>(arguments, type_res)
            || checkLeftType<DataTypeDecimal<Decimal64>>(arguments, type_res)
            || checkLeftType<DataTypeDecimal<Decimal128>>(arguments, type_res)
            || checkLeftType<DataTypeDecimal<Decimal256>>(arguments, type_res)
            || checkLeftType<DataTypeFloat32>(arguments, type_res)
            || checkLeftType<DataTypeFloat64>(arguments, type_res)))
            throw Exception("Illegal types " + arguments[0]->getName() + " and " + arguments[1]->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if constexpr (!default_impl_for_nulls)
            type_res = makeNullable(type_res);
        return type_res;
    }

    template <typename F>
    bool castType(const IDataType * type, F && f)
    {
        return castTypeToEither<
            DataTypeUInt8,
            DataTypeUInt16,
            DataTypeUInt32,
            DataTypeUInt64,
            DataTypeInt8,
            DataTypeInt16,
            DataTypeInt32,
            DataTypeInt64,
            DataTypeFloat32,
            DataTypeFloat64,
            DataTypeDate,
            DataTypeDateTime,
            DataTypeDecimal32,
            DataTypeDecimal64,
            DataTypeDecimal128,
            DataTypeDecimal256
        >(type, std::forward<F>(f));
    }

    template <typename F>
    bool castBothTypes(DataTypePtr left, DataTypePtr right, DataTypePtr result, F && f)
    {
        return castType(left.get(), [&](const auto & left_, bool is_left_nullable_) {
            return castType(right.get(), [&](const auto & right_, bool is_right_nullable_) {
                return castType(result.get(), [&](const auto & result_, bool){
                    return f(left_, is_left_nullable_, right_, is_right_nullable_, result_);
                });
            });
        });
    }

    template <typename A, typename B, bool check = IsDecimal<A>> struct RefineCls;

    template<typename T, typename ResultType>
    struct RefineCls <T, ResultType, true> {
        using Type = If<std::is_floating_point_v<ResultType>, ResultType, typename T::NativeType>;
    };

    template<typename T, typename ResultType>
    struct RefineCls <T, ResultType, false> {
        using Type = T;
    };

    template<typename A, typename B>
    using Refine = typename RefineCls<A,B>::Type;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Interval.
        if (auto function_builder = getFunctionForIntervalArithmetic(block.getByPosition(arguments[0]).type, block.getByPosition(arguments[1]).type))
        {
            ColumnNumbers new_arguments = arguments;

            /// Interval argument must be second.
            if (checkDataType<DataTypeInterval>(block.getByPosition(arguments[0]).type.get()))
                std::swap(new_arguments[0], new_arguments[1]);

            /// Change interval argument type to its representation
            Block new_block = block;
            new_block.getByPosition(new_arguments[1]).type = std::make_shared<typename DataTypeFromFieldType<DataTypeInterval::FieldType>::Type>();

            ColumnsWithTypeAndName new_arguments_with_type_and_name =
                    {new_block.getByPosition(new_arguments[0]), new_block.getByPosition(new_arguments[1])};
            auto function = function_builder->build(new_arguments_with_type_and_name);

            function->execute(new_block, new_arguments, result);
            block.getByPosition(result).column = new_block.getByPosition(result).column;

            return;
        }

        auto left_generic = block.getByPosition(arguments[0]).type;
        auto right_generic = block.getByPosition(arguments[1]).type;
        DataTypes types;
        types.push_back(left_generic);
        types.push_back(right_generic);
        DataTypePtr result_type = getReturnTypeImpl(types);
        if constexpr (!default_impl_for_nulls)
        {
            if (result_type->onlyNull())
            {
                block.getByPosition(result).column = result_type->createColumnConst(block.rows(), Null());
                return;
            }
        }
        bool valid = castBothTypes(left_generic, right_generic, result_type, [&](const auto & left, bool is_left_nullable [[maybe_unused]], const auto & right, bool is_right_nullable [[maybe_unused]], const auto & result_type)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            using ResultDataType = std::decay_t<decltype(result_type)>;
            constexpr bool result_is_decimal = IsDecimal<typename ResultDataType::FieldType>;
            constexpr bool is_multiply [[maybe_unused]] = std::is_same_v<Op<UInt8, UInt8>, MultiplyImpl<UInt8, UInt8>>;
            constexpr bool is_division [[maybe_unused]] = std::is_same_v<Op<UInt8, UInt8>, DivideFloatingImpl<UInt8, UInt8>> ||
                                            std::is_same_v<Op<UInt8, UInt8>, TiDBDivideFloatingImpl<UInt8, UInt8>> ||
                                            std::is_same_v<Op<UInt8, UInt8>, DivideIntegralImpl<UInt8, UInt8>> ||
                                            std::is_same_v<Op<UInt8, UInt8>, DivideIntegralOrZeroImpl<UInt8, UInt8>>;

            using T0 = typename LeftDataType::FieldType;
            using T1 = typename RightDataType::FieldType;
            using ResultType = typename ResultDataType::FieldType;
            using ExpectedResultType = typename Op<T0, T1>::ResultType;
            if constexpr ((!IsDecimal<ResultType> || !IsDecimal<ExpectedResultType>) && !std::is_same_v<ResultType, ExpectedResultType>)
            {
                return false;
            }
            else if constexpr (!std::is_same_v<ResultDataType, InvalidType>)
            {
                using ColVecT0 = std::conditional_t<IsDecimal<T0>, ColumnDecimal<T0>, ColumnVector<T0>>;
                using ColVecT1 = std::conditional_t<IsDecimal<T1>, ColumnDecimal<T1>, ColumnVector<T1>>;
                using ColVecResult = std::conditional_t<IsDecimal<ResultType>, ColumnDecimal<ResultType>, ColumnVector<typename Op<T0, T1>::ResultType>>;

                /// Only for arithmatic operator
                using T0_ = Refine<T0, ResultType>;
                using T1_ = Refine<T1, ResultType>;
                using FieldT0 = typename NearestFieldType<T0>::Type;
                using FieldT1 = typename NearestFieldType<T1>::Type;
                /// Decimal operations need scale. Operations are on result type.
                using OpImpl = std::conditional_t<result_is_decimal,
                    DecimalBinaryOperation<T0, T1, Op, ResultType>,
                    BinaryOperationImpl<T0, T1, Op<T0_, T1_>, typename Op<T0,T1>::ResultType> >; // Use template to resolve !!!!!

                auto col_left_raw = block.getByPosition(arguments[0]).column.get();
                auto col_right_raw = block.getByPosition(arguments[1]).column.get();
                const ColumnUInt8 * col_left_nullmap [[maybe_unused]] = nullptr ;
                const ColumnUInt8 * col_right_nullmap [[maybe_unused]] = nullptr;
                bool is_left_null_constant [[maybe_unused]] = false;
                bool is_right_null_constant [[maybe_unused]] = false;
                DataTypePtr nullable_result_type [[maybe_unused]] = nullptr;
                if constexpr (result_is_decimal)
                {
                    nullable_result_type = makeNullable(std::make_shared<ResultDataType>(result_type.getPrec(), result_type.getScale()));
                }
                else
                {
                    nullable_result_type = makeNullable(std::make_shared<ResultDataType>());
                }

                if constexpr (!default_impl_for_nulls)
                {
                    if (is_left_nullable)
                    {
                        if (auto * col_nullable = typeid_cast<const ColumnNullable *>(col_left_raw))
                        {
                            col_left_nullmap = &col_nullable->getNullMapColumn();
                            col_left_raw = &col_nullable->getNestedColumn();
                        }
                        else if (auto * col_const = typeid_cast<const ColumnConst *>(col_left_raw))
                        {
                            if (col_const->isNullAt(0))
                                is_left_null_constant = true;
                        }
                        else
                        {
                            return false;
                        }
                    }
                    if (is_right_nullable)
                    {
                        if (auto * col_nullable = typeid_cast<const ColumnNullable *>(col_right_raw))
                        {
                            col_right_nullmap = &col_nullable->getNullMapColumn();
                            col_right_raw = &col_nullable->getNestedColumn();
                        }
                        else if (auto * col_const = typeid_cast<const ColumnConst *>(col_right_raw))
                        {
                            if (col_const->isNullAt(0))
                                is_right_null_constant = true;
                        }
                        else
                        {
                            return false;
                        }
                    }
                    if (is_left_null_constant || is_right_null_constant)
                    {
                        /// if one of the input is null constant, just return null constant
                        block.getByPosition(result).column = nullable_result_type->createColumnConst(col_left_raw->size(), Null());
                        return true;
                    }
                }

                if (auto col_left = checkAndGetColumnConst<ColVecT0>(col_left_raw, is_left_nullable))
                {
                    if (auto col_right = checkAndGetColumnConst<ColVecT1>(col_right_raw, is_right_nullable))
                    {
                        /// the only case with a non-vector result
                        if constexpr (result_is_decimal)
                        {
                            auto [scale_a, scale_b, scale_result] = result_type.getScales(left, right, is_multiply, is_division);

                            if constexpr (default_impl_for_nulls)
                            {
                                auto res = OpImpl::constant_constant(col_left->template getValue<T0>(), col_right->template getValue<T1>(),
                                                                     scale_a, scale_b, scale_result);
                                block.getByPosition(result).column =
                                    ResultDataType(result_type.getPrec(), result_type.getScale()).createColumnConst(
                                        col_left->size(), toField(res, result_type.getScale()));
                            }
                            else
                            {
                                UInt8 res_null = false;
                                Field result_field = Null();
                                auto res = OpImpl::constant_constant_nullable(col_left->template getValue<T0>(),
                                    col_right->template getValue<T1>(), scale_a, scale_b, scale_result, res_null);
                                if (!res_null)
                                    result_field = toField(res, result_type.getScale());
                                block.getByPosition(result).column = nullable_result_type->createColumnConst(col_left->size(), result_field);
                            }
                        }
                        else
                        {
                            if constexpr (default_impl_for_nulls)
                            {
                                auto res = OpImpl::constant_constant(col_left->getField().template safeGet<FieldT0>(), col_right->getField().template safeGet<FieldT1>());
                                block.getByPosition(result).column = ResultDataType().createColumnConst(col_left->size(), toField(res));
                            }
                            else
                            {
                                UInt8 res_null = false;
                                Field result_field = Null();
                                auto res = OpImpl::constant_constant_nullable(col_left->getField().template safeGet<FieldT0>(),
                                    col_right->getField().template safeGet<FieldT1>(), res_null);
                                if (!res_null)
                                    result_field = toField(res);
                                block.getByPosition(result).column = nullable_result_type->createColumnConst(col_left->size(), result_field);
                            }
                        }
                        return true;
                    }
                }

                typename ColVecResult::MutablePtr col_res = nullptr;
                if constexpr (result_is_decimal)
                {
                    col_res = ColVecResult::create(0, result_type.getScale());
                }
                else
                    col_res = ColVecResult::create();

                auto & vec_res = col_res->getData();
                vec_res.resize(block.rows());

                typename ColumnUInt8::MutablePtr res_nullmap = ColumnUInt8::create();
                typename ColumnUInt8::Container & vec_res_nulmap = res_nullmap->getData();
                if constexpr (!default_impl_for_nulls)
                {
                    vec_res_nulmap.assign(block.rows(), (UInt8)0);
                }

                if (auto col_left_const = checkAndGetColumnConst<ColVecT0>(col_left_raw, is_left_nullable))
                {
                    if (auto col_right = checkAndGetColumn<ColVecT1>(col_right_raw))
                    {
                        if constexpr (result_is_decimal)
                        {
                            auto [scale_a, scale_b, scale_result] = result_type.getScales(left, right, is_multiply, is_division);
                            if constexpr (default_impl_for_nulls)
                            {
                                OpImpl::constant_vector(
                                    col_left_const->template getValue<T0>(), col_right->getData(), vec_res, scale_a, scale_b, scale_result);
                            }
                            else
                            {
                                OpImpl::constant_vector_nullable(col_left_const->template getValue<T0>(), col_right->getData(),
                                    col_right_nullmap, vec_res, vec_res_nulmap, scale_a, scale_b, scale_result);
                            }
                        }
                        else
                        {
                            if constexpr (default_impl_for_nulls)
                            {
                                OpImpl::constant_vector(col_left_const->getField().template safeGet<FieldT0>(), col_right->getData(), vec_res);
                            }
                            else
                            {
                                OpImpl::constant_vector_nullable(col_left_const->getField().template safeGet<FieldT0>(), col_right->getData(),
                                    col_right_nullmap, vec_res, vec_res_nulmap);
                            }
                        }
                    }
                    else
                        return false;
                }
                else if (auto col_left = checkAndGetColumn<ColVecT0>(col_left_raw))
                {
                    if (auto col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_raw, is_right_nullable))
                    {
                        if constexpr (result_is_decimal)
                        {
                            auto [scale_a, scale_b, scale_result] = result_type.getScales(left, right, is_multiply, is_division);
                            if constexpr (default_impl_for_nulls)
                            {
                                OpImpl::vector_constant(
                                    col_left->getData(), col_right_const->template getValue<T1>(), vec_res, scale_a, scale_b, scale_result);
                            }
                            else
                            {
                                OpImpl::vector_constant_nullable(col_left->getData(), col_left_nullmap, col_right_const->template getValue<T1>(),
                                                                 vec_res, vec_res_nulmap, scale_a, scale_b, scale_result);
                            }
                        }
                        else
                        {
                            if constexpr (default_impl_for_nulls)
                            {
                                OpImpl::vector_constant(col_left->getData(), col_right_const->getField().template safeGet<FieldT1>(), vec_res);
                            }
                            else
                            {
                                OpImpl::vector_constant_nullable(col_left->getData(), col_left_nullmap, col_right_const->getField().template safeGet<FieldT1>(),
                                                                 vec_res, vec_res_nulmap);
                            }
                        }
                    }
                    else if (auto col_right = checkAndGetColumn<ColVecT1>(col_right_raw))
                    {
                        if constexpr (result_is_decimal)
                        {
                            auto [scale_a, scale_b, scale_result] = result_type.getScales(left, right, is_multiply, is_division);
                            if constexpr (default_impl_for_nulls)
                            {
                                OpImpl::vector_vector(col_left->getData(), col_right->getData(), vec_res, scale_a, scale_b,
                                                      scale_result);
                            }
                            else
                            {
                                OpImpl::vector_vector_nullable(col_left->getData(), col_left_nullmap, col_right->getData(), col_right_nullmap,
                                    vec_res, vec_res_nulmap, scale_a, scale_b, scale_result);
                            }
                        }
                        else
                        {
                            if constexpr (default_impl_for_nulls)
                            {
                                OpImpl::vector_vector(col_left->getData(), col_right->getData(), vec_res);
                            }
                            else
                            {
                                OpImpl::vector_vector_nullable(col_left->getData(), col_left_nullmap, col_right->getData(),
                                    col_right_nullmap, vec_res, vec_res_nulmap);
                            }
                        }
                    }
                    else
                        return false;
                }
                else
                    return false;

                if constexpr (default_impl_for_nulls)
                {
                    block.getByPosition(result).column = std::move(col_res);
                }
                else
                {
                    block.getByPosition(result).column = ColumnNullable::create(std::move(col_res), std::move(res_nullmap));
                }
                return true;
            }
            return false;
        });
        if (!valid)
            throw Exception(getName() + "'s arguments do not match the expected data types", ErrorCodes::LOGICAL_ERROR);
    }
};

template <typename FunctionName>
struct FunctionUnaryArithmeticMonotonicity;


template <template <typename> class Op, typename Name, bool is_injective>
class FunctionUnaryArithmetic : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionUnaryArithmetic>(); }

private:
    template <typename T0>
    bool checkType(const DataTypes & arguments, DataTypePtr & result) const
    {
        if (typeid_cast<const T0 *>(arguments[0].get()))
        {
            if constexpr (IsDecimal<typename T0::FieldType>) {
                auto t = static_cast<const T0 *> (arguments[0].get());
                result = std::make_shared<T0>(t->getPrec(), t->getScale());
            } else {
                result = std::make_shared<typename DataTypeFromFieldType<typename Op<typename T0::FieldType>::ResultType>::Type>();
            }
            return true;
        }
        return false;
    }

    template <typename T0>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (const ColumnVector<T0> * col = checkAndGetColumn<ColumnVector<T0>>(block.getByPosition(arguments[0]).column.get()))
        {
            using ResultType = typename Op<T0>::ResultType;

            auto col_res = ColumnVector<ResultType>::create();

            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col->getData().size());
            UnaryOperationImpl<T0, Op<T0>>::vector(col->getData(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }

        return false;
    }

    template <typename T0>
    bool executeDecimalType(Block & block, const ColumnNumbers & arguments, size_t result) {
        if (const ColumnDecimal<T0> * col = checkAndGetColumn<ColumnDecimal<T0>>(block.getByPosition(arguments[0]).column.get()))
        {
            using ResultType = typename Op<T0>::ResultType;

            if constexpr (IsDecimal<ResultType>) {
                auto col_res = ColumnDecimal<ResultType>::create(0, col->getData().getScale());

                typename ColumnDecimal<ResultType>::Container & vec_res = col_res->getData();
                vec_res.resize(col->getData().size());
                UnaryOperationImpl<T0, Op<T0>>::vector(col->getData(), vec_res);

                block.getByPosition(result).column = std::move(col_res);
                return true;
            } else {
                auto col_res = ColumnVector<ResultType>::create();

                typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
                vec_res.resize(col->getData().size());
                UnaryOperationImpl<T0, Op<T0>>::vector(col->getData(), vec_res);

                block.getByPosition(result).column = std::move(col_res);
                return true;
            }
        }

        return false;
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) override { return is_injective; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr result;

        if (!( checkType<DataTypeUInt8>(arguments, result)
            || checkType<DataTypeUInt16>(arguments, result)
            || checkType<DataTypeUInt32>(arguments, result)
            || checkType<DataTypeUInt64>(arguments, result)
            || checkType<DataTypeInt8>(arguments, result)
            || checkType<DataTypeInt16>(arguments, result)
            || checkType<DataTypeInt32>(arguments, result)
            || checkType<DataTypeInt64>(arguments, result)
            || checkType<DataTypeFloat32>(arguments, result)
            || checkType<DataTypeDecimal32>(arguments, result)
            || checkType<DataTypeDecimal64>(arguments, result)
            || checkType<DataTypeDecimal128>(arguments, result)
            || checkType<DataTypeDecimal256>(arguments, result)
            || checkType<DataTypeFloat64>(arguments, result)))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return result;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (!( executeType<UInt8>(block, arguments, result)
            || executeType<UInt16>(block, arguments, result)
            || executeType<UInt32>(block, arguments, result)
            || executeType<UInt64>(block, arguments, result)
            || executeType<Int8>(block, arguments, result)
            || executeType<Int16>(block, arguments, result)
            || executeType<Int32>(block, arguments, result)
            || executeType<Int64>(block, arguments, result)
            || executeDecimalType<Decimal32>(block, arguments, result)
            || executeDecimalType<Decimal64>(block, arguments, result)
            || executeDecimalType<Decimal128>(block, arguments, result)
            || executeDecimalType<Decimal256>(block, arguments, result)
            || executeType<Float32>(block, arguments, result)
            || executeType<Float64>(block, arguments, result)))
           throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return FunctionUnaryArithmeticMonotonicity<Name>::has();
    }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field & left, const Field & right) const override
    {
        return FunctionUnaryArithmeticMonotonicity<Name>::get(left, right);
    }
};


struct NamePlus                 { static constexpr auto name = "plus"; };
struct NameMinus                { static constexpr auto name = "minus"; };
struct NameMultiply             { static constexpr auto name = "multiply"; };
struct NameDivideFloating       { static constexpr auto name = "divide"; };
struct NameTiDBDivideFloating   { static constexpr auto name = "tidbDivide"; };
struct NameDivideIntegral       { static constexpr auto name = "intDiv"; };
struct NameDivideIntegralOrZero { static constexpr auto name = "intDivOrZero"; };
struct NameModulo               { static constexpr auto name = "modulo"; };
struct NameNegate               { static constexpr auto name = "negate"; };
struct NameAbs                  { static constexpr auto name = "abs"; };
struct NameBitAnd               { static constexpr auto name = "bitAnd"; };
struct NameBitOr                { static constexpr auto name = "bitOr"; };
struct NameBitXor               { static constexpr auto name = "bitXor"; };
struct NameBitNot               { static constexpr auto name = "bitNot"; };
struct NameBitShiftLeft         { static constexpr auto name = "bitShiftLeft"; };
struct NameBitShiftRight        { static constexpr auto name = "bitShiftRight"; };
struct NameBitRotateLeft        { static constexpr auto name = "bitRotateLeft"; };
struct NameBitRotateRight       { static constexpr auto name = "bitRotateRight"; };
struct NameBitTest              { static constexpr auto name = "bitTest"; };
struct NameBitTestAny           { static constexpr auto name = "bitTestAny"; };
struct NameBitTestAll           { static constexpr auto name = "bitTestAll"; };
struct NameLeast                { static constexpr auto name = "least"; };
struct NameGreatest             { static constexpr auto name = "greatest"; };
struct NameGCD                  { static constexpr auto name = "gcd"; };
struct NameLCM                  { static constexpr auto name = "lcm"; };
struct NameIntExp2              { static constexpr auto name = "intExp2"; };
struct NameIntExp10             { static constexpr auto name = "intExp10"; };

template<typename A, typename B> using PlusImpl_t = PlusImpl<A, B>;
using FunctionPlus = FunctionBinaryArithmetic<PlusImpl_t, NamePlus>;
template<typename A, typename B> using MinusImpl_t = MinusImpl<A, B>;
using FunctionMinus = FunctionBinaryArithmetic<MinusImpl_t, NameMinus>;
template<typename A, typename B> using MultiplyImpl_t = MultiplyImpl<A, B>;
using FunctionMultiply = FunctionBinaryArithmetic<MultiplyImpl_t, NameMultiply>;
template<typename A, typename B> using DivideFloatingImpl_t = DivideFloatingImpl<A, B>;
using FunctionDivideFloating = FunctionBinaryArithmetic<DivideFloatingImpl_t, NameDivideFloating>;
template<typename A, typename B> using TiDBDivideFloatingImpl_t = TiDBDivideFloatingImpl<A, B>;
using FunctionTiDBDivideFloating = FunctionBinaryArithmetic<TiDBDivideFloatingImpl_t, NameTiDBDivideFloating, false>;
template<typename A, typename B> using DivideIntegralImpl_t = DivideIntegralImpl<A, B>;
using FunctionDivideIntegral = FunctionBinaryArithmetic<DivideIntegralImpl_t, NameDivideIntegral>;
template<typename A, typename B> using DivideIntegralOrZeroImpl_t = DivideIntegralOrZeroImpl<A, B>;
using FunctionDivideIntegralOrZero = FunctionBinaryArithmetic<DivideIntegralOrZeroImpl_t, NameDivideIntegralOrZero>;
template<typename A, typename B> using ModuloImpl_t = ModuloImpl<A, B>;
using FunctionModulo = FunctionBinaryArithmetic<ModuloImpl_t, NameModulo>;
template<typename A, typename B> using BitAndImpl_t = BitAndImpl<A, B>;
using FunctionBitAnd = FunctionBinaryArithmetic<BitAndImpl_t, NameBitAnd>;
template<typename A, typename B> using BitOrImpl_t = BitOrImpl<A, B>;
using FunctionBitOr = FunctionBinaryArithmetic<BitOrImpl_t, NameBitOr>;
template<typename A, typename B> using BitXorImpl_t = BitXorImpl<A, B>;
using FunctionBitXor = FunctionBinaryArithmetic<BitXorImpl_t, NameBitXor>;
template<typename A, typename B> using BitShiftLeftImpl_t = BitShiftLeftImpl<A, B>;
using FunctionBitShiftLeft = FunctionBinaryArithmetic<BitShiftLeftImpl_t, NameBitShiftLeft>;
template<typename A, typename B> using BitShiftRightImpl_t = BitShiftRightImpl<A, B>;
using FunctionBitShiftRight = FunctionBinaryArithmetic<BitShiftRightImpl_t, NameBitShiftRight>;
template<typename A, typename B> using BitRotateLeftImpl_t = BitRotateLeftImpl<A, B>;
using FunctionBitRotateLeft = FunctionBinaryArithmetic<BitRotateLeftImpl_t, NameBitRotateLeft>;
template<typename A, typename B> using BitRotateRightImpl_t = BitRotateRightImpl<A, B>;
using FunctionBitRotateRight = FunctionBinaryArithmetic<BitRotateRightImpl_t, NameBitRotateRight>;
template<typename A, typename B> using BitTestImpl_t = BitTestImpl<A, B>;
using FunctionBitTest = FunctionBinaryArithmetic<BitTestImpl_t, NameBitTest>;
template<typename A, typename B> using LeastImpl_t = LeastImpl<A, B>;
using FunctionLeast = FunctionBinaryArithmetic<LeastImpl_t, NameLeast>;
template<typename A, typename B> using GreatestImpl_t = GreatestImpl<A, B>;
using FunctionGreatest = FunctionBinaryArithmetic<GreatestImpl_t, NameGreatest>;
template<typename A, typename B> using GCDImpl_t = GCDImpl<A, B>;
using FunctionGCD = FunctionBinaryArithmetic<GCDImpl_t, NameGCD>;
template<typename A, typename B> using LCMImpl_t = LCMImpl<A, B>;
using FunctionLCM = FunctionBinaryArithmetic<LCMImpl_t, NameLCM>;

using FunctionNegate = FunctionUnaryArithmetic<NegateImpl, NameNegate, true>;
using FunctionAbs = FunctionUnaryArithmetic<AbsImpl, NameAbs, false>;
using FunctionBitNot = FunctionUnaryArithmetic<BitNotImpl, NameBitNot, true>;

/// Assumed to be injective for the purpose of query optimization, but in fact it is not injective because of possible overflow.
using FunctionIntExp2 = FunctionUnaryArithmetic<IntExp2Impl, NameIntExp2, true>;
using FunctionIntExp10 = FunctionUnaryArithmetic<IntExp10Impl, NameIntExp10, true>;

/// Monotonicity properties for some functions.

template <> struct FunctionUnaryArithmeticMonotonicity<NameNegate>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return { true, false };
    }
};

template <> struct FunctionUnaryArithmeticMonotonicity<NameAbs>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if ((left_float < 0 && right_float > 0) || (left_float > 0 && right_float < 0))
            return {};

        return { true, (left_float > 0) };
    }
};

template <> struct FunctionUnaryArithmeticMonotonicity<NameBitNot>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return {};
    }
};

template <> struct FunctionUnaryArithmeticMonotonicity<NameIntExp2>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if (left_float < 0 || right_float > 63)
            return {};

        return { true };
    }
};

template <> struct FunctionUnaryArithmeticMonotonicity<NameIntExp10>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if (left_float < 0 || right_float > 19)
            return {};

        return { true };
    }
};

}

/// Optimizations for integer division by a constant.

#if __SSE2__
    #define LIBDIVIDE_USE_SSE2 1
#endif

#include <libdivide.h>

namespace DB
{

template <typename A, typename B>
struct DivideIntegralByConstantImpl
    : BinaryOperationImplBase<A, B, DivideIntegralImpl<A, B>>
{
    using ResultType = typename DivideIntegralImpl<A, B>::ResultType;

    static void vector_constant(const PaddedPODArray<A> & a, B b, PaddedPODArray<ResultType> & c)
    {
        if (unlikely(b == 0))
            throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

        if (unlikely(std::is_signed_v<B> && b == -1))
        {
            size_t size = a.size();
            for (size_t i = 0; i < size; ++i)
                c[i] = -c[i];
            return;
        }

#pragma GCC diagnostic pop

        libdivide::divider<A> divider(b);

        size_t size = a.size();
        const A * a_pos = &a[0];
        const A * a_end = a_pos + size;
        ResultType * c_pos = &c[0];

#if __SSE2__
        static constexpr size_t values_per_sse_register = 16 / sizeof(A);
        const A * a_end_sse = a_pos + size / values_per_sse_register * values_per_sse_register;

        while (a_pos < a_end_sse)
        {
            _mm_storeu_si128(reinterpret_cast<__m128i *>(c_pos),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(a_pos)) / divider);

            a_pos += values_per_sse_register;
            c_pos += values_per_sse_register;
        }
#endif

        while (a_pos < a_end)
        {
            *c_pos = *a_pos / divider;
            ++a_pos;
            ++c_pos;
        }
    }
};

template <typename A, typename B>
struct ModuloByConstantImpl
    : BinaryOperationImplBase<A, B, ModuloImpl<A, B>>
{
    using ResultType = typename ModuloImpl<A, B>::ResultType;

    static void vector_constant(const PaddedPODArray<A> & a, B b, PaddedPODArray<ResultType> & c)
    {
        if (unlikely(b == 0))
            throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

        if (unlikely((std::is_signed_v<B> && b == -1) || b == 1))
        {
            size_t size = a.size();
            for (size_t i = 0; i < size; ++i)
                c[i] = 0;
            return;
        }

#pragma GCC diagnostic pop

        libdivide::divider<A> divider(b);

        /// Here we failed to make the SSE variant from libdivide give an advantage.
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = a[i] - (a[i] / divider) * b; /// NOTE: perhaps, the division semantics with the remainder of negative numbers is not preserved.
    }
};


/** Specializations are specified for dividing numbers of the type UInt64 and UInt32 by the numbers of the same sign.
  * Can be expanded to all possible combinations, but more code is needed.
  */

template <> struct BinaryOperationImpl<UInt64, UInt8, DivideIntegralImpl<UInt64, UInt8>> : DivideIntegralByConstantImpl<UInt64, UInt8> {};
template <> struct BinaryOperationImpl<UInt64, UInt16, DivideIntegralImpl<UInt64, UInt16>> : DivideIntegralByConstantImpl<UInt64, UInt16> {};
template <> struct BinaryOperationImpl<UInt64, UInt32, DivideIntegralImpl<UInt64, UInt32>> : DivideIntegralByConstantImpl<UInt64, UInt32> {};
template <> struct BinaryOperationImpl<UInt64, UInt64, DivideIntegralImpl<UInt64, UInt64>> : DivideIntegralByConstantImpl<UInt64, UInt64> {};

template <> struct BinaryOperationImpl<UInt32, UInt8, DivideIntegralImpl<UInt32, UInt8>> : DivideIntegralByConstantImpl<UInt32, UInt8> {};
template <> struct BinaryOperationImpl<UInt32, UInt16, DivideIntegralImpl<UInt32, UInt16>> : DivideIntegralByConstantImpl<UInt32, UInt16> {};
template <> struct BinaryOperationImpl<UInt32, UInt32, DivideIntegralImpl<UInt32, UInt32>> : DivideIntegralByConstantImpl<UInt32, UInt32> {};
template <> struct BinaryOperationImpl<UInt32, UInt64, DivideIntegralImpl<UInt32, UInt64>> : DivideIntegralByConstantImpl<UInt32, UInt64> {};

template <> struct BinaryOperationImpl<Int64, Int8, DivideIntegralImpl<Int64, Int8>> : DivideIntegralByConstantImpl<Int64, Int8> {};
template <> struct BinaryOperationImpl<Int64, Int16, DivideIntegralImpl<Int64, Int16>> : DivideIntegralByConstantImpl<Int64, Int16> {};
template <> struct BinaryOperationImpl<Int64, Int32, DivideIntegralImpl<Int64, Int32>> : DivideIntegralByConstantImpl<Int64, Int32> {};
template <> struct BinaryOperationImpl<Int64, Int64, DivideIntegralImpl<Int64, Int64>> : DivideIntegralByConstantImpl<Int64, Int64> {};

template <> struct BinaryOperationImpl<Int32, Int8, DivideIntegralImpl<Int32, Int8>> : DivideIntegralByConstantImpl<Int32, Int8> {};
template <> struct BinaryOperationImpl<Int32, Int16, DivideIntegralImpl<Int32, Int16>> : DivideIntegralByConstantImpl<Int32, Int16> {};
template <> struct BinaryOperationImpl<Int32, Int32, DivideIntegralImpl<Int32, Int32>> : DivideIntegralByConstantImpl<Int32, Int32> {};
template <> struct BinaryOperationImpl<Int32, Int64, DivideIntegralImpl<Int32, Int64>> : DivideIntegralByConstantImpl<Int32, Int64> {};


template <> struct BinaryOperationImpl<UInt64, UInt8, ModuloImpl<UInt64, UInt8>> : ModuloByConstantImpl<UInt64, UInt8> {};
template <> struct BinaryOperationImpl<UInt64, UInt16, ModuloImpl<UInt64, UInt16>> : ModuloByConstantImpl<UInt64, UInt16> {};
template <> struct BinaryOperationImpl<UInt64, UInt32, ModuloImpl<UInt64, UInt32>> : ModuloByConstantImpl<UInt64, UInt32> {};
template <> struct BinaryOperationImpl<UInt64, UInt64, ModuloImpl<UInt64, UInt64>> : ModuloByConstantImpl<UInt64, UInt64> {};

template <> struct BinaryOperationImpl<UInt32, UInt8, ModuloImpl<UInt32, UInt8>> : ModuloByConstantImpl<UInt32, UInt8> {};
template <> struct BinaryOperationImpl<UInt32, UInt16, ModuloImpl<UInt32, UInt16>> : ModuloByConstantImpl<UInt32, UInt16> {};
template <> struct BinaryOperationImpl<UInt32, UInt32, ModuloImpl<UInt32, UInt32>> : ModuloByConstantImpl<UInt32, UInt32> {};
template <> struct BinaryOperationImpl<UInt32, UInt64, ModuloImpl<UInt32, UInt64>> : ModuloByConstantImpl<UInt32, UInt64> {};

template <> struct BinaryOperationImpl<Int64, Int8, ModuloImpl<Int64, Int8>> : ModuloByConstantImpl<Int64, Int8> {};
template <> struct BinaryOperationImpl<Int64, Int16, ModuloImpl<Int64, Int16>> : ModuloByConstantImpl<Int64, Int16> {};
template <> struct BinaryOperationImpl<Int64, Int32, ModuloImpl<Int64, Int32>> : ModuloByConstantImpl<Int64, Int32> {};
template <> struct BinaryOperationImpl<Int64, Int64, ModuloImpl<Int64, Int64>> : ModuloByConstantImpl<Int64, Int64> {};

template <> struct BinaryOperationImpl<Int32, Int8, ModuloImpl<Int32, Int8>> : ModuloByConstantImpl<Int32, Int8> {};
template <> struct BinaryOperationImpl<Int32, Int16, ModuloImpl<Int32, Int16>> : ModuloByConstantImpl<Int32, Int16> {};
template <> struct BinaryOperationImpl<Int32, Int32, ModuloImpl<Int32, Int32>> : ModuloByConstantImpl<Int32, Int32> {};
template <> struct BinaryOperationImpl<Int32, Int64, ModuloImpl<Int32, Int64>> : ModuloByConstantImpl<Int32, Int64> {};


template <typename Impl, typename Name>
struct FunctionBitTestMany : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitTestMany>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception{
                "Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be at least 2.",
                ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION};

        const auto first_arg = arguments.front().get();

        if (!first_arg->isInteger())
            throw Exception{
                "Illegal type " + first_arg->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};


        for (const auto i : ext::range(1, arguments.size()))
        {
            const auto pos_arg = arguments[i].get();

            if (!pos_arg->isUnsignedInteger())
                throw Exception{
                    "Illegal type " + pos_arg->getName() + " of " + toString(i) + " argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto value_col = block.getByPosition(arguments.front()).column.get();

        if (!execute<UInt8>(block, arguments, result, value_col)
            && !execute<UInt16>(block, arguments, result, value_col)
            && !execute<UInt32>(block, arguments, result, value_col)
            && !execute<UInt64>(block, arguments, result, value_col)
            && !execute<Int8>(block, arguments, result, value_col)
            && !execute<Int16>(block, arguments, result, value_col)
            && !execute<Int32>(block, arguments, result, value_col)
            && !execute<Int64>(block, arguments, result, value_col))
            throw Exception{
                "Illegal column " + value_col->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
    }

private:
    template <typename T>
    bool execute(
        Block & block, const ColumnNumbers & arguments, const size_t result,
        const IColumn * const value_col_untyped)
    {
        if (const auto value_col = checkAndGetColumn<ColumnVector<T>>(value_col_untyped))
        {
            const auto size = value_col->size();
            bool is_const;
            const auto mask = createConstMask<T>(block, arguments, is_const);
            const auto & val = value_col->getData();

            auto out_col = ColumnVector<UInt8>::create(size);
            auto & out = out_col->getData();

            if (is_const)
            {
                for (const auto i : ext::range(0, size))
                    out[i] = Impl::apply(val[i], mask);
            }
            else
            {
                const auto mask = createMask<T>(size, block, arguments);

                for (const auto i : ext::range(0, size))
                    out[i] = Impl::apply(val[i], mask[i]);
            }

            block.getByPosition(result).column = std::move(out_col);
            return true;
        }
        else if (const auto value_col = checkAndGetColumnConst<ColumnVector<T>>(value_col_untyped))
        {
            const auto size = value_col->size();
            bool is_const;
            const auto mask = createConstMask<T>(block, arguments, is_const);
            const auto val = value_col->template getValue<T>();

            if (is_const)
            {
                block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(size, toField(Impl::apply(val, mask)));
            }
            else
            {
                const auto mask = createMask<T>(size, block, arguments);
                auto out_col = ColumnVector<UInt8>::create(size);

                auto & out = out_col->getData();

                for (const auto i : ext::range(0, size))
                    out[i] = Impl::apply(val, mask[i]);

                block.getByPosition(result).column = std::move(out_col);
            }

            return true;
        }

        return false;
    }

    template <typename ValueType>
    ValueType createConstMask(const Block & block, const ColumnNumbers & arguments, bool & is_const)
    {
        is_const = true;
        ValueType mask = 0;

        for (const auto i : ext::range(1, arguments.size()))
        {
            if (auto pos_col_const = checkAndGetColumnConst<ColumnVector<ValueType>>(block.getByPosition(arguments[i]).column.get()))
            {
                const auto pos = pos_col_const->template getValue<ValueType>();
                mask = mask | (1 << pos);
            }
            else
            {
                is_const = false;
                return {};
            }
        }

        return mask;
    }

    template <typename ValueType>
    PaddedPODArray<ValueType> createMask(const size_t size, const Block & block, const ColumnNumbers & arguments)
    {
        PaddedPODArray<ValueType> mask(size, ValueType{});

        for (const auto i : ext::range(1, arguments.size()))
        {
            const auto pos_col = block.getByPosition(arguments[i]).column.get();

            if (!addToMaskImpl<UInt8>(mask, pos_col)
                && !addToMaskImpl<UInt16>(mask, pos_col)
                && !addToMaskImpl<UInt32>(mask, pos_col)
                && !addToMaskImpl<UInt64>(mask, pos_col))
                throw Exception{
                    "Illegal column " + pos_col->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN};
        }

        return mask;
    }

    template <typename PosType, typename ValueType>
    bool addToMaskImpl(PaddedPODArray<ValueType> & mask, const IColumn * const pos_col_untyped)
    {
        if (const auto pos_col = checkAndGetColumn<ColumnVector<PosType>>(pos_col_untyped))
        {
            const auto & pos = pos_col->getData();

            for (const auto i : ext::range(0, mask.size()))
                mask[i] = mask[i] | (1 << pos[i]);

            return true;
        }
        else if (const auto pos_col = checkAndGetColumnConst<ColumnVector<PosType>>(pos_col_untyped))
        {
            const auto & pos = pos_col->template getValue<PosType>();
            const auto new_mask = 1 << pos;

            for (const auto i : ext::range(0, mask.size()))
                mask[i] = mask[i] | new_mask;

            return true;
        }

        return false;
    }
};


struct BitTestAnyImpl
{
    template <typename A, typename B>
    static inline UInt8 apply(A a, B b) { return (a & b) != 0; };
};

struct BitTestAllImpl
{
    template <typename A, typename B>
    static inline UInt8 apply(A a, B b) { return (a & b) == b; };
};


using FunctionBitTestAny = FunctionBitTestMany<BitTestAnyImpl, NameBitTestAny>;
using FunctionBitTestAll = FunctionBitTestMany<BitTestAllImpl, NameBitTestAll>;

}
