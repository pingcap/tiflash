#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitors.h>
#include <Common/toSafeUnsigned.h>
#include <Common/typeid_cast.h>
#include <Core/AccurateComparison.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>
#include <Functions/DataTypeFromFieldType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <common/intExp.h>

#include <boost/integer/common_factor.hpp>
#include <ext/range.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
extern const int DECIMAL_OVERFLOW;
}

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

template <typename A>
struct NegateImpl
{
    using ResultType = typename NumberTraits::ResultOfNegate<A>::Type;

    static inline ResultType apply(A a)
    {
        if constexpr (IsDecimal<A>)
        {
            return static_cast<ResultType>(-a.value);
        }
        else
        {
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
            if unlikely (a == INT64_MIN)
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
            if constexpr (IsDecimal<typename T0::FieldType>)
            {
                auto t = static_cast<const T0 *>(arguments[0].get());
                result = std::make_shared<T0>(t->getPrec(), t->getScale());
            }
            else
            {
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
    bool executeDecimalType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (const ColumnDecimal<T0> * col = checkAndGetColumn<ColumnDecimal<T0>>(block.getByPosition(arguments[0]).column.get()))
        {
            using ResultType = typename Op<T0>::ResultType;

            if constexpr (IsDecimal<ResultType>)
            {
                auto col_res = ColumnDecimal<ResultType>::create(0, col->getData().getScale());

                typename ColumnDecimal<ResultType>::Container & vec_res = col_res->getData();
                vec_res.resize(col->getData().size());
                UnaryOperationImpl<T0, Op<T0>>::vector(col->getData(), vec_res);

                block.getByPosition(result).column = std::move(col_res);
                return true;
            }
            else
            {
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

        if (!(checkType<DataTypeUInt8>(arguments, result)
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
        if (!(executeType<UInt8>(block, arguments, result)
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


struct NameNegate               { static constexpr auto name = "negate"; };
struct NameAbs                  { static constexpr auto name = "abs"; };
struct NameBitNot               { static constexpr auto name = "bitNot"; };
struct NameIntExp2              { static constexpr auto name = "intExp2"; };
struct NameIntExp10             { static constexpr auto name = "intExp10"; };

using FunctionNegate = FunctionUnaryArithmetic<NegateImpl, NameNegate, true>;
using FunctionAbs = FunctionUnaryArithmetic<AbsImpl, NameAbs, false>;
using FunctionBitNot = FunctionUnaryArithmetic<BitNotImpl, NameBitNot, true>;

/// Assumed to be injective for the purpose of query optimization, but in fact it is not injective because of possible overflow.
using FunctionIntExp2 = FunctionUnaryArithmetic<IntExp2Impl, NameIntExp2, true>;
using FunctionIntExp10 = FunctionUnaryArithmetic<IntExp10Impl, NameIntExp10, true>;

/// Monotonicity properties for some functions.

template <>
struct FunctionUnaryArithmeticMonotonicity<NameNegate>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return {true, false};
    }
};

template <>
struct FunctionUnaryArithmeticMonotonicity<NameAbs>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if ((left_float < 0 && right_float > 0) || (left_float > 0 && right_float < 0))
            return {};

        return {true, (left_float > 0)};
    }
};

template <>
struct FunctionUnaryArithmeticMonotonicity<NameBitNot>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return {};
    }
};

template <>
struct FunctionUnaryArithmeticMonotonicity<NameIntExp2>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if (left_float < 0 || right_float > 63)
            return {};

        return {true};
    }
};

template <>
struct FunctionUnaryArithmeticMonotonicity<NameIntExp10>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if (left_float < 0 || right_float > 19)
            return {};

        return {true};
    }
};

}