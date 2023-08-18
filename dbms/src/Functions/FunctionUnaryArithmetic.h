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
#include <fmt/core.h>

#include <boost/integer/common_factor.hpp>
#include <ext/range.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
extern const int DECIMAL_OVERFLOW;
} // namespace ErrorCodes

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

    static void constant(A a, ResultType & c) { c = Op::apply(a); }
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
                result = std::make_shared<
                    typename DataTypeFromFieldType<typename Op<typename T0::FieldType>::ResultType>::Type>();
            }
            return true;
        }
        return false;
    }

    template <typename T0>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        if (const ColumnVector<T0> * col
            = checkAndGetColumn<ColumnVector<T0>>(block.getByPosition(arguments[0]).column.get()))
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
    bool executeDecimalType(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        if (const ColumnDecimal<T0> * col
            = checkAndGetColumn<ColumnDecimal<T0>>(block.getByPosition(arguments[0]).column.get()))
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
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) const override { return is_injective; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr result;
        if (!(checkType<DataTypeUInt8>(arguments, result) || checkType<DataTypeUInt16>(arguments, result)
              || checkType<DataTypeUInt32>(arguments, result) || checkType<DataTypeUInt64>(arguments, result)
              || checkType<DataTypeInt8>(arguments, result) || checkType<DataTypeInt16>(arguments, result)
              || checkType<DataTypeInt32>(arguments, result) || checkType<DataTypeInt64>(arguments, result)
              || checkType<DataTypeFloat32>(arguments, result) || checkType<DataTypeDecimal32>(arguments, result)
              || checkType<DataTypeDecimal64>(arguments, result) || checkType<DataTypeDecimal128>(arguments, result)
              || checkType<DataTypeDecimal256>(arguments, result) || checkType<DataTypeFloat64>(arguments, result)))
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return result;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        if (!(executeType<UInt8>(block, arguments, result) || executeType<UInt16>(block, arguments, result)
              || executeType<UInt32>(block, arguments, result) || executeType<UInt64>(block, arguments, result)
              || executeType<Int8>(block, arguments, result) || executeType<Int16>(block, arguments, result)
              || executeType<Int32>(block, arguments, result) || executeType<Int64>(block, arguments, result)
              || executeDecimalType<Decimal32>(block, arguments, result)
              || executeDecimalType<Decimal64>(block, arguments, result)
              || executeDecimalType<Decimal128>(block, arguments, result)
              || executeDecimalType<Decimal256>(block, arguments, result)
              || executeType<Float32>(block, arguments, result) || executeType<Float64>(block, arguments, result)))
            throw Exception(
                fmt::format(
                    "Illegal column {} of argument of function {}",
                    block.getByPosition(arguments[0]).column->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    bool hasInformationAboutMonotonicity() const override { return FunctionUnaryArithmeticMonotonicity<Name>::has(); }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field & left, const Field & right) const override
    {
        return FunctionUnaryArithmeticMonotonicity<Name>::get(left, right);
    }
};

} // namespace DB
