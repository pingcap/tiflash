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

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionUnaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
extern const int OVERFLOW_ERROR;
} // namespace ErrorCodes

namespace
{
template <typename A>
struct NegateImpl
{
    using ResultType = typename NumberTraits::ResultOfNegate<A>::Type;

    static ResultType apply(A a)
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

// clang-format off
struct NameNegate               { static constexpr auto name = "negate"; };
// clang-format on

using FunctionNegate = FunctionUnaryArithmetic<NegateImpl, NameNegate, true>;

template <typename ColumnType, typename ResultColumnType, typename Op>
bool executeUnaryMinusTyped(const IColumn * argument_column, ResultColumnType & result_column)
{
    const auto * column = checkAndGetColumn<ColumnType>(argument_column);
    if (column == nullptr)
        return false;

    const auto & input_data = column->getData();
    auto & result_data = result_column.getData();
    result_data.resize(input_data.size());
    for (size_t i = 0; i < input_data.size(); ++i)
        result_data[i] = Op::apply(input_data[i]);
    return true;
}

template <typename SourceType>
struct TiDBUnaryMinusIntOp
{
    static Int64 apply(SourceType value) { return -static_cast<Int64>(value); }
};

template <>
struct TiDBUnaryMinusIntOp<UInt64>
{
    static Int64 apply(UInt64 value)
    {
        constexpr UInt64 signed_min_abs = static_cast<UInt64>(std::numeric_limits<Int64>::max()) + 1;
        if (unlikely(value > signed_min_abs))
            throw Exception(fmt::format("BIGINT value is out of range in '-{}'", value), ErrorCodes::OVERFLOW_ERROR);
        if (unlikely(value == signed_min_abs))
            return std::numeric_limits<Int64>::min();
        return -static_cast<Int64>(value);
    }
};

template <>
struct TiDBUnaryMinusIntOp<Int64>
{
    static Int64 apply(Int64 value)
    {
        if (unlikely(value == std::numeric_limits<Int64>::min()))
            throw Exception(fmt::format("BIGINT value is out of range in '-{}'", value), ErrorCodes::OVERFLOW_ERROR);
        return -value;
    }
};

template <typename SourceType>
struct TiDBUnaryMinusRealOp
{
    static Float64 apply(SourceType value) { return -static_cast<Float64>(value); }
};

template <typename DecimalType>
struct TiDBUnaryMinusDecimalOp
{
    static DecimalType apply(DecimalType value) { return DecimalType(-value.value); }
};

template <typename DecimalType, typename Op>
bool executeUnaryMinusDecimal(const IColumn * argument_column, ColumnPtr & result_column)
{
    using ColumnType = ColumnDecimal<DecimalType>;
    const auto * column = checkAndGetColumn<ColumnType>(argument_column);
    if (column == nullptr)
        return false;

    auto typed_result_column = ColumnType::create(0, column->getData().getScale());
    const auto & input_data = column->getData();
    auto & result_data = typed_result_column->getData();
    result_data.resize(input_data.size());
    for (size_t i = 0; i < input_data.size(); ++i)
        result_data[i] = Op::apply(input_data[i]);

    result_column = std::move(typed_result_column);
    return true;
}

template <typename Name>
class FunctionTiDBUnaryMinusInt : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTiDBUnaryMinusInt>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isInteger())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto & argument_column = block.getByPosition(arguments[0]).column;
        auto result_column = ColumnInt64::create();
        if (!(executeUnaryMinusTyped<ColumnUInt8, ColumnInt64, TiDBUnaryMinusIntOp<UInt8>>(
                  argument_column.get(),
                  *result_column)
              || executeUnaryMinusTyped<ColumnUInt16, ColumnInt64, TiDBUnaryMinusIntOp<UInt16>>(
                  argument_column.get(),
                  *result_column)
              || executeUnaryMinusTyped<ColumnUInt32, ColumnInt64, TiDBUnaryMinusIntOp<UInt32>>(
                  argument_column.get(),
                  *result_column)
              || executeUnaryMinusTyped<ColumnUInt64, ColumnInt64, TiDBUnaryMinusIntOp<UInt64>>(
                  argument_column.get(),
                  *result_column)
              || executeUnaryMinusTyped<ColumnInt8, ColumnInt64, TiDBUnaryMinusIntOp<Int8>>(
                  argument_column.get(),
                  *result_column)
              || executeUnaryMinusTyped<ColumnInt16, ColumnInt64, TiDBUnaryMinusIntOp<Int16>>(
                  argument_column.get(),
                  *result_column)
              || executeUnaryMinusTyped<ColumnInt32, ColumnInt64, TiDBUnaryMinusIntOp<Int32>>(
                  argument_column.get(),
                  *result_column)
              || executeUnaryMinusTyped<ColumnInt64, ColumnInt64, TiDBUnaryMinusIntOp<Int64>>(
                  argument_column.get(),
                  *result_column)))
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", argument_column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = std::move(result_column);
    }
};

template <typename Name>
class FunctionTiDBUnaryMinusReal : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTiDBUnaryMinusReal>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isFloatingPoint())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeFloat64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto & argument_column = block.getByPosition(arguments[0]).column;
        auto result_column = ColumnFloat64::create();
        if (!(executeUnaryMinusTyped<ColumnFloat32, ColumnFloat64, TiDBUnaryMinusRealOp<Float32>>(
                  argument_column.get(),
                  *result_column)
              || executeUnaryMinusTyped<ColumnFloat64, ColumnFloat64, TiDBUnaryMinusRealOp<Float64>>(
                  argument_column.get(),
                  *result_column)))
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", argument_column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = std::move(result_column);
    }
};

template <typename Name>
class FunctionTiDBUnaryMinusDecimal : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTiDBUnaryMinusDecimal>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isDecimal())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return arguments[0];
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto & argument_column = block.getByPosition(arguments[0]).column;
        if (!(executeUnaryMinusDecimal<Decimal32, TiDBUnaryMinusDecimalOp<Decimal32>>(
                  argument_column.get(),
                  block.getByPosition(result).column)
              || executeUnaryMinusDecimal<Decimal64, TiDBUnaryMinusDecimalOp<Decimal64>>(
                  argument_column.get(),
                  block.getByPosition(result).column)
              || executeUnaryMinusDecimal<Decimal128, TiDBUnaryMinusDecimalOp<Decimal128>>(
                  argument_column.get(),
                  block.getByPosition(result).column)
              || executeUnaryMinusDecimal<Decimal256, TiDBUnaryMinusDecimalOp<Decimal256>>(
                  argument_column.get(),
                  block.getByPosition(result).column)))
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", argument_column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

// clang-format off
struct NameTiDBUnaryMinusInt      { static constexpr auto name = "tidbUnaryMinusInt"; };
struct NameTiDBUnaryMinusReal     { static constexpr auto name = "tidbUnaryMinusReal"; };
struct NameTiDBUnaryMinusDecimal  { static constexpr auto name = "tidbUnaryMinusDecimal"; };
// clang-format on

using FunctionTiDBUnaryMinusIntImpl = FunctionTiDBUnaryMinusInt<NameTiDBUnaryMinusInt>;
using FunctionTiDBUnaryMinusRealImpl = FunctionTiDBUnaryMinusReal<NameTiDBUnaryMinusReal>;
using FunctionTiDBUnaryMinusDecimalImpl = FunctionTiDBUnaryMinusDecimal<NameTiDBUnaryMinusDecimal>;

} // namespace

template <>
struct FunctionUnaryArithmeticMonotonicity<NameNegate>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field &, const Field &) { return {true, false}; }
};

void registerFunctionNegate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNegate>();
    factory.registerFunction<FunctionTiDBUnaryMinusIntImpl>();
    factory.registerFunction<FunctionTiDBUnaryMinusRealImpl>();
    factory.registerFunction<FunctionTiDBUnaryMinusDecimalImpl>();
}

} // namespace DB
