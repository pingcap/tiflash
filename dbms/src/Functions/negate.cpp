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
        auto & result_data = result_column->getData();
        result_data.resize(argument_column->size());

        auto execute = [&](const auto * column, auto apply) {
            if (column == nullptr)
                return false;
            const auto & data = column->getData();
            for (size_t i = 0; i < data.size(); ++i)
                result_data[i] = apply(data[i]);
            return true;
        };

        if (execute(
                checkAndGetColumn<ColumnUInt8>(argument_column.get()),
                [](UInt8 value) { return -static_cast<Int64>(value); })
            || execute(
                checkAndGetColumn<ColumnUInt16>(argument_column.get()),
                [](UInt16 value) { return -static_cast<Int64>(value); })
            || execute(
                checkAndGetColumn<ColumnUInt32>(argument_column.get()),
                [](UInt32 value) { return -static_cast<Int64>(value); })
            || execute(
                checkAndGetColumn<ColumnUInt64>(argument_column.get()),
                [](UInt64 value) {
                    constexpr UInt64 signed_min_abs = static_cast<UInt64>(std::numeric_limits<Int64>::max()) + 1;
                    if (value > signed_min_abs)
                        throw Exception(
                            fmt::format("BIGINT value is out of range in '-{}'", value),
                            ErrorCodes::DECIMAL_OVERFLOW);
                    if (value == signed_min_abs)
                        return std::numeric_limits<Int64>::min();
                    return -static_cast<Int64>(value);
                })
            || execute(
                checkAndGetColumn<ColumnInt8>(argument_column.get()),
                [](Int8 value) { return -static_cast<Int64>(value); })
            || execute(
                checkAndGetColumn<ColumnInt16>(argument_column.get()),
                [](Int16 value) { return -static_cast<Int64>(value); })
            || execute(
                checkAndGetColumn<ColumnInt32>(argument_column.get()),
                [](Int32 value) { return -static_cast<Int64>(value); })
            || execute(checkAndGetColumn<ColumnInt64>(argument_column.get()), [](Int64 value) {
                   if (value == std::numeric_limits<Int64>::min())
                       throw Exception(
                           fmt::format("BIGINT value is out of range in '-{}'", value),
                           ErrorCodes::DECIMAL_OVERFLOW);
                   return -value;
               }))
        {
            block.getByPosition(result).column = std::move(result_column);
            return;
        }

        throw Exception(
            fmt::format("Illegal column {} of argument of function {}", argument_column->getName(), getName()),
            ErrorCodes::ILLEGAL_COLUMN);
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
        auto & result_data = result_column->getData();
        result_data.resize(argument_column->size());

        auto execute = [&](const auto * column) {
            if (column == nullptr)
                return false;
            const auto & data = column->getData();
            for (size_t i = 0; i < data.size(); ++i)
                result_data[i] = -static_cast<Float64>(data[i]);
            return true;
        };

        if (execute(checkAndGetColumn<ColumnFloat32>(argument_column.get()))
            || execute(checkAndGetColumn<ColumnFloat64>(argument_column.get())))
        {
            block.getByPosition(result).column = std::move(result_column);
            return;
        }

        throw Exception(
            fmt::format("Illegal column {} of argument of function {}", argument_column->getName(), getName()),
            ErrorCodes::ILLEGAL_COLUMN);
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

        auto execute = [&](auto dummy_decimal) {
            using DecimalType = decltype(dummy_decimal);
            using ColumnType = ColumnDecimal<DecimalType>;
            const auto * column = checkAndGetColumn<ColumnType>(argument_column.get());
            if (column == nullptr)
                return false;

            auto result_column = ColumnType::create(0, column->getData().getScale());
            auto & result_data = result_column->getData();
            result_data.resize(column->getData().size());
            const auto & input_data = column->getData();
            for (size_t i = 0; i < input_data.size(); ++i)
                result_data[i] = DecimalType(-input_data[i].value);

            block.getByPosition(result).column = std::move(result_column);
            return true;
        };

        if (execute(Decimal32()) || execute(Decimal64()) || execute(Decimal128()) || execute(Decimal256()))
            return;

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
