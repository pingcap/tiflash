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
#include <Columns/ColumnsNumber.h>
#include <Common/config.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsGeo.h>
#include <Functions/IFunction.h>
#include <common/preciseExp10.h>
#include <fmt/core.h>

/** More efficient implementations of mathematical functions are possible when using a separate library.
  * Disabled due to licence compatibility limitations.
  * To enable: download http://www.agner.org/optimize/vectorclass.zip and unpack to contrib/vectorclass
  * Then rebuild with -DENABLE_VECTORCLASS=1
  */

#if USE_VECTORCLASS
#if __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshift-negative-value"
#endif

#include <vectorf128.h>
#include <vectormath_exp.h>
#include <vectormath_trig.h>

#if __clang__
#pragma clang diagnostic pop
#endif
#endif


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

template <typename Impl>
class FunctionMathNullaryConstFloat64 : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMathNullaryConstFloat64>(); }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & /*arguments*/, const size_t result) const override
    {
        block.getByPosition(result).column
            = block.getByPosition(result).type->createColumnConst(block.rows(), Impl::value);
    }
};


template <typename Impl, bool Nullable = false>
class FunctionMathUnaryFloat64 : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMathUnaryFloat64>(); }
    static_assert(Impl::rows_per_iteration > 0, "Impl must process at least one row per iteration");

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments.front()->isNumber())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments.front()->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if constexpr (Nullable)
        {
            return makeNullable(std::make_shared<DataTypeFloat64>());
        }
        else
        {
            return std::make_shared<DataTypeFloat64>();
        }
    }

    template <typename FieldType>
    bool execute(Block & block, const IColumn * arg, const size_t result) const
    {
        if (const auto col = checkAndGetColumn<ColumnVector<FieldType>>(arg))
        {
            auto dst = ColumnVector<Float64>::create();

            const auto & src_data = col->getData();
            const auto src_size = src_data.size();
            auto & dst_data = dst->getData();
            dst_data.resize(src_size);

            const auto rows_remaining = src_size % Impl::rows_per_iteration;
            const auto rows_size = src_size - rows_remaining;

            if constexpr (Nullable)
            {
                auto null_map = ColumnUInt8::create();
                auto & null_map_data = null_map->getData();
                null_map_data.resize(src_size);

                for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                    Impl::execute(&src_data[i], &dst_data[i], &null_map_data[i]);

                if (rows_remaining != 0)
                {
                    FieldType src_remaining[Impl::rows_per_iteration];
                    memcpy(src_remaining, &src_data[rows_size], rows_remaining * sizeof(FieldType));
                    memset(
                        src_remaining + rows_remaining,
                        0,
                        (Impl::rows_per_iteration - rows_remaining) * sizeof(FieldType));
                    Float64 dst_remaining[Impl::rows_per_iteration];
                    UInt8 null_map_remaining[Impl::rows_per_iteration];


                    Impl::execute(src_remaining, dst_remaining, null_map_remaining);

                    memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
                    memcpy(&null_map_data[rows_size], null_map_remaining, rows_remaining * sizeof(UInt8));
                }

                block.getByPosition(result).column = ColumnNullable::create(std::move(dst), std::move(null_map));
            }
            else
            {
                for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                    Impl::execute(&src_data[i], &dst_data[i]);

                if (rows_remaining != 0)
                {
                    FieldType src_remaining[Impl::rows_per_iteration];
                    memcpy(src_remaining, &src_data[rows_size], rows_remaining * sizeof(FieldType));
                    memset(
                        src_remaining + rows_remaining,
                        0,
                        (Impl::rows_per_iteration - rows_remaining) * sizeof(FieldType));
                    Float64 dst_remaining[Impl::rows_per_iteration];

                    Impl::execute(src_remaining, dst_remaining);

                    memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
                }

                block.getByPosition(result).column = std::move(dst);
            }
            return true;
        }

        return false;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        const auto * const arg = block.getByPosition(arguments[0]).column.get();

        if (!execute<UInt8>(block, arg, result) && !execute<UInt16>(block, arg, result)
            && !execute<UInt32>(block, arg, result) && !execute<UInt64>(block, arg, result)
            && !execute<Int8>(block, arg, result) && !execute<Int16>(block, arg, result)
            && !execute<Int32>(block, arg, result) && !execute<Int64>(block, arg, result)
            && !execute<Float32>(block, arg, result) && !execute<Float64>(block, arg, result))
        {
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", arg->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};

template <typename Impl>
using FunctionMathUnaryFloat64Nullable = FunctionMathUnaryFloat64<Impl, true>;

template <typename Name, bool(Function)(Float64, Float64 &)>
struct UnaryFunctionNullablePlain
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T>
    static void execute(const T * src, Float64 * dst, UInt8 * is_null)
    {
        *is_null = Function(static_cast<Float64>(*src), *dst);
    }
};

template <typename Name, Float64(Function)(Float64)>
struct UnaryFunctionPlain
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T>
    static void execute(const T * src, Float64 * dst)
    {
        dst[0] = static_cast<Float64>(Function(static_cast<Float64>(src[0])));
    }
};

#if USE_VECTORCLASS

template <typename Name, Vec2d(Function)(const Vec2d &)>
struct UnaryFunctionVectorized
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 2;

    template <typename T>
    static void execute(const T * src, Float64 * dst)
    {
        const auto result = Function(Vec2d(src[0], src[1]));
        result.store(dst);
    }
};

#else

#define UnaryFunctionVectorized UnaryFunctionPlain
#define UnaryFunctionNullableVectorized UnaryFunctionNullablePlain

#endif


template <typename Impl, bool Nullable = false>
class FunctionMathBinaryFloat64 : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMathBinaryFloat64>(); }
    static_assert(Impl::rows_per_iteration > 0, "Impl must process at least one row per iteration");

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto check_argument_type = [this](const IDataType * arg) {
            if (!arg->isNumber())
                throw Exception(
                    fmt::format("Illegal type {} of argument of function {}", arg->getName(), getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        };

        check_argument_type(arguments.front().get());
        check_argument_type(arguments.back().get());
        if constexpr (Nullable)
        {
            return makeNullable(std::make_shared<DataTypeFloat64>());
        }
        else
        {
            return std::make_shared<DataTypeFloat64>();
        }
    }

    template <typename LeftType, typename RightType>
    bool executeRight(Block & block, const size_t result, const ColumnConst * left_arg, const IColumn * right_arg) const
    {
        if (const auto right_arg_typed = checkAndGetColumn<ColumnVector<RightType>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();

            LeftType left_src_data[Impl::rows_per_iteration];
            std::fill(std::begin(left_src_data), std::end(left_src_data), left_arg->template getValue<LeftType>());
            const auto & right_src_data = right_arg_typed->getData();
            const auto src_size = right_src_data.size();
            auto & dst_data = dst->getData();
            dst_data.resize(src_size);

            const auto rows_remaining = src_size % Impl::rows_per_iteration;
            const auto rows_size = src_size - rows_remaining;

            if constexpr (Nullable)
            {
                auto null_map = ColumnUInt8::create();
                auto & null_map_data = null_map->getData();
                null_map_data.resize(src_size);
                for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                {
                    Impl::execute(left_src_data, &right_src_data[i], &dst_data[i], &null_map_data[i]);
                }
                if (rows_remaining != 0)
                {
                    RightType right_src_remaining[Impl::rows_per_iteration];
                    memcpy(right_src_remaining, &right_src_data[rows_size], rows_remaining * sizeof(RightType));
                    memset(
                        right_src_remaining + rows_remaining,
                        0,
                        (Impl::rows_per_iteration - rows_remaining) * sizeof(RightType));
                    Float64 dst_remaining[Impl::rows_per_iteration];
                    UInt8 null_map_remaining[Impl::rows_per_iteration];

                    Impl::execute(left_src_data, right_src_remaining, dst_remaining, null_map_remaining);

                    memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
                    memcpy(&null_map_data[rows_size], null_map_remaining, rows_remaining * sizeof(UInt8));
                }

                block.getByPosition(result).column = ColumnNullable::create(std::move(dst), std::move(null_map));
            }
            else
            {
                for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                    Impl::execute(left_src_data, &right_src_data[i], &dst_data[i]);

                if (rows_remaining != 0)
                {
                    RightType right_src_remaining[Impl::rows_per_iteration];
                    memcpy(right_src_remaining, &right_src_data[rows_size], rows_remaining * sizeof(RightType));
                    memset(
                        right_src_remaining + rows_remaining,
                        0,
                        (Impl::rows_per_iteration - rows_remaining) * sizeof(RightType));
                    Float64 dst_remaining[Impl::rows_per_iteration];

                    Impl::execute(left_src_data, right_src_remaining, dst_remaining);

                    memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
                }

                block.getByPosition(result).column = std::move(dst);
            }

            return true;
        }

        return false;
    }

    template <typename LeftType, typename RightType>
    bool executeRight(
        Block & block,
        const size_t result,
        const ColumnVector<LeftType> * left_arg,
        const IColumn * right_arg) const
    {
        if (const auto right_arg_typed = checkAndGetColumn<ColumnVector<RightType>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();

            const auto & left_src_data = left_arg->getData();
            const auto & right_src_data = right_arg_typed->getData();
            const auto src_size = left_src_data.size();
            auto & dst_data = dst->getData();
            dst_data.resize(src_size);

            const auto rows_remaining = src_size % Impl::rows_per_iteration;
            const auto rows_size = src_size - rows_remaining;

            if constexpr (Nullable)
            {
                auto null_map = ColumnUInt8::create();
                auto & null_map_data = null_map->getData();
                null_map_data.resize(src_size);

                for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                    Impl::execute(&left_src_data[i], &right_src_data[i], &dst_data[i], &null_map_data[i]);

                if (rows_remaining != 0)
                {
                    LeftType left_src_remaining[Impl::rows_per_iteration];
                    memcpy(left_src_remaining, &left_src_data[rows_size], rows_remaining * sizeof(LeftType));
                    memset(
                        left_src_remaining + rows_remaining,
                        0,
                        (Impl::rows_per_iteration - rows_remaining) * sizeof(LeftType));
                    RightType right_src_remaining[Impl::rows_per_iteration];
                    memcpy(right_src_remaining, &right_src_data[rows_size], rows_remaining * sizeof(RightType));
                    memset(
                        right_src_remaining + rows_remaining,
                        0,
                        (Impl::rows_per_iteration - rows_remaining) * sizeof(RightType));
                    Float64 dst_remaining[Impl::rows_per_iteration];
                    UInt8 null_map_remaining[Impl::rows_per_iteration];

                    Impl::execute(left_src_remaining, right_src_remaining, dst_remaining, null_map_remaining);

                    memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
                    memcpy(&null_map_data[rows_size], null_map_remaining, rows_remaining * sizeof(UInt8));
                }

                block.getByPosition(result).column = ColumnNullable::create(std::move(dst), std::move(null_map));
            }
            else
            {
                for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                    Impl::execute(&left_src_data[i], &right_src_data[i], &dst_data[i]);

                if (rows_remaining != 0)
                {
                    LeftType left_src_remaining[Impl::rows_per_iteration];
                    memcpy(left_src_remaining, &left_src_data[rows_size], rows_remaining * sizeof(LeftType));
                    memset(
                        left_src_remaining + rows_remaining,
                        0,
                        (Impl::rows_per_iteration - rows_remaining) * sizeof(LeftType));
                    RightType right_src_remaining[Impl::rows_per_iteration];
                    memcpy(right_src_remaining, &right_src_data[rows_size], rows_remaining * sizeof(RightType));
                    memset(
                        right_src_remaining + rows_remaining,
                        0,
                        (Impl::rows_per_iteration - rows_remaining) * sizeof(RightType));
                    Float64 dst_remaining[Impl::rows_per_iteration];

                    Impl::execute(left_src_remaining, right_src_remaining, dst_remaining);

                    memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
                }

                block.getByPosition(result).column = std::move(dst);
            }
            return true;
        }
        else if (const auto right_arg_typed = checkAndGetColumnConst<ColumnVector<RightType>>(right_arg))
        {
            auto dst = ColumnVector<Float64>::create();

            const auto & left_src_data = left_arg->getData();
            RightType right_src_data[Impl::rows_per_iteration];
            std::fill(
                std::begin(right_src_data),
                std::end(right_src_data),
                right_arg_typed->template getValue<RightType>());
            const auto src_size = left_src_data.size();
            auto & dst_data = dst->getData();
            dst_data.resize(src_size);

            const auto rows_remaining = src_size % Impl::rows_per_iteration;
            const auto rows_size = src_size - rows_remaining;

            if constexpr (Nullable)
            {
                auto null_map = ColumnUInt8::create();
                auto & null_map_data = null_map->getData();
                null_map_data.resize(src_size);

                for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                    Impl::execute(&left_src_data[i], right_src_data, &dst_data[i], &null_map_data[i]);

                if (rows_remaining != 0)
                {
                    LeftType left_src_remaining[Impl::rows_per_iteration];
                    memcpy(left_src_remaining, &left_src_data[rows_size], rows_remaining * sizeof(LeftType));
                    memset(
                        left_src_remaining + rows_remaining,
                        0,
                        (Impl::rows_per_iteration - rows_remaining) * sizeof(LeftType));
                    Float64 dst_remaining[Impl::rows_per_iteration];
                    UInt8 null_map_remaining[Impl::rows_per_iteration];

                    Impl::execute(left_src_remaining, right_src_data, dst_remaining, null_map_remaining);

                    memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
                    memcpy(&null_map_data[rows_size], null_map_remaining, rows_remaining * sizeof(UInt8));
                }

                block.getByPosition(result).column = ColumnNullable::create(std::move(dst), std::move(null_map));
            }
            else
            {
                for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                    Impl::execute(&left_src_data[i], right_src_data, &dst_data[i]);

                if (rows_remaining != 0)
                {
                    LeftType left_src_remaining[Impl::rows_per_iteration];
                    memcpy(left_src_remaining, &left_src_data[rows_size], rows_remaining * sizeof(LeftType));
                    memset(
                        left_src_remaining + rows_remaining,
                        0,
                        (Impl::rows_per_iteration - rows_remaining) * sizeof(LeftType));
                    Float64 dst_remaining[Impl::rows_per_iteration];

                    Impl::execute(left_src_remaining, right_src_data, dst_remaining);

                    memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
                }

                block.getByPosition(result).column = std::move(dst);
            }
            return true;
        }

        return false;
    }

    template <typename LeftType>
    bool executeLeft(Block & block, const ColumnNumbers & arguments, const size_t result, const IColumn * left_arg)
        const
    {
        if (const auto left_arg_typed = checkAndGetColumn<ColumnVector<LeftType>>(left_arg))
        {
            const auto * right_arg = block.getByPosition(arguments[1]).column.get();

            if (executeRight<LeftType, UInt8>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, UInt16>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, UInt32>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, UInt64>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, Int8>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, Int16>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, Int32>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, Int64>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, Float32>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, Float64>(block, result, left_arg_typed, right_arg))
            {
                return true;
            }
            else
            {
                throw Exception(
                    fmt::format(
                        "Illegal column {} of second argument of function {}",
                        block.getByPosition(arguments[1]).column->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_COLUMN);
            }
        }
        else if (const auto left_arg_typed = checkAndGetColumnConst<ColumnVector<LeftType>>(left_arg))
        {
            const auto * right_arg = block.getByPosition(arguments[1]).column.get();

            if (executeRight<LeftType, UInt8>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, UInt16>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, UInt32>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, UInt64>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, Int8>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, Int16>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, Int32>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, Int64>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, Float32>(block, result, left_arg_typed, right_arg)
                || executeRight<LeftType, Float64>(block, result, left_arg_typed, right_arg))
            {
                return true;
            }
            else
            {
                throw Exception(
                    fmt::format(
                        "Illegal column {} of second argument of function {}",
                        block.getByPosition(arguments[1]).column->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_COLUMN);
            }
        }

        return false;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        const auto * left_arg = block.getByPosition(arguments[0]).column.get();

        if (!executeLeft<UInt8>(block, arguments, result, left_arg)
            && !executeLeft<UInt16>(block, arguments, result, left_arg)
            && !executeLeft<UInt32>(block, arguments, result, left_arg)
            && !executeLeft<UInt64>(block, arguments, result, left_arg)
            && !executeLeft<Int8>(block, arguments, result, left_arg)
            && !executeLeft<Int16>(block, arguments, result, left_arg)
            && !executeLeft<Int32>(block, arguments, result, left_arg)
            && !executeLeft<Int64>(block, arguments, result, left_arg)
            && !executeLeft<Float32>(block, arguments, result, left_arg)
            && !executeLeft<Float64>(block, arguments, result, left_arg))
        {
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", left_arg->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};

template <typename Impl>
using FunctionMathBinaryFloat64Nullable = FunctionMathBinaryFloat64<Impl, true>;


template <typename Name, bool(Function)(Float64, Float64, Float64 &)>
struct BinaryFunctionNullablePlain
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T1, typename T2>
    static void execute(const T1 * src_left, const T2 * src_right, Float64 * dst, UInt8 * is_null)
    {
        *is_null = Function(static_cast<Float64>(*src_left), static_cast<Float64>(*src_right), *dst);
    }
};


template <typename Name, Float64(Function)(Float64, Float64)>
struct BinaryFunctionPlain
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T1, typename T2>
    static void execute(const T1 * src_left, const T2 * src_right, Float64 * dst)
    {
        dst[0] = static_cast<Float64>(Function(static_cast<Float64>(src_left[0]), static_cast<Float64>(src_right[0])));
    }
};

#if USE_VECTORCLASS

template <typename Name, Vec2d(Function)(const Vec2d &, const Vec2d &)>
struct BinaryFunctionVectorized
{
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 2;

    template <typename T1, typename T2>
    static void execute(const T1 * src_left, const T2 * src_right, Float64 * dst)
    {
        const auto result = Function(Vec2d(src_left[0], src_left[1]), Vec2d(src_right[0], src_right[1]));
        result.store(dst);
    }
};

#else

#define BinaryFunctionVectorized BinaryFunctionPlain

#endif


struct EImpl
{
    static constexpr auto name = "e";
    static const double value; /// See .cpp
};

struct PiImpl
{
    static constexpr auto name = "pi";
    static const double value;
};


bool logNullable(double b, double & result)
{
    if (b <= 0)
        return true;
    result = log(b);
    return false;
}

bool log2Nullable(double b, double & result)
{
    if (b <= 0)
        return true;
    result = log2(b);
    return false;
}

bool log10Nullable(double b, double & result)
{
    if (b <= 0)
        return true;
    result = log10(b);
    return false;
}

bool log2args(double b, double e, double & result)
{
    if (b == 1 || b <= 0 || e <= 0)
    {
        return true;
    }
    result = log(e) / log(b);
    return false;
}

bool sqrtNullable(double x, double & result)
{
    if (x < 0)
    {
        return true;
    }
    result = sqrt(x);
    return false;
}

double sign(double x)
{
    return (x > 0) - (x < 0);
}


struct RadiansName
{
    static constexpr auto name = "radians";
};
struct DegreesName
{
    static constexpr auto name = "degrees";
};
struct SignName
{
    static constexpr auto name = "sign";
};
struct ExpName
{
    static constexpr auto name = "exp";
};
struct LogName
{
    static constexpr auto name = "log";
};
struct Log2ArgsName
{
    static constexpr auto name = "log2args";
};
struct Exp2Name
{
    static constexpr auto name = "exp2";
};
struct Log2Name
{
    static constexpr auto name = "log2";
};
struct Exp10Name
{
    static constexpr auto name = "exp10";
};
struct Log10Name
{
    static constexpr auto name = "log10";
};
struct SqrtName
{
    static constexpr auto name = "sqrt";
};
struct CbrtName
{
    static constexpr auto name = "cbrt";
};
struct SinName
{
    static constexpr auto name = "sin";
};
struct CosName
{
    static constexpr auto name = "cos";
};
struct TanName
{
    static constexpr auto name = "tan";
};
struct AsinName
{
    static constexpr auto name = "asin";
};
struct AcosName
{
    static constexpr auto name = "acos";
};
struct AtanName
{
    static constexpr auto name = "atan";
};
struct ErfName
{
    static constexpr auto name = "erf";
};
struct ErfcName
{
    static constexpr auto name = "erfc";
};
struct LGammaName
{
    static constexpr auto name = "lgamma";
};
struct TGammaName
{
    static constexpr auto name = "tgamma";
};
struct PowName
{
    static constexpr auto name = "pow";
};

using FunctionRadians = FunctionMathUnaryFloat64<UnaryFunctionPlain<RadiansName, DB::degToRad>>;
using FunctionDegrees = FunctionMathUnaryFloat64<UnaryFunctionPlain<DegreesName, DB::radToDeg>>;
using FunctionSign = FunctionMathUnaryFloat64<UnaryFunctionPlain<SignName, DB::sign>>;
using FunctionE = FunctionMathNullaryConstFloat64<EImpl>;
using FunctionPi = FunctionMathNullaryConstFloat64<PiImpl>;
using FunctionExp = FunctionMathUnaryFloat64<UnaryFunctionVectorized<ExpName, exp>>;
using FunctionLog = FunctionMathUnaryFloat64Nullable<UnaryFunctionNullableVectorized<LogName, DB::logNullable>>;
using FunctionLog2Args = FunctionMathBinaryFloat64Nullable<BinaryFunctionNullablePlain<Log2ArgsName, DB::log2args>>;
using FunctionExp2 = FunctionMathUnaryFloat64<UnaryFunctionVectorized<Exp2Name, exp2>>;
using FunctionLog2 = FunctionMathUnaryFloat64Nullable<UnaryFunctionNullableVectorized<Log2Name, DB::log2Nullable>>;
using FunctionExp10 = FunctionMathUnaryFloat64<UnaryFunctionVectorized<Exp10Name, preciseExp10>>;
using FunctionLog10 = FunctionMathUnaryFloat64Nullable<UnaryFunctionNullableVectorized<Log10Name, DB::log10Nullable>>;
using FunctionSqrt = FunctionMathUnaryFloat64Nullable<UnaryFunctionNullableVectorized<SqrtName, DB::sqrtNullable>>;

using FunctionCbrt = FunctionMathUnaryFloat64<UnaryFunctionVectorized<
    CbrtName,
#if USE_VECTORCLASS
    Power_rational<1, 3>::pow
#else
    cbrt
#endif
    >>;

using FunctionSin = FunctionMathUnaryFloat64<UnaryFunctionVectorized<SinName, sin>>;
using FunctionCos = FunctionMathUnaryFloat64<UnaryFunctionVectorized<CosName, cos>>;
using FunctionTan = FunctionMathUnaryFloat64<UnaryFunctionVectorized<TanName, tan>>;
using FunctionAsin = FunctionMathUnaryFloat64<UnaryFunctionVectorized<AsinName, asin>>;
using FunctionAcos = FunctionMathUnaryFloat64<UnaryFunctionVectorized<AcosName, acos>>;
using FunctionAtan = FunctionMathUnaryFloat64<UnaryFunctionVectorized<AtanName, atan>>;
using FunctionErf = FunctionMathUnaryFloat64<UnaryFunctionPlain<ErfName, std::erf>>;
using FunctionErfc = FunctionMathUnaryFloat64<UnaryFunctionPlain<ErfcName, std::erfc>>;
using FunctionLGamma = FunctionMathUnaryFloat64<UnaryFunctionPlain<LGammaName, std::lgamma>>;
using FunctionTGamma = FunctionMathUnaryFloat64<UnaryFunctionPlain<TGammaName, std::tgamma>>;
using FunctionPow = FunctionMathBinaryFloat64<BinaryFunctionVectorized<PowName, pow>>;

} // namespace DB
