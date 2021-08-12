#pragma once

#include <Common/UnifiedLogPatternFormatter.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{

template <typename T>
struct Nullable
{
    using NativeType = T;
};

template <typename T>
struct NullableTraits
{
    static constexpr bool is_nullable = false;
    static constexpr bool is_decimal = false;
    using FieldType = typename NearestFieldType<T>::Type;
};

template <typename T>
struct NullableTraits<Nullable<T>>
{
    static constexpr bool is_nullable = true;
    static constexpr bool is_decimal = false;
    using FieldType = std::optional<typename NearestFieldType<T>::Type>;
};

template <typename T>
struct NullableTraits<Decimal<T>>
{
    static constexpr bool is_nullable = false;
    static constexpr bool is_decimal = true;
    using DecimalType = Decimal<T>;
    using FieldType = DecimalField<Decimal<T>>;
};

template <typename T>
struct NullableTraits<Nullable<Decimal<T>>>
{
    static constexpr bool is_nullable = true;
    static constexpr bool is_decimal = true;
    using DecimalType = Decimal<T>;
    using FieldType = std::optional<DecimalField<Decimal<T>>>;;
};

template <typename T>
struct InferredDataType;

template <>
struct InferredDataType<UInt8>
{
    using Type = DataTypeUInt8;
};

template <>
struct InferredDataType<UInt16>
{
    using Type = DataTypeUInt16;
};

template <>
struct InferredDataType<UInt32>
{
    using Type = DataTypeUInt32;
};

template <>
struct InferredDataType<UInt64>
{
    using Type = DataTypeUInt64;
};

template <>
struct InferredDataType<Int8>
{
    using Type = DataTypeInt8;
};

template <>
struct InferredDataType<Int16>
{
    using Type = DataTypeInt16;
};

template <>
struct InferredDataType<Int32>
{
    using Type = DataTypeInt32;
};

template <>
struct InferredDataType<Int64>
{
    using Type = DataTypeInt64;
};

template <>
struct InferredDataType<Float32>
{
    using Type = DataTypeFloat32;
};

template <>
struct InferredDataType<Float64>
{
    using Type = DataTypeFloat64;
};

template <>
struct InferredDataType<String>
{
    using Type = DataTypeString;
};

template <typename T>
struct InferredDataType<Decimal<T>>
{
    using Type = DataTypeDecimal<Decimal<T>>;
};

template <typename T>
using InferredFieldType = typename NullableTraits<T>::FieldType;

template <typename T>
using InferredDataVector = std::vector<InferredFieldType<T>>;

template <typename T>
using InferredDataInitializerList = std::initializer_list<InferredFieldType<T>>;

template <typename T>
using InferredLiteralType = std::conditional_t<NullableTraits<T>::is_nullable, std::optional<String>, String>;

template <typename T>
using InferredLiteralVector = std::vector<InferredLiteralType<T>>;

template <typename T, typename... Args>
DataTypePtr makeDataType(const Args &... args)
{
    if constexpr (NullableTraits<T>::is_nullable)
        return makeNullable(makeDataType<typename T::NativeType, Args...>(args...));
    else
        return std::make_shared<typename InferredDataType<T>::Type>(args...);
}

template <typename T, typename... Args>
std::tuple<DataTypePtr, PrecType, ScaleType> makeDecimalDataType(const Args &... args)
{
    DataTypePtr data_type = makeDataType<T, Args...>(args...);
    PrecType prec = getDecimalPrecision(*removeNullable(data_type), 0);
    ScaleType scale = getDecimalScale(*removeNullable(data_type), 0);

    return std::make_tuple(data_type, prec, scale);
}

template <typename T>
Field makeField(const T & value)
{
    return Field(value);
}

template <typename T>
Field makeField(const std::optional<T> & value)
{
    if (value.has_value())
        return Field(value.value());
    else
        return Null();
}

template <typename T>
ColumnPtr makeColumn(const DataTypePtr & data_type, const InferredDataVector<T> & vec)
{
    auto column = data_type->createColumn();

    for (const auto & data : vec)
        column->insert(makeField(data));

    return column;
}

template <typename T>
ColumnPtr makeConstColumn(const DataTypePtr & data_type, size_t size, const InferredFieldType<T> & value)
{
    return data_type->createColumnConst(size, makeField(value));
}

template <typename T>
ColumnWithTypeAndName createColumn(const InferredDataVector<T> & vec, String name = "")
{
    DataTypePtr data_type = makeDataType<T>();
    return {makeColumn<T>(data_type, vec), data_type, name};
}

template <typename T>
ColumnWithTypeAndName createColumn(InferredDataInitializerList<T> init, const String & name = "")
{
    auto vec = InferredDataVector<T>(init);
    return createColumn<T>(vec, name);
}

template <typename T>
ColumnWithTypeAndName createConstColumn(size_t size, const InferredFieldType<T> & value, const String & name = "")
{
    DataTypePtr data_type = makeDataType<T>();
    return {makeConstColumn<T>(data_type, size, value), data_type, name};
}

template <typename T, typename ... Args>
ColumnWithTypeAndName createColumn(const std::tuple<Args...> & data_type_args, const InferredDataVector<T> & vec, const String & name = "")
{
    DataTypePtr data_type = std::apply(makeDataType<T, Args...>, data_type_args);
    return {makeColumn<T>(data_type, vec), data_type, name};
}

template <typename T, typename ... Args>
ColumnWithTypeAndName createColumn(const std::tuple<Args...> & data_type_args, InferredDataInitializerList<T> init, const String & name = "")
{
    auto vec = InferredDataVector<T>(init);
    return createColumn<T>(data_type_args, vec, name);
}

template <typename T, typename ... Args>
ColumnWithTypeAndName createConstColumn(const std::tuple<Args...> & data_type_args, size_t size, const InferredFieldType<T> & value, const String & name = "")
{
    DataTypePtr data_type = std::apply(makeDataType<T, Args...>, data_type_args);
    return {makeConstColumn<T>(data_type, size, value), data_type, name};
}

// parse a string into decimal field.
//
// examples:
// - "123.123" -> Decimal(6, 3)
// - " 123.123" -> Error
// - "+.123" -> Decimal(3, 3)
// - "-0.123" -> Decimal(4, 3)
// - "" -> Decimal(0, 0)
// - "." -> Error
// - "0" -> Decimal(1, 0)
// - "0''''0" -> Decimal(2, 0)
// - "0." -> Error
// - "0.0" -> Decimal(2, 1)
// - "000'000.000'000" -> Decimal(12, 6)
template <typename T>
typename NullableTraits<T>::FieldType parseDecimal(const InferredLiteralType<T> & literal_,
    PrecType max_prec = std::numeric_limits<PrecType>::max(),
    ScaleType expected_scale = std::numeric_limits<ScaleType>::max())
{
    using Traits = NullableTraits<T>;
    using DecimalType = typename Traits::DecimalType;
    using NativeType = typename DecimalType::NativeType;
    static_assert(is_signed_v<NativeType>);

    if constexpr (Traits::is_nullable)
    {
        if (!literal_.has_value())
            return std::nullopt;
    }

    const String & literal = [&] {
        if constexpr (Traits::is_nullable)
        {
            assert(literal_.has_value());
            return literal_.value();
        }
        else
            return literal_;
    }();

    size_t pos = 0;
    bool negative = false;

    if (literal.size() > 0)
    {
        if (literal[pos] == '-')
        {
            negative = true;
            ++pos;
        }
        else if (literal[pos] == '+')
        {
            //  ignore plus sign. e.g. "+10000" = "10000".
            ++pos;
        }
    }

    bool has_dot = false;
    PrecType prec = 0;
    ScaleType scale = 0;
    NativeType value = 0;

    for (; pos < literal.size(); ++pos)
    {
        char c = literal[pos];

        switch (c)
        {
            case '\'':
                // use "'" as separator. e.g. 1'000'000'000.
                continue;

            case '.':
                if (has_dot)
                    throw TiFlashTestException("At most one decimal point is allowed");

                has_dot = true;
                break;

            default:
                if (!isNumericASCII(c))
                    throw TiFlashTestException(fmt::format("Expect decimal digit, got \"{}\"", c));

                ++prec;
                if (prec > maxDecimalPrecision<DecimalType>())
                    throw TiFlashTestException(fmt::format("{} overflow", TypeName<DecimalType>::get()));
                if (has_dot)
                    ++scale;

                value = value * 10 + (c - '0');
        }
    }

    if (negative)
        value = -value;

    // "9." is not allowed. It should be explicitly written as "9.0".
    if (has_dot && scale == 0)
        throw TiFlashTestException("No digit after decimal point. Is it missing?");
    if (prec > max_prec)
        throw TiFlashTestException(fmt::format("Precision is too large: max = {}, actual = {}", max_prec, prec));
    if (expected_scale != std::numeric_limits<ScaleType>::max() && scale != expected_scale)
        throw TiFlashTestException(fmt::format("Scale does not match: expected = {}, actual = {}", expected_scale, scale));

    return DecimalField<DecimalType>(value, scale);
}

// e.g. `createColumn<Decimal32>(std::make_tuple(9, 4), {"99999.9999"})`
template <typename T, typename... Args>
ColumnWithTypeAndName createColumn(const std::tuple<Args...> & data_type_args, const InferredLiteralVector<T> & literals,
    const String & name = "", std::enable_if_t<NullableTraits<T>::is_decimal, int> = 0)
{
    auto [data_type, prec, scale] = std::apply(makeDecimalDataType<T, Args...>, data_type_args);

    InferredDataVector<T> vec;
    vec.reserve(literals.size());
    for (const auto & literal : literals)
        vec.push_back(parseDecimal<T>(literal, prec, scale));

    return {makeColumn<T>(data_type, vec), data_type, name};
}

// e.g. `createConstColumn<Decimal32>(std::make_tuple(9, 4), 1, "99999.9999")`
template <typename T, typename... Args>
ColumnWithTypeAndName createConstColumn(const std::tuple<Args...> & data_type_args, size_t size, const InferredLiteralType<T> & literal,
    const String & name = "", std::enable_if_t<NullableTraits<T>::is_decimal, int> = 0)
{
    auto [data_type, prec, scale] = std::apply(makeDecimalDataType<T, Args...>, data_type_args);
    return {makeConstColumn<T>(data_type, size, parseDecimal<T>(literal, prec, scale)), data_type, name};
}

::testing::AssertionResult dataTypeEqual(
    const DataTypePtr & expected,
    const DataTypePtr & actual);

::testing::AssertionResult columnEqual(
    const ColumnPtr & expected,
    const ColumnPtr & actual);

// ignore name
::testing::AssertionResult columnEqual(
    const ColumnWithTypeAndName & expected,
    const ColumnWithTypeAndName & actual);

ColumnWithTypeAndName executeFunction(const String & func_name, const ColumnsWithTypeAndName & columns);

template <typename... Args>
ColumnWithTypeAndName executeFunction(const String & func_name, const ColumnWithTypeAndName & first_column, const Args & ... columns)
{
    ColumnsWithTypeAndName vec({first_column, columns...});
    return executeFunction(func_name, vec);
}

#define ASSERT_COLUMN_EQ(expected, actual) ASSERT_TRUE(DB::tests::columnEqual((expected), (actual)))

} // namespace tests
} // DB
