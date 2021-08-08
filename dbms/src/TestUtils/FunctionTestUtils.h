#pragma once

#include <Common/UnifiedLogPatternFormatter.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/core.h>

#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#else
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-compare"
#endif

#include <gtest/gtest.h>

#if !__clang__
#pragma GCC diagnostic pop
#else
#pragma clang diagnostic pop
#endif

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
    using FieldType = typename NearestFieldType<T>::Type;
};

template <typename T>
struct NullableTraits<Nullable<T>>
{
    static constexpr bool is_nullable = true;
    using FieldType = std::optional<typename NearestFieldType<T>::Type>;
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

template <typename T, typename... Args>
DataTypePtr makeDataType(Args &&... args)
{
    if constexpr (NullableTraits<T>::is_nullable)
        return makeNullable(makeDataType<typename T::NativeType, Args...>(std::forward<Args...>(args)...));
    else
        return std::make_shared<typename InferredDataType<T>::Type>(std::forward<Args...>(args)...);
}

template <typename T>
Field makeField(T && value)
{
    return Field(std::move(value));
}

template <typename T>
Field makeField(std::optional<T> && value)
{
    if (value.has_value())
        return Field(std::move(value.value()));
    else
        return Null();
}

template <typename T>
ColumnPtr makeColumn(const DataTypePtr & data_type, InferredDataVector<T> && vec)
{
    auto column = data_type->createColumn();

    for (auto && data : vec)
        column->insert(makeField(std::move(data)));

    return column;
}

template <typename T>
ColumnPtr makeConstColumn(const DataTypePtr & data_type, size_t size, T && value)
{
    return data_type->createColumnConst(size, makeField(std::move(value)));
}

template <typename T>
ColumnWithTypeAndName createColumn(InferredDataVector<T> && vec, String name = "")
{
    DataTypePtr data_type = makeDataType<T>();
    return {makeColumn<T>(data_type, std::move(vec)), std::move(data_type), std::move(name)};
}

template <typename T>
ColumnWithTypeAndName createColumn(InferredDataInitializerList<T> init)
{
    auto vec = InferredDataVector<T>(init);
    return createColumn<T>(std::move(vec), "");
}

template <typename T>
ColumnWithTypeAndName createConstColumn(size_t size, T && value, String name = "")
{
    DataTypePtr data_type = makeDataType<T>();
    return {makeConstColumn(data_type, size, std::move(value)), std::move(data_type), std::move(name)};
}

template <typename T, typename ... Args>
ColumnWithTypeAndName createColumn(std::tuple<Args...> && data_type_args, InferredDataVector<T> && vec, const String & name = "")
{
    DataTypePtr data_type = std::apply(makeDataType<T, Args...>, std::move(data_type_args));
    return {makeColumn<T>(data_type, vec), data_type, name};
}

template <typename T, typename ... Args>
ColumnWithTypeAndName createColumn(std::tuple<Args...> && data_type_args, InferredDataInitializerList<T> init, const String & name = "")
{
    auto vec = InferredDataVector<T>(init);
    return createColumn<T>(data_type_args, vec, name);
}

template <typename T, typename ... Args>
ColumnWithTypeAndName createConstColumn(std::tuple<Args...> && data_type_args, size_t size, T && value, String name = "")
{
    DataTypePtr data_type = std::apply(makeDataType<T, Args...>, std::move(data_type_args));
    return {makeConstColumn(data_type, size, std::move(value)), std::move(data_type), std::move(name)};
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

#define ASSERT_COLUMN_EQ(expected, actual) ASSERT_TRUE(DB::tests::columnEqual((expected), (actual)))

} // namespace tests
} // DB

