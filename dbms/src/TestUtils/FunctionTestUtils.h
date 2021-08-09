#pragma once

#include <Common/UnifiedLogPatternFormatter.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
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
    using FieldType = typename NearestFieldType<T>::Type;
};

template <typename T>
struct NullableTraits<Nullable<T>>
{
    static constexpr bool is_nullable = true;
    using FieldType = std::optional<typename NearestFieldType<T>::Type>;
};

template <typename T>
struct NullableTraits<Decimal<T>>
{
    static constexpr bool is_nullable = false;
    using FieldType = DecimalField<Decimal<T>>;
};

template <typename T>
struct NullableTraits<Nullable<Decimal<T>>>
{
    static constexpr bool is_nullable = true;
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

template <typename T, typename... Args>
DataTypePtr makeDataType(const Args &... args)
{
    if constexpr (NullableTraits<T>::is_nullable)
        return makeNullable(makeDataType<typename T::NativeType, Args...>(args...));
    else
        return std::make_shared<typename InferredDataType<T>::Type>(args...);
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

