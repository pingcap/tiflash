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

#include <Common/MyDuration.h>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeMyDuration.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Functions/registerFunctions.h>
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
struct TypeTraits
{
    static constexpr bool is_nullable = false;
    static constexpr bool is_decimal = false;
    using FieldType = typename NearestFieldType<T>::Type;
};

template <>
struct TypeTraits<Null>
{
    static constexpr bool is_nullable = false;
    static constexpr bool is_decimal = false;
    using FieldType = Null;
};

template <>
struct TypeTraits<MyDate>
{
    static constexpr bool is_nullable = false;
    static constexpr bool is_decimal = false;
    using FieldType = DataTypeMyDate::FieldType;
};

template <>
struct TypeTraits<MyDateTime>
{
    static constexpr bool is_nullable = false;
    static constexpr bool is_decimal = false;
    using FieldType = DataTypeMyDateTime::FieldType;
};

template <>
struct TypeTraits<MyDuration>
{
    static constexpr bool is_nullable = false;
    static constexpr bool is_decimal = false;
    using FieldType = DataTypeMyDuration::FieldType;
};

template <typename T>
struct TypeTraits<Nullable<T>>
{
    static constexpr bool is_nullable = true;
    static constexpr bool is_decimal = false;
    using FieldType = std::optional<typename TypeTraits<T>::FieldType>;
};

template <typename T>
struct TypeTraits<Decimal<T>>
{
    static constexpr bool is_nullable = false;
    static constexpr bool is_decimal = true;
    using DecimalType = Decimal<T>;
    using FieldType = DecimalField<DecimalType>;
};

template <typename T>
struct TypeTraits<Nullable<Decimal<T>>>
{
    static constexpr bool is_nullable = true;
    static constexpr bool is_decimal = true;
    using DecimalType = Decimal<T>;
    using FieldType = std::optional<DecimalField<DecimalType>>;
};

template <typename T>
struct InferredDataType;

template <>
struct InferredDataType<Null>
{
    using Type = DataTypeNothing;
};

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

template <>
struct InferredDataType<MyDate>
{
    using Type = DataTypeMyDate;
};

template <>
struct InferredDataType<MyDateTime>
{
    using Type = DataTypeMyDateTime;
};

template <>
struct InferredDataType<MyDuration>
{
    using Type = DataTypeMyDuration;
};

template <typename T>
struct InferredDataType<Decimal<T>>
{
    using Type = DataTypeDecimal<Decimal<T>>;
};

template <typename T>
using InferredFieldType = typename TypeTraits<T>::FieldType;

template <typename T>
using InferredDataVector = std::vector<InferredFieldType<T>>;

template <typename T>
using InferredDataInitializerList = std::initializer_list<InferredFieldType<T>>;

template <typename T>
using InferredLiteralType = std::conditional_t<TypeTraits<T>::is_nullable, std::optional<String>, String>;

template <typename T>
using InferredLiteralVector = std::vector<InferredLiteralType<T>>;

template <typename T>
using InferredLiteralInitializerList = std::initializer_list<InferredLiteralType<T>>;

template <typename T, typename... Args>
DataTypePtr makeDataType(const Args &... args)
{
    if constexpr (TypeTraits<T>::is_nullable)
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

ColumnWithTypeAndName createOnlyNullColumnConst(size_t size, const String & name = "");
ColumnWithTypeAndName createOnlyNullColumn(size_t size, const String & name = "");

template <typename T>
ColumnWithTypeAndName createColumn(const InferredDataVector<T> & vec, const String & name = "", Int64 column_id = 0)
{
    DataTypePtr data_type = makeDataType<T>();
    return {makeColumn<T>(data_type, vec), data_type, name, column_id};
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

template <typename T, typename... Args>
ColumnWithTypeAndName createColumn(
    const std::tuple<Args...> & data_type_args,
    const InferredDataVector<T> & vec,
    const String & name = "")
{
    DataTypePtr data_type = std::apply(makeDataType<T, Args...>, data_type_args);
    return {makeColumn<T>(data_type, vec), data_type, name};
}

template <typename T, typename... Args>
ColumnWithTypeAndName createColumn(
    const std::tuple<Args...> & data_type_args,
    InferredDataInitializerList<T> init,
    const String & name = "")
{
    auto vec = InferredDataVector<T>(init);
    return createColumn<T>(data_type_args, vec, name);
}

template <typename T, typename... Args>
ColumnWithTypeAndName createConstColumn(
    const std::tuple<Args...> & data_type_args,
    size_t size,
    const InferredFieldType<T> & value,
    const String & name = "")
{
    DataTypePtr data_type = std::apply(makeDataType<T, Args...>, data_type_args);
    return {makeConstColumn<T>(data_type, size, value), data_type, name};
}

template <bool is_nullable = true>
ColumnWithTypeAndName createDateTimeColumn(std::initializer_list<std::optional<MyDateTime>> init, int fraction)
{
    DataTypePtr data_type_ptr = std::make_shared<DataTypeMyDateTime>(fraction);
    if constexpr (is_nullable)
    {
        data_type_ptr = makeNullable(data_type_ptr);
    }
    auto col = data_type_ptr->createColumn();
    for (const auto & dt : init)
    {
        if (dt.has_value())
            col->insert(Field(dt->toPackedUInt()));
        else
        {
            if constexpr (is_nullable)
            {
                col->insert(Null());
            }
            else
            {
                throw Exception("Null value for not nullable DataTypeMyDateTime");
            }
        }
    }
    return {std::move(col), data_type_ptr, "datetime"};
}

template <bool is_nullable = true>
ColumnWithTypeAndName createDateTimeColumnConst(size_t size, const std::optional<MyDateTime> & dt, int fraction)
{
    DataTypePtr data_type_ptr = std::make_shared<DataTypeMyDateTime>(fraction);
    if constexpr (is_nullable)
    {
        data_type_ptr = makeNullable(data_type_ptr);
    }

    ColumnPtr col;
    if (dt.has_value())
        col = data_type_ptr->createColumnConst(size, Field(dt->toPackedUInt()));
    else
    {
        if constexpr (is_nullable)
        {
            col = data_type_ptr->createColumnConst(size, Field(Null()));
        }
        else
        {
            throw Exception("Null value for not nullable DataTypeMyDateTime");
        }
    }
    return {std::move(col), data_type_ptr, "datetime"};
}

template <bool is_nullable = true>
ColumnWithTypeAndName createDurationColumn(std::initializer_list<std::optional<MyDuration>> init, int fraction)
{
    DataTypePtr data_type_ptr = std::make_shared<DataTypeMyDuration>(fraction);
    if constexpr (is_nullable)
    {
        data_type_ptr = makeNullable(data_type_ptr);
    }
    auto col = data_type_ptr->createColumn();
    for (const auto & dt : init)
    {
        if (dt.has_value())
            col->insert(Field(dt->nanoSecond()));
        else
        {
            if constexpr (is_nullable)
            {
                col->insert(Null());
            }
            else
            {
                throw Exception("Null value for not nullable DataTypeMyDuration");
            }
        }
    }
    return {std::move(col), data_type_ptr, "duration"};
}

template <bool is_nullable = true>
ColumnWithTypeAndName createDurationColumnConst(size_t size, const std::optional<MyDuration> & duration, int fraction)
{
    DataTypePtr data_type_ptr = std::make_shared<DataTypeMyDuration>(fraction);
    if constexpr (is_nullable)
    {
        data_type_ptr = makeNullable(data_type_ptr);
    }

    ColumnPtr col;
    if (duration.has_value())
        col = data_type_ptr->createColumnConst(size, Field(duration->nanoSecond()));
    else
    {
        if constexpr (is_nullable)
        {
            col = data_type_ptr->createColumnConst(size, Field(Null()));
        }
        else
        {
            throw Exception("Null value for not nullable DataTypeMyDuration");
        }
    }
    return {std::move(col), data_type_ptr, "duration"};
}

// parse a string into decimal field.
template <typename T>
typename TypeTraits<T>::FieldType parseDecimal(
    const InferredLiteralType<T> & literal_,
    PrecType max_prec,
    ScaleType expected_scale)
{
    using Traits = TypeTraits<T>;
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
            RUNTIME_ASSERT(literal_.has_value());
            return literal_.value();
        }
        else
            return literal_;
    }();

    auto parsed = DB::parseDecimal(literal.data(), literal.size());
    if (!parsed.has_value())
        throw TiFlashTestException(fmt::format("Failed to parse '{}'", literal));

    auto [parsed_value, prec, scale] = parsed.value();

    (void)prec;
    max_prec = std::min(max_prec, maxDecimalPrecision<DecimalType>());
    if (scale != expected_scale)
    {
        Int256 scale_factor = 1;
        size_t scale_diff = expected_scale > scale ? expected_scale - scale : scale - expected_scale;
        for (size_t i = 0; i < scale_diff; i++)
            scale_factor *= 10;
        if (expected_scale > scale)
        {
            parsed_value *= scale_factor;
        }
        else
        {
            bool need_round = (parsed_value >= 0 ? parsed_value : -parsed_value) % scale_factor >= scale_factor / 2;
            parsed_value /= scale_factor;
            if (need_round)
            {
                if (parsed_value > 0)
                    parsed_value++;
                else
                    parsed_value--;
            }
        }
    }
    auto max_value = DecimalMaxValue::get(max_prec);
    if (parsed_value > max_value || parsed_value < -max_value)
        throw TiFlashTestException(
            fmt::format("Input {} overflow for decimal({},{})", literal, max_prec, expected_scale));
    auto value = static_cast<NativeType>(parsed_value);
    return DecimalField<DecimalType>(value, expected_scale);
}

// e.g. `createColumn<Decimal32>(std::make_tuple(9, 4), {"99999.9999"})`
template <typename T, typename... Args>
ColumnWithTypeAndName createColumn(
    const std::tuple<Args...> & data_type_args,
    const InferredLiteralVector<T> & literals,
    const String & name = "",
    std::enable_if_t<TypeTraits<T>::is_decimal, int> = 0)
{
    DataTypePtr data_type = std::apply(makeDataType<T, Args...>, data_type_args);
    PrecType prec = getDecimalPrecision(*removeNullable(data_type), 0);
    ScaleType scale = getDecimalScale(*removeNullable(data_type), 0);

    InferredDataVector<T> vec;
    vec.reserve(literals.size());
    for (const auto & literal : literals)
        vec.push_back(parseDecimal<T>(literal, prec, scale));

    return {makeColumn<T>(data_type, vec), data_type, name};
}

template <typename T, typename... Args>
ColumnWithTypeAndName createColumn(
    const std::tuple<Args...> & data_type_args,
    InferredLiteralInitializerList<T> literals,
    const String & name = "",
    std::enable_if_t<TypeTraits<T>::is_decimal, int> = 0)
{
    auto vec = InferredLiteralVector<T>(literals);
    return createColumn<T, Args...>(data_type_args, vec, name);
}

// e.g. `createConstColumn<Decimal32>(std::make_tuple(9, 4), 1, "99999.9999")`
template <typename T, typename... Args>
ColumnWithTypeAndName createConstColumn(
    const std::tuple<Args...> & data_type_args,
    size_t size,
    const InferredLiteralType<T> & literal,
    const String & name = "",
    std::enable_if_t<TypeTraits<T>::is_decimal, int> = 0)
{
    DataTypePtr data_type = std::apply(makeDataType<T, Args...>, data_type_args);
    PrecType prec = getDecimalPrecision(*removeNullable(data_type), 0);
    ScaleType scale = getDecimalScale(*removeNullable(data_type), 0);

    return {makeConstColumn<T>(data_type, size, parseDecimal<T>(literal, prec, scale)), data_type, name};
}

// resolve ambiguous overloads for `createColumn<Nullable<Decimal>>(..., {std::nullopt})`.
template <typename T, typename... Args>
ColumnWithTypeAndName createColumn(
    const std::tuple<Args...> & data_type_args,
    std::initializer_list<std::nullopt_t> init,
    const String & name = "",
    std::enable_if_t<TypeTraits<T>::is_nullable, int> = 0)
{
    InferredDataVector<T> vec(init.size(), std::nullopt);
    return createColumn<T>(data_type_args, vec, name);
}

// resolve ambiguous overloads for `createConstColumn<Nullable<Decimal>>(..., std::nullopt)`.
template <typename T, typename... Args>
ColumnWithTypeAndName createConstColumn(
    const std::tuple<Args...> & data_type_args,
    size_t size,
    std::nullopt_t,
    const String & name = "",
    std::enable_if_t<TypeTraits<T>::is_nullable, int> = 0)
{
    return createConstColumn<T>(data_type_args, size, InferredFieldType<T>(std::nullopt), name);
}

String getColumnsContent(const ColumnsWithTypeAndName & cols);

/// We can designate the range of columns printed with begin and end. range: [begin, end)
String getColumnsContent(const ColumnsWithTypeAndName & cols, size_t begin, size_t end);

// This wrapper function only serves to construct columns input for function-like macros,
// since preprocessor recognizes `{col1, col2, col3}` as three arguments instead of one.
// E.g. preprocessor does not allow us to write `ASSERT_COLUMNS_EQ_R({col1, col2, col3}, actual_cols)`,
//  but with this func we can write `ASSERT_COLUMNS_EQ_R(createColumns{col1, col2, col3}, actual_cols)` instead.
ColumnsWithTypeAndName createColumns(const ColumnsWithTypeAndName & cols);

::testing::AssertionResult dataTypeEqual(const DataTypePtr & expected, const DataTypePtr & actual);

::testing::AssertionResult columnEqual(
    const ColumnPtr & expected,
    const ColumnPtr & actual,
    const ICollator * collator = nullptr,
    bool is_floating_point = false);

// ignore name
::testing::AssertionResult columnEqual(
    const ColumnWithTypeAndName & expected,
    const ColumnWithTypeAndName & actual,
    const ICollator * collator = nullptr);

::testing::AssertionResult blockEqual(const Block & expected, const Block & actual);

::testing::AssertionResult columnsEqual(
    const ColumnsWithTypeAndName & expected,
    const ColumnsWithTypeAndName & actual,
    bool _restrict);

/// Note that during execution, the offsets of each column might be reordered.
ColumnWithTypeAndName executeFunction(
    Context & context,
    const String & func_name,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    const String & val = "",
    bool raw_function_test = false);

ColumnWithTypeAndName executeFunction(
    Context & context,
    const String & func_name,
    const ColumnNumbers & argument_column_numbers,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    const String & val = "",
    bool raw_function_test = false);

template <typename... Args>
ColumnWithTypeAndName executeFunction(
    Context & context,
    const String & func_name,
    const ColumnWithTypeAndName & first_column,
    const Args &... columns)
{
    ColumnsWithTypeAndName vec({first_column, columns...});
    return executeFunction(context, func_name, vec);
}

DataTypePtr getReturnTypeForFunction(
    Context & context,
    const String & func_name,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    bool raw_function_test = false);

template <typename T>
ColumnWithTypeAndName createNullableColumn(
    InferredDataVector<T> init_vec,
    const std::vector<Int32> & null_map,
    const String name = "",
    Int64 column_id = 0)
{
    static_assert(TypeTraits<T>::is_nullable == false);
    auto updated_vec = InferredDataVector<Nullable<T>>();
    size_t vec_length = std::min(init_vec.size(), null_map.size());
    for (size_t i = 0; i < vec_length; i++)
    {
        if (null_map[i])
            updated_vec.push_back({});
        else
            updated_vec.push_back(init_vec[i]);
    }
    return createColumn<Nullable<T>>(updated_vec, name, column_id);
}

template <typename T>
ColumnWithTypeAndName createNullableColumn(
    InferredDataInitializerList<T> init,
    const std::vector<Int32> & null_map,
    const String name = "",
    Int64 column_id = 0)
{
    static_assert(TypeTraits<T>::is_nullable == false);
    auto vec = InferredDataVector<T>(init);
    return createNullableColumn<T>(vec, null_map, name, column_id);
}

template <typename T, typename... Args>
ColumnWithTypeAndName createNullableColumn(
    const std::tuple<Args...> & data_type_args,
    const InferredDataVector<T> & init_vec,
    const std::vector<Int32> & null_map,
    const String & name = "")
{
    static_assert(TypeTraits<T>::is_nullable == false);
    auto updated_vec = InferredDataVector<Nullable<T>>();
    size_t vec_length = std::min(init_vec.size(), null_map.size());
    for (size_t i = 0; i < vec_length; i++)
    {
        if (null_map[i])
            updated_vec.push_back({});
        else
            updated_vec.push_back(init_vec[i]);
    }
    return createColumn<Nullable<T>>(data_type_args, updated_vec, name);
}

template <typename T, typename... Args>
ColumnWithTypeAndName createNullableColumn(
    const std::tuple<Args...> & data_type_args,
    InferredDataInitializerList<T> init,
    const std::vector<Int32> & null_map,
    const String & name = "")
{
    static_assert(TypeTraits<T>::is_nullable == false);
    auto vec = InferredDataVector<T>(init);
    return createNullableColumn<T>(data_type_args, vec, null_map, name);
}

template <typename T, typename... Args>
ColumnWithTypeAndName createNullableColumn(
    const std::tuple<Args...> & data_type_args,
    const InferredLiteralVector<T> & init_vec,
    const std::vector<Int32> & null_map,
    const String & name = "",
    std::enable_if_t<TypeTraits<T>::is_decimal, int> = 0)
{
    static_assert(TypeTraits<T>::is_nullable == false);
    auto updated_vec = InferredLiteralVector<Nullable<T>>();
    size_t vec_length = std::min(init_vec.size(), null_map.size());
    for (size_t i = 0; i < vec_length; i++)
    {
        if (null_map[i])
            updated_vec.push_back({});
        else
            updated_vec.push_back(init_vec[i]);
    }
    return createColumn<Nullable<T>>(data_type_args, updated_vec, name, 0);
}

template <typename T, typename... Args>
ColumnWithTypeAndName createNullableColumn(
    const std::tuple<Args...> & data_type_args,
    const InferredLiteralInitializerList<T> & init,
    const std::vector<Int32> & null_map,
    const String & name = "",
    std::enable_if_t<TypeTraits<T>::is_decimal, int> = 0)
{
    static_assert(TypeTraits<T>::is_nullable == false);
    auto vec = InferredLiteralVector<T>(init);
    return createNullableColumn<T>(data_type_args, vec, null_map, name, 0);
}

template <typename T>
ColumnWithTypeAndName toNullableVec(const std::vector<std::optional<typename TypeTraits<T>::FieldType>> & v)
{
    return createColumn<Nullable<T>>(v);
}

template <typename T>
ColumnWithTypeAndName toVec(const std::vector<typename TypeTraits<T>::FieldType> & v)
{
    return createColumn<T>(v);
}

template <typename T>
ColumnWithTypeAndName toNullableVec(
    String name,
    const std::vector<std::optional<typename TypeTraits<T>::FieldType>> & v)
{
    return createColumn<Nullable<T>>(v, name);
}

template <typename T>
ColumnWithTypeAndName toVec(String name, const std::vector<typename TypeTraits<T>::FieldType> & v)
{
    return createColumn<T>(v, name);
}

ColumnWithTypeAndName toDatetimeVec(String name, const std::vector<String> & v, int fsp);

ColumnWithTypeAndName toNullableDatetimeVec(String name, const std::vector<String> & v, int fsp);

struct FuncMetaData
{
    String val; // This is for the val field of tipb::expr
};

class FunctionTest : public ::testing::Test
{
protected:
    void SetUp() override { initializeDAGContext(); }

public:
    static void SetUpTestCase()
    {
        try
        {
            DB::registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registered, ignore exception here.
        }
    }

    FunctionTest();

    virtual void initializeDAGContext();

    ColumnWithTypeAndName executeFunction(
        const String & func_name,
        const ColumnsWithTypeAndName & columns,
        const TiDB::TiDBCollatorPtr & collator = nullptr,
        bool raw_function_test = false);

    template <typename... Args>
    ColumnWithTypeAndName executeFunction(
        const String & func_name,
        const ColumnWithTypeAndName & first_column,
        const Args &... columns)
    {
        ColumnsWithTypeAndName vec({first_column, columns...});
        return executeFunction(func_name, vec);
    }

    ColumnWithTypeAndName executeFunction(
        const String & func_name,
        const ColumnNumbers & argument_column_numbers,
        const ColumnsWithTypeAndName & columns,
        const TiDB::TiDBCollatorPtr & collator = nullptr,
        bool raw_function_test = false);

    template <typename... Args>
    ColumnWithTypeAndName executeFunction(
        const String & func_name,
        const ColumnNumbers & argument_column_numbers,
        const ColumnWithTypeAndName & first_column,
        const Args &... columns)
    {
        ColumnsWithTypeAndName vec({first_column, columns...});
        return executeFunction(func_name, argument_column_numbers, vec);
    }

    ColumnWithTypeAndName executeFunctionWithMetaData(
        const String & func_name,
        const ColumnsWithTypeAndName & columns,
        const FuncMetaData & meta,
        const TiDB::TiDBCollatorPtr & collator = nullptr);

    ColumnWithTypeAndName executeFunctionWithMetaData(
        const String & func_name,
        const ColumnNumbers & argument_column_numbers,
        const ColumnsWithTypeAndName & columns,
        const FuncMetaData & meta,
        const TiDB::TiDBCollatorPtr & collator = nullptr);

    DAGContext & getDAGContext()
    {
        RUNTIME_ASSERT(dag_context_ptr != nullptr);
        return *dag_context_ptr;
    }

protected:
    ContextPtr context;
    std::unique_ptr<DAGContext> dag_context_ptr;
};

template <typename...>
struct TestTypeList
{
};

using TestNullableIntTypes = TestTypeList<Nullable<Int8>, Nullable<Int16>, Nullable<Int32>, Nullable<Int64>>;

using TestNullableUIntTypes = TestTypeList<Nullable<UInt8>, Nullable<UInt16>, Nullable<UInt32>, Nullable<UInt64>>;

using TestIntTypes = TestTypeList<Int8, Int16, Int32, Int64>;

using TestUIntTypes = TestTypeList<UInt8, UInt16, UInt32, UInt64>;

using TestAllIntTypes
    = TestTypeList<Nullable<Int8>, Nullable<Int16>, Nullable<Int32>, Nullable<Int64>, Int8, Int16, Int32, Int64>;

using TestAllUIntTypes = TestTypeList<
    Nullable<UInt8>,
    Nullable<UInt16>,
    Nullable<UInt32>,
    Nullable<UInt64>,
    UInt8,
    UInt16,
    UInt32,
    UInt64>;

template <typename T1, typename T2List, template <typename, typename> class Func, typename FuncParam>
struct TestTypeSingle;

template <typename T1, typename T2, typename... T2Rest, template <typename, typename> class Func, typename FuncParam>
struct TestTypeSingle<T1, TestTypeList<T2, T2Rest...>, Func, FuncParam>
{
    static void run(FuncParam & p)
    {
        Func<T1, T2>::operator()(p);
        // Recursively handle the rest of T2List
        TestTypeSingle<T1, TestTypeList<T2Rest...>, Func, FuncParam>::run(p);
    }
};

template <typename T1, template <typename, typename> class Func, typename FuncParam>
struct TestTypeSingle<T1, TestTypeList<>, Func, FuncParam>
{
    static void run(FuncParam &)
    {
        // Do nothing when T2List is empty
    }
};

template <typename T1List, typename T2List, template <typename, typename> class Func, typename FuncParam>
struct TestTypePair;

template <
    typename T1,
    typename... T1Rest,
    typename T2List,
    template <typename, typename>
    class Func,
    typename FuncParam>
struct TestTypePair<TestTypeList<T1, T1Rest...>, T2List, Func, FuncParam>
{
    static void run(FuncParam & p)
    {
        // For the current T1, traverse all types in T2List
        TestTypeSingle<T1, T2List, Func, FuncParam>::run(p);
        // Recursively handle the rest of T1List
        TestTypePair<TestTypeList<T1Rest...>, T2List, Func, FuncParam>::run(p);
    }
};

template <typename T2List, template <typename, typename> class Func, typename FuncParam>
struct TestTypePair<TestTypeList<>, T2List, Func, FuncParam>
{
    static void run(FuncParam &)
    {
        // Do nothing when T1List is empty
    }
};

#define ASSERT_COLUMN_EQ(expected, actual) ASSERT_TRUE(DB::tests::columnEqual((expected), (actual)))
#define ASSERT_BLOCK_EQ(expected, actual) ASSERT_TRUE(DB::tests::blockEqual((expected), (actual)))

/// restrictly checking columns equality, both data set and each row's offset should be the same
#define ASSERT_COLUMNS_EQ_R(expected, actual) ASSERT_TRUE(DB::tests::columnsEqual((expected), (actual), true))
/// unrestrictly checking columns equality, only checking data set equality
#define ASSERT_COLUMNS_EQ_UR(expected, actual) ASSERT_TRUE(DB::tests::columnsEqual((expected), (actual), false))

/// Check the profile event change after the body.
#define ASSERT_PROFILE_EVENT(event, diff_expr, ...)                          \
    do                                                                       \
    {                                                                        \
        auto profile_event_count = ProfileEvents::get(event);                \
        {__VA_ARGS__};                                                       \
        ASSERT_EQ(profile_event_count diff_expr, ProfileEvents::get(event)); \
    } while (false);

} // namespace tests
} // namespace DB
