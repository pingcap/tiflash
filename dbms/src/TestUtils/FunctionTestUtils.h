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

template <typename T>
struct TypeTraits<Nullable<T>>
{
    static constexpr bool is_nullable = true;
    static constexpr bool is_decimal = false;
    using FieldType = std::optional<typename NearestFieldType<T>::Type>;
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

template <typename T, typename... Args>
ColumnWithTypeAndName createColumn(const std::tuple<Args...> & data_type_args, const InferredDataVector<T> & vec, const String & name = "")
{
    DataTypePtr data_type = std::apply(makeDataType<T, Args...>, data_type_args);
    return {makeColumn<T>(data_type, vec), data_type, name};
}

template <typename T, typename... Args>
ColumnWithTypeAndName createColumn(const std::tuple<Args...> & data_type_args, InferredDataInitializerList<T> init, const String & name = "")
{
    auto vec = InferredDataVector<T>(init);
    return createColumn<T>(data_type_args, vec, name);
}

template <typename T, typename... Args>
ColumnWithTypeAndName createConstColumn(const std::tuple<Args...> & data_type_args, size_t size, const InferredFieldType<T> & value, const String & name = "")
{
    DataTypePtr data_type = std::apply(makeDataType<T, Args...>, data_type_args);
    return {makeConstColumn<T>(data_type, size, value), data_type, name};
}

// parse a string into decimal field.
template <typename T>
typename TypeTraits<T>::FieldType parseDecimal(const InferredLiteralType<T> & literal_,
                                               PrecType max_prec = std::numeric_limits<PrecType>::max(),
                                               ScaleType expected_scale = std::numeric_limits<ScaleType>::max())
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
            assert(literal_.has_value());
            return literal_.value();
        }
        else
            return literal_;
    }();

    auto parsed = DB::parseDecimal(literal.data(), literal.size());
    if (!parsed.has_value())
        throw TiFlashTestException(fmt::format("Failed to parse '{}'", literal));

    auto [parsed_value, prec, scale] = parsed.value();

    max_prec = std::min(max_prec, maxDecimalPrecision<DecimalType>());
    if (prec > max_prec)
        throw TiFlashTestException(
            fmt::format("Precision is too large for {}({}): prec = {}", TypeName<DecimalType>::get(), max_prec, prec));
    if (expected_scale != std::numeric_limits<ScaleType>::max() && scale != expected_scale)
        throw TiFlashTestException(fmt::format("Scale does not match: expected = {}, actual = {}", expected_scale, scale));
    if (scale > max_prec)
        throw TiFlashTestException(fmt::format("Scale is larger than max_prec: max_prec = {}, scale = {}", max_prec, scale));

    auto value = static_cast<NativeType>(parsed_value);
    return DecimalField<DecimalType>(value, scale);
}

// e.g. `createColumn<Decimal32>(std::make_tuple(9, 4), {"99999.9999"})`
template <typename T, typename... Args>
ColumnWithTypeAndName createColumn(const std::tuple<Args...> & data_type_args, const InferredLiteralVector<T> & literals, const String & name = "", std::enable_if_t<TypeTraits<T>::is_decimal, int> = 0)
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
ColumnWithTypeAndName createColumn(const std::tuple<Args...> & data_type_args, InferredLiteralInitializerList<T> literals, const String & name = "", std::enable_if_t<TypeTraits<T>::is_decimal, int> = 0)
{
    auto vec = InferredLiteralVector<T>(literals);
    return createColumn<T, Args...>(data_type_args, vec, name);
}

// e.g. `createConstColumn<Decimal32>(std::make_tuple(9, 4), 1, "99999.9999")`
template <typename T, typename... Args>
ColumnWithTypeAndName createConstColumn(const std::tuple<Args...> & data_type_args, size_t size, const InferredLiteralType<T> & literal, const String & name = "", std::enable_if_t<TypeTraits<T>::is_decimal, int> = 0)
{
    DataTypePtr data_type = std::apply(makeDataType<T, Args...>, data_type_args);
    PrecType prec = getDecimalPrecision(*removeNullable(data_type), 0);
    ScaleType scale = getDecimalScale(*removeNullable(data_type), 0);

    return {makeConstColumn<T>(data_type, size, parseDecimal<T>(literal, prec, scale)), data_type, name};
}

// resolve ambiguous overloads for `createColumn<Nullable<Decimal>>(..., {std::nullopt})`.
template <typename T, typename... Args>
ColumnWithTypeAndName createColumn(const std::tuple<Args...> & data_type_args, std::initializer_list<std::nullopt_t> init, const String & name = "", std::enable_if_t<TypeTraits<T>::is_nullable, int> = 0)
{
    InferredDataVector<T> vec(init.size(), std::nullopt);
    return createColumn<T>(data_type_args, vec, name);
}

// resolve ambiguous overloads for `createConstColumn<Nullable<Decimal>>(..., std::nullopt)`.
template <typename T, typename... Args>
ColumnWithTypeAndName createConstColumn(const std::tuple<Args...> & data_type_args, size_t size, std::nullopt_t, const String & name = "", std::enable_if_t<TypeTraits<T>::is_nullable, int> = 0)
{
    return createConstColumn<T>(data_type_args, size, InferredFieldType<T>(std::nullopt), name);
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

ColumnWithTypeAndName executeFunction(Context & context, const String & func_name, const ColumnsWithTypeAndName & columns);

template <typename... Args>
ColumnWithTypeAndName executeFunction(Context & context, const String & func_name, const ColumnWithTypeAndName & first_column, const Args &... columns)
{
    ColumnsWithTypeAndName vec({first_column, columns...});
    return executeFunction(context, func_name, vec);
}

class FunctionTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        initializeDAGContext();
    }

public:
    static void SetUpTestCase()
    {
        try
        {
            DB::registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registed, ignore exception here.
        }
    }
    FunctionTest()
        : context(TiFlashTestEnv::getContext())
    {}
    virtual void initializeDAGContext()
    {
        dag_context_ptr = std::make_unique<DAGContext>(1024);
        context.setDAGContext(dag_context_ptr.get());
    }
    ColumnWithTypeAndName executeFunction(const String & func_name, const ColumnsWithTypeAndName & columns);

    template <typename... Args>
    ColumnWithTypeAndName executeFunction(const String & func_name, const ColumnWithTypeAndName & first_column, const Args &... columns)
    {
        ColumnsWithTypeAndName vec({first_column, columns...});
        return executeFunction(func_name, vec);
    }
    DAGContext & getDAGContext()
    {
        assert(dag_context_ptr != nullptr);
        return *dag_context_ptr;
    }

protected:
    Context context;
    std::unique_ptr<DAGContext> dag_context_ptr;
};

#define ASSERT_COLUMN_EQ(expected, actual) ASSERT_TRUE(DB::tests::columnEqual((expected), (actual)))

} // namespace tests
} // namespace DB
