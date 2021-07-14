#pragma once

#include <Common/FieldVisitors.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/types.h>

namespace DB
{
template <typename... Args>
ColumnPtr createColumn(DataTypePtr data_type, Args &&... args)
{
    MutableColumnPtr column = data_type->createColumn();
    (insert(data_type, column, args), ...);
    return column;
}

template <typename... Args>
void insertInto(Block & block, Args... args);

void insertColumnDef(Block & block, const String & col_name, const DataTypePtr data_type);

void evalFunc(Block & block, const String & func_name, const Strings & args, const String & result);

void formatBlock(const Block & block, String & buff);

bool operator==(const IColumn & lhs, const IColumn & rhs);

std::ostream & operator<<(std::ostream & stream, IColumn const & column);

#define APPLY_FOR_TYPE_LIST(M) \
    M(Int8)                    \
    M(Int16)                   \
    M(Int32)                   \
    M(Int64)                   \
    M(UInt8)                   \
    M(UInt16)                  \
    M(UInt32)                  \
    M(UInt64)                  \
    M(Float32)                 \
    M(Float64)                 \
    M(String)

// Specialized in utils.cpp
template <typename T>
inline bool validateDataTypeForField(DataTypePtr data_type)
{
#define VALIDATE_DATA_TYPE_FOR_FIELD(type_name)                    \
    if constexpr (std::is_same_v<T, type_name>)                    \
    {                                                              \
        return removeNullable(data_type)->getName() == #type_name; \
    }

    APPLY_FOR_TYPE_LIST(VALIDATE_DATA_TYPE_FOR_FIELD)

#undef VALIDATE_DATA_TYPE_FOR_FIELD

    if constexpr (isDecimalField<T>())
    {
        return String(removeNullable(data_type)->getFamilyName()) == "Decimal";
    }
    if constexpr (std::is_same_v<T, MyDateTime>)
    {
        return String(removeNullable(data_type)->getFamilyName()) == "MyDateTime";
    }
    if constexpr (std::is_same_v<T, MyDate>)
    {
        return String(removeNullable(data_type)->getFamilyName()) == "MyDate";
    }

    throw Exception("Shouldn't reach here: DataType " + data_type->getName() + " Literal type " + typeid(T).name());
}

#define DATA_TYPE(data_type_name) DataTypeFactory::instance().get(data_type_name)

// DecimalVal is a utility to help find the concrete DecimalField type.
template <int scale>
using ScaleToDecimalType = std::conditional_t<scale <= 9 && 0 < scale, Decimal32,
    std::conditional_t<scale <= 18 && 9 < scale, Decimal64,
        std::conditional_t<scale <= 38 && 18 < scale, Decimal128, std::conditional_t<scale <= 65, Decimal256, void>>>>;

template <int scale, typename T>
auto NewDecimalField(T decimal_lit)
{
    using DecimalType = ScaleToDecimalType<scale>;
    using FieldType = DecimalField<DecimalType>;

    return FieldType(DecimalType(decimal_lit), scale);
}

class Table
{
    Block data;

public:
    Table(std::initializer_list<std::tuple<String, String>> args)
    {
        for (auto & [col_name, data_type_name] : args)
        {
            DataTypePtr data_type = DataTypeFactory::instance().get(data_type_name);
            ColumnWithTypeAndName col(data_type, col_name);
            data.insert(std::move(col));
        }
    }

    Table(const Table &) = delete;

    Table(const Block & block) : data(block) {}

    Table(Block && block) : data(block) {}

    template <typename... Args>
    Table & insert(Args... args)
    {
        insertRow(data, args...);
        return *this;
    }

    template <typename... Args>
    Table & eval(const String & func_name, const String & result, Args &&... arguments)
    {
        Strings args;
        (args.push_back(std::forward<Args>(arguments)), ...);
        evalFunc(data, func_name, args, result);
        return *this;
    }

    Table clone() const;

    Block && build() { return std::move(data); }

    const Block & getData() { return data; }
};

template <typename U>
inline void insert(DataTypePtr data_type, MutableColumnPtr & column, U && arg)
{
    using T = std::decay_t<U>;
    (void)data_type;
    //        if (!validateDataTypeForField<T>(data_type))
    //            throw Exception("DataType doesn't match literal: " + data_type->getName() + ", " + typeid(T).name());
    Field field;
    if constexpr (std::is_same_v<T, const char *> || std::is_same_v<T, String>)
    {
        // String types: const char*, String
        field = Field(String(arg));
    }
    else if constexpr (isDecimalField<T>())
    {
        // Pass DecimalField with Decimal types
        // Decimal types: Decimal32, Decimal64, Decimal128, Decimal256
        field = arg;
    }
    else if constexpr (std::is_same_v<T, MyDateTime> || std::is_same_v<T, MyDate>)
    {
        // Datetime types: MyDateTime, MyDate
        field = toField(arg.toPackedUInt());
    }
    else if constexpr (std::is_floating_point_v<T> || std::is_integral_v<T>)
    {
        // Integral types: Int8, Int16, Int32, Int64, UInt8, UInt16, UInt64
        // Float point types: Float32, Float64
        field = toField(arg);
    }
    else if constexpr (std::is_same_v<T, Null>)
    {
        field = Null{};
    }
    else if constexpr (std::is_same_v<T, Field>)
    {
        // Given Field
        field = arg;
    }
    else
    {
        throw Exception("Unrecognized Literal type: " + String(typeid(T).name()));
    }
    column->insert(field);
}

template <typename T>
inline void insert(DataTypePtr data_type, MutableColumnPtr & column, const std::initializer_list<T> & args)
{
    for (auto & arg : args)
    {
        insert(data_type, column, std::move(arg));
    }
}

template <size_t I = 0, typename T, typename... Args>
void insertRow(Block & block, T arg, Args... args)
{
    //    if (!validateDataTypeForField<T>(block.getByPosition(I).type))
    //        throw Exception("DataType doesn't match literal: " + block.getByPosition(I).type->getName() + ", " + typeid(T).name());
    MutableColumnPtr column = std::move(*block.getByPosition(I).column).mutate();
    insert(block.getByPosition(I).type, column, {arg});
    block.getByPosition(I).column = std::move(column);

    if constexpr (0 != sizeof...(Args))
        insertRow<I + 1>(block, args...);
}

template <typename... Args>
void insertInto(Block & block, Args... args)
{
    insertRow(block, args...);
}

#define PRINT_TABLE(block)              \
    do                                  \
    {                                   \
        String buff;                    \
        formatBlock(block, buff);       \
        std::cout << buff << std::endl; \
    } while (0)

} // namespace DB
