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

using ColumnDefines = std::vector<std::tuple<String, String>>;

// createColumn returns
template <typename T>
ColumnPtr createColumn(DataTypePtr data_type, std::initializer_list<T> args);

template <typename... Args>
void insertInto(Block & block, Args... args);

void insertColumnDef(Block & block, const String & col_name, const DataTypePtr data_type);

void validateFieldType(const Field & field, DataTypePtr data_type);

void evalFunc(Block & block, const String & func_name, const Strings & args, const String & result);

void formatBlock(const Block & block, String & buff);

template <typename T>
static void insert(DataTypePtr data_type, MutableColumnPtr & column, const std::initializer_list<T> & args)
{
    for (auto & arg : args)
    {
        Field field = toField(arg);
        (void)data_type;
        //        validateFieldType(field, data_type);
        column->insert(field);
    }
}

template <typename T>
ColumnPtr createColumn(DataTypePtr data_type, std::initializer_list<T> args)
{
    MutableColumnPtr column = data_type->createColumn();
    insert(data_type, column, args);
    return column;
}

template <size_t I = 0, typename T, typename... Args>
static void insertRow(Block & block, T arg, Args... args)
{
    Field field;
    field = toField(arg);
    MutableColumnPtr column = std::move(*block.getByPosition(I).column).mutate();
    column->insert(field);
    block.getByPosition(I).column = std::move(column);

    if constexpr (0 != sizeof...(Args))
        insertRow<I + 1>(block, args...);
}

template <typename... Args>
void insertInto(Block & block, Args... args)
{
    insertRow(block, args...);
}

bool operator==(const IColumn & lhs, const IColumn & rhs);

std::ostream & operator<<(std::ostream & stream, IColumn const & column);

#define CREATE_COLUMN(column, data_type_name, ...)                                   \
    ColumnPtr column{};                                                              \
    do                                                                               \
    {                                                                                \
        DataTypePtr data_type_ptr = DataTypeFactory::instance().get(data_type_name); \
        column = createColumn(data_type_ptr, __VA_ARGS__);                           \
    } while (0)

#define CREATE_TABLE(block, ...)                                                               \
    Block block;                                                                               \
    do                                                                                         \
    {                                                                                          \
        std::vector<std::tuple<String, String>> cols{__VA_ARGS__};                             \
        for (auto & [col_name, data_type_name] : cols)                                         \
        {                                                                                      \
            insertColumnDef(block, col_name, DataTypeFactory::instance().get(data_type_name)); \
        }                                                                                      \
    } while (0)

#define INSERT_INTO(block, ...)         \
    do                                  \
    {                                   \
        insertInto(block, __VA_ARGS__); \
    } while (0)

#define ADD_COLUMN(block, name, data_type, column)                                           \
    {                                                                                        \
        ColumnWithTypeAndName col(column, DataTypeFactory::instance().get(data_type), name); \
        block.insert(col);                                                                   \
    }

#define EVAL_FUNC(block, func_name, result, ...)                 \
    do                                                           \
    {                                                            \
        evalFunc(block, func_name, Strings __VA_ARGS__, result); \
    } while (0)

#define PRINT_TABLE(block)              \
    do                                  \
    {                                   \
        String buff;                    \
        formatBlock(block, buff);       \
        std::cout << buff << std::endl; \
    } while (0)

} // namespace DB