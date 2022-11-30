// Copyright 2022 PingCAP, Ltd.
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
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Storages/Transaction/JsonBinary.h>
#include <Storages/Transaction/JsonPathExprRef.h>

namespace DB
{
/** Json related functions:
  *
  * json_extract(json_object, path_string...) -
  *     The function takes 1 or more path_string parameters. Return the extracted JsonObject.
  *     Throw exception if any path_string failed to parse.
  * json_unquote(json_string)
  * cast_json_as_string(json_object)
  *
  */

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

inline bool isNullJsonBinary(size_t size)
{
    return size == 0;
}

class FunctionsJsonExtract : public IFunction
{
public:
    static constexpr auto name = "json_extract";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionsJsonExtract>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto & arg : arguments)
        {
            if (const auto * nested_type = checkAndGetDataType<DataTypeNullable>(arg.get()))
            {
                if unlikely (!nested_type->getNestedType()->isStringOrFixedString())
                    throw Exception(
                        "Illegal type " + arg->getName() + " of argument of function " + getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
            else if unlikely (!arg->isStringOrFixedString())
            {
                throw Exception(
                    "Illegal type " + arg->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
        return makeNullable(std::make_shared<DataTypeString>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        size_t rows = block.rows();
        /// First check if JsonObject is only null, if so, result is null, no need to parse path exprs even if path is
        const auto * json_column = block.getByPosition(arguments[0]).column.get();
        bool all_null = false;
        bool const_json = false;
        const ColumnString * source_data_column_ptr;
        const ColumnNullable * source_nullable_column_ptr = nullptr;
        if (const auto * const_nullable_col = checkAndGetColumnConst<ColumnNullable>(json_column))
        {
            const_json = true;
            json_column = const_nullable_col->getDataColumnPtr().get();
        }
        else if (const auto * const_col = checkAndGetColumnConst<ColumnString>(json_column))
        {
            const_json = true;
            json_column = const_col->getDataColumnPtr().get();
        }

        if (const auto * nullable_col = checkAndGetColumn<ColumnNullable>(json_column))
        {
            source_nullable_column_ptr = nullable_col;
            all_null = isColumnOnlyNull(nullable_col); /// changes
            source_data_column_ptr = checkAndGetColumn<ColumnString>(nullable_col->getNestedColumnPtr().get());
            if unlikely (!source_data_column_ptr)
                throw Exception(fmt::format("Illegal column {} of argument of function {}", json_column->getName(), getName()), ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (const auto * string_col = checkAndGetColumn<ColumnString>(json_column))
        {
            source_data_column_ptr = string_col;
        }
        else
        {
            throw Exception(fmt::format("Illegal column {} of argument of function {}", json_column->getName(), getName()), ErrorCodes::ILLEGAL_COLUMN);
        }

        if unlikely (all_null)
        {
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(rows, Null());
            return;
        }

        bool all_path_arguments_constants = true;
        size_t arguments_size = arguments.size();
        RUNTIME_CHECK(arguments_size > 1);
        for (size_t i = 0; i < arguments_size; ++i)
        {
            const auto & elem = block.getByPosition(arguments[i]);
            if (i > 0)
                all_path_arguments_constants &= elem.column->isColumnConst();
        }

        if unlikely (!all_path_arguments_constants)
            throw Exception(
                "None const type of json path argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        /// Parses all paths
        std::vector<JsonPathExprRefContainerPtr> path_expr_container_vec;
        path_expr_container_vec.reserve(arguments_size - 1);
        for (size_t i = 1; i < arguments_size; ++i)
        {
            const ColumnPtr column = block.getByPosition(arguments[i]).column;
            const auto * nested_column = static_cast<const ColumnConst *>(column.get())->getDataColumnPtr().get();
            StringRef path_str;
            if (const auto * nullable_string_path_col = checkAndGetColumn<ColumnNullable>(nested_column))
            {
                if unlikely (nullable_string_path_col->isNullAt(0))
                {
                    block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(rows, Null());
                    return;
                }
                nested_column = nullable_string_path_col->getNestedColumnPtr().get();
            }

            if (const auto * col = checkAndGetColumn<ColumnString>(nested_column))
            {
                path_str = col->getDataAt(0);
            }
            else if (const auto * fixed_string_col = checkAndGetColumn<ColumnFixedString>(nested_column))
            {
                path_str = fixed_string_col->getDataAt(0);
            }
            else
                throw Exception(fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()), ErrorCodes::ILLEGAL_COLUMN);

            auto path_expr = JsonPathExpr::parseJsonPathExpr(path_str);
            /// If any path_expr failed to parse, return null
            if (!path_expr)
                throw Exception(fmt::format("Illegal json path expression {} of argument of function {}", column->getName(), getName()), ErrorCodes::ILLEGAL_COLUMN);
            path_expr_container_vec.push_back(std::make_unique<JsonPathExprRefContainer>(path_expr));
        }

        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const_json)
        {
            auto nullable_col = calculateResultCol(source_data_column_ptr, source_nullable_column_ptr, path_expr_container_vec);
            block.getByPosition(result).column = ColumnConst::create(std::move(nullable_col), rows);
        }
        else
        {
            block.getByPosition(result).column = calculateResultCol(source_data_column_ptr, source_nullable_column_ptr, path_expr_container_vec);
        }
    }

private:
    static MutableColumnPtr calculateResultCol(const ColumnString * source_col, const ColumnNullable * source_nullable_col, std::vector<JsonPathExprRefContainerPtr> & path_expr_container_vec)
    {
        size_t rows = source_col->size();
        const ColumnString::Chars_t & data_from = source_col->getChars();
        const IColumn::Offsets & offsets_from = source_col->getOffsets();

        auto col_to = ColumnString::create();
        ColumnString::Chars_t & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();
        offsets_to.resize(rows);
        ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(rows, 0);
        ColumnUInt8::Container & vec_null_map = col_null_map->getData();
        WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);
        size_t current_offset = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            size_t next_offset = offsets_from[i];
            size_t data_length = next_offset - current_offset - 1;
            bool found;
            if unlikely (isNullJsonBinary(data_length))
            {
                found = false;
            }
            else
            {
                JsonBinary json_binary(data_from[current_offset], StringRef(&data_from[current_offset + 1], data_length - 1));
                found = json_binary.extract(path_expr_container_vec, write_buffer);
            }
            if (!found)
                vec_null_map[i] = 1;
            writeChar(0, write_buffer);
            offsets_to[i] = write_buffer.count();
            current_offset = next_offset;
        }
        data_to.resize(write_buffer.count());

        if (source_nullable_col)
        {
            const auto & source_null_map = source_nullable_col->getNullMapColumn().getData();
            for (size_t i = 0, size = vec_null_map.size(); i < size; ++i)
                if (source_null_map[i])
                    vec_null_map[i] = 1;
        }
        return ColumnNullable::create(std::move(col_to), std::move(col_null_map));
    }

    inline static bool isColumnOnlyNull(const ColumnNullable * column)
    {
        if unlikely (column->empty())
            return false;

        bool ret = true;
        const auto & data = column->getNullMapColumn().getData();
        auto size = data.size();
        for (size_t i = 0; i < size; ++i)
            ret &= (data[i] != 0);
        return ret;
    }
};


class FunctionsJsonUnquote : public IFunction
{
public:
    static constexpr auto name = "json_unquote";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionsJsonUnquote>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!arguments[0]->isString())
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return makeNullable(std::make_shared<DataTypeString>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        size_t rows = block.rows();
        if (const auto * col_from = checkAndGetColumn<ColumnString>(column.get()))
        {
            const ColumnString::Chars_t & data_from = col_from->getChars();
            const IColumn::Offsets & offsets_from = col_from->getOffsets();

            auto col_to = ColumnString::create();
            ColumnString::Chars_t & data_to = col_to->getChars();
            data_to.reserve(data_from.size()); /// Reserve the same size of from string
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            offsets_to.resize(rows);
            ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(rows, 0);
            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);
            size_t current_offset = 0;
            for (size_t i = 0; i < block.rows(); ++i)
            {
                size_t next_offset = offsets_from[i];
                size_t data_length = next_offset - current_offset - 1;
                JsonBinary::unquoteStringInBuffer(StringRef(&data_from[current_offset], data_length), write_buffer);
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
                current_offset = next_offset;
            }
            data_to.resize(write_buffer.count());
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map));
        }
        else
            throw Exception(fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()), ErrorCodes::ILLEGAL_COLUMN);
    }
};

class FunctionsCastJsonAsString : public IFunction
{
public:
    static constexpr auto name = "cast_json_as_string";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionsCastJsonAsString>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!arguments[0]->isString())
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return makeNullable(std::make_shared<DataTypeString>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        size_t rows = block.rows();
        if (const auto * col_from = checkAndGetColumn<ColumnString>(column.get()))
        {
            const ColumnString::Chars_t & data_from = col_from->getChars();
            const IColumn::Offsets & offsets_from = col_from->getOffsets();

            auto col_to = ColumnString::create();
            ColumnString::Chars_t & data_to = col_to->getChars();
            data_to.reserve(data_from.size() * 3 / 2); /// Rough estimate, 1.5x from TiDB
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            offsets_to.resize(rows);
            ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(rows, 0);
            ColumnUInt8::Container & vec_null_map = col_null_map->getData();
            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);
            size_t current_offset = 0;
            for (size_t i = 0; i < block.rows(); ++i)
            {
                size_t next_offset = offsets_from[i];
                size_t json_length = next_offset - current_offset - 1;
                if unlikely (isNullJsonBinary(json_length))
                {
                    vec_null_map[i] = 1;
                }
                else
                {
                    JsonBinary json_binary(data_from[current_offset], StringRef(&data_from[current_offset + 1], json_length - 1));
                    json_binary.toStringInBuffer(write_buffer);
                }
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
                current_offset = next_offset;
            }
            data_to.resize(write_buffer.count());
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map));
        }
        else
            throw Exception(fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()), ErrorCodes::ILLEGAL_COLUMN);
    }
};

} // namespace DB