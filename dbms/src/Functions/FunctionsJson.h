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
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Storages/Transaction/JsonPathExprRef.h>
#include <Storages/Transaction/JsonBinary.h>

namespace DB
{
/** Json related functions:
  *
  * json_extract(json_object, path_string...) - The function takes 1 or more path_string parameters. Return the extracted JsonObject.
  * json_unquote(json_string)
  */

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
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

    bool useDefaultImplementationForNulls() const override { return true; }
    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto & arg : arguments)
        {
            if unlikely (!arg->isStringOrFixedString())
                throw Exception(
                    "Illegal type " + arg->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        return makeNullable(std::make_shared<DataTypeString>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
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
            const auto nested_column  = static_cast<const ColumnConst *>(column.get())->getDataColumnPtr();
            StringRef path_str;
            if (const auto * col = checkAndGetColumn<ColumnString>(nested_column.get()))
            {
                path_str = col->getDataAt(0);
            }
            else if (const auto * fixed_string_col = checkAndGetColumn<ColumnFixedString>(nested_column.get()))
            {
                path_str = fixed_string_col->getDataAt(0);
            }
            else
                throw Exception(fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()), ErrorCodes::ILLEGAL_COLUMN);

            auto path_expr = JsonPathExpr::parseJsonPathExpr(path_str);
            /// If any path_expr failed to parse, return null
            if (!path_expr)
            {
                block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
                return;
            }
            path_expr_container_vec.push_back(std::make_unique<JsonPathExprRefContainer>(path_expr));
        }

        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        size_t rows = block.rows();
        if (const auto * const_col = checkAndGetColumnConst<ColumnString>(column.get()))
        {
            const auto & data = const_col->getDataAt(0);
            /// Null JsonBinary
            if (data.size == 0)
            {
                block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(rows, Null());
                return;
            }
            auto col_to = ColumnString::create();
            ColumnString::Chars_t & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            offsets_to.resize(1);
            ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(1, 0);
            ColumnUInt8::Container & vec_null_map = col_null_map->getData();
            JsonBinary json_binary(data.data[0], StringRef(data.data + 1, data.size - 1));
            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);
            bool found = json_binary.extract(path_expr_container_vec, write_buffer);
            if (!found)
                vec_null_map[0] = 1;
            writeChar(0, write_buffer);
            auto buffer_count = write_buffer.count();
            offsets_to[0] = buffer_count;
            data_to.resize(buffer_count);
            auto nullable_col = ColumnNullable::create(std::move(col_to), std::move(col_null_map));
            block.getByPosition(result).column = ColumnConst::create(std::move(nullable_col), rows);
        }
        else if (const auto * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_to = ColumnString::create();
            ColumnString::Chars_t & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            offsets_to.resize(rows);
            ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(rows, 0);
            ColumnUInt8::Container & vec_null_map = col_null_map->getData();
            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                const auto & from_data = col->getDataAt(i);
                bool found;
                if (from_data.size == 0)
                {
                    found = false;
                }
                else
                {
                    JsonBinary json_binary(from_data.data[0], StringRef(from_data.data + 1, from_data.size - 1));
                    found = json_binary.extract(path_expr_container_vec, write_buffer);
                }
                if (!found)
                    vec_null_map[i] = 1;
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
            }
            data_to.resize(write_buffer.count());
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map));
        }
        else
            throw Exception(fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()), ErrorCodes::ILLEGAL_COLUMN);
    }

private:
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
        if (const auto * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_to = ColumnString::create();
            ColumnString::Chars_t & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            offsets_to.resize(rows);
            ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(rows, 0);
            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                const auto & from_data = col->getDataAt(i);
                auto str = JsonBinary::unquoteString(from_data);
                write_buffer.write(str.c_str(), str.length());
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
            }
            data_to.resize(write_buffer.count());
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map));
        }
        else
            throw Exception(fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()), ErrorCodes::ILLEGAL_COLUMN);
    }
private:
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
        if (const auto * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_to = ColumnString::create();
            ColumnString::Chars_t & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            offsets_to.resize(rows);
            ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(rows, 0);
            ColumnUInt8::Container & vec_null_map = col_null_map->getData();
            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                const auto & from_data = col->getDataAt(i);
                if (from_data.size == 0)
                {
                    vec_null_map[i] = 1;
                }
                else
                {
                    JsonBinary json_binary(from_data.data[0], StringRef(from_data.data + 1, from_data.size - 1));
                    auto str = json_binary.toString();
                    write_buffer.write(str.c_str(), str.length());
                }
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
            }
            data_to.resize(write_buffer.count());
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map));
        }
        else
            throw Exception(fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()), ErrorCodes::ILLEGAL_COLUMN);
    }
private:
};

} // namespace DB