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
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeMyDuration.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <TiDB/Decode/JsonBinary.h>
#include <TiDB/Decode/JsonPathExprRef.h>
#include <TiDB/Schema/TiDB.h>
#include <common/JSON.h>
#include <simdjson.h>
#include <tipb/expression.pb.h>

#include <ext/range.h>
#include <magic_enum.hpp>
#include <string_view>
#include <type_traits>

namespace DB
{
/** Json related functions:
  *
  * json_extract(json_object, path_string...) -
  *     The function takes 1 or more path_string parameters. Return the extracted JsonObject.
  *     Throw exception if any path_string failed to parse.
  * json_unquote(json_string)
  * cast_json_as_string(json_object)
  * json_length(json_object)
  * json_array(json_object...)
  * cast(column as json)
  *
  */

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int UNKNOWN_TYPE;
} // namespace ErrorCodes

inline bool isNullJsonBinary(size_t size)
{
    return size == 0;
}

using namespace GatherUtils;

class FunctionJsonExtract : public IFunction
{
public:
    static constexpr auto name = "json_extract";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonExtract>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

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
                throw Exception(
                    fmt::format("Illegal column {} of argument of function {}", json_column->getName(), getName()),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (const auto * string_col = checkAndGetColumn<ColumnString>(json_column))
        {
            source_data_column_ptr = string_col;
        }
        else
        {
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", json_column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
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
                    block.getByPosition(result).column
                        = block.getByPosition(result).type->createColumnConst(rows, Null());
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
                throw Exception(
                    fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()),
                    ErrorCodes::ILLEGAL_COLUMN);

            auto path_expr = JsonPathExpr::parseJsonPathExpr(path_str);
            /// If any path_expr failed to parse, return null
            if (!path_expr)
                throw Exception(
                    fmt::format(
                        "Illegal json path expression {} of argument of function {}",
                        column->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_COLUMN);
            path_expr_container_vec.push_back(std::make_unique<JsonPathExprRefContainer>(path_expr));
        }

        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const_json)
        {
            auto nullable_col
                = calculateResultCol(source_data_column_ptr, source_nullable_column_ptr, path_expr_container_vec);
            block.getByPosition(result).column = ColumnConst::create(std::move(nullable_col), rows);
        }
        else
        {
            block.getByPosition(result).column
                = calculateResultCol(source_data_column_ptr, source_nullable_column_ptr, path_expr_container_vec);
        }
    }

private:
    static MutableColumnPtr calculateResultCol(
        const ColumnString * source_col,
        const ColumnNullable * source_nullable_col,
        std::vector<JsonPathExprRefContainerPtr> & path_expr_container_vec)
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
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to);
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
                JsonBinary json_binary(
                    data_from[current_offset],
                    StringRef(&data_from[current_offset + 1], data_length - 1));
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


class FunctionJsonUnquote : public IFunction
{
public:
    static constexpr auto name = "json_unquote";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonUnquote>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

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
            JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to);
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
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionCastJsonAsString : public IFunction
{
public:
    static constexpr auto name = "cast_json_as_string";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionCastJsonAsString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

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
            JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to);
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
                    JsonBinary json_binary(
                        data_from[current_offset],
                        StringRef(&data_from[current_offset + 1], json_length - 1));
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
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionJsonLength : public IFunction
{
public:
    static constexpr auto name = "jsonLength";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonLength>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const auto * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnUInt64::create();
            typename ColumnUInt64::Container & vec_col_res = col_res->getData();
            {
                const auto & data = col->getChars();
                const auto & offsets = col->getOffsets();
                const size_t size = offsets.size();
                vec_col_res.resize(size);

                ColumnString::Offset prev_offset = 0;
                for (size_t i = 0; i < size; ++i)
                {
                    std::string_view sv(
                        reinterpret_cast<const char *>(&data[prev_offset]),
                        offsets[i] - prev_offset - 1);
                    vec_col_res[i] = JsonBinary::getJsonLength(sv);
                    prev_offset = offsets[i];
                }
            }
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionJsonArray : public IFunction
{
public:
    static constexpr auto name = "json_array";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonArray>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            if (!arguments[arg_idx]->onlyNull())
            {
                const auto * arg = removeNullable(arguments[arg_idx]).get();
                if (!arg->isStringOrFixedString())
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument {} of function {}",
                        arg->getName(),
                        arg_idx + 1,
                        getName());
            }
        }
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto nested_block = createBlockWithNestedColumns(block, arguments);
        StringSources sources;
        for (auto column_number : arguments)
        {
            sources.push_back(
                block.getByPosition(column_number).type->onlyNull()
                    ? nullptr
                    : createDynamicStringSource(*nested_block.getByPosition(column_number).column));
        }

        auto rows = block.rows();
        auto col_to = ColumnString::create();
        auto & data_to = col_to->getChars();
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to);
        auto & offsets_to = col_to->getOffsets();
        offsets_to.resize(rows);

        std::vector<const NullMap *> nullmaps;
        nullmaps.reserve(sources.size());
        bool is_input_nullable = false;
        for (auto column_number : arguments)
        {
            const auto & col = block.getByPosition(column_number).column;
            if (col->isColumnNullable())
            {
                const auto & column_nullable = static_cast<const ColumnNullable &>(*col);
                nullmaps.push_back(&(column_nullable.getNullMapData()));
                is_input_nullable = true;
            }
            else
            {
                nullmaps.push_back(nullptr);
            }
        }

        if (is_input_nullable)
            doExecuteImpl<true>(sources, rows, write_buffer, offsets_to, nullmaps);
        else
            doExecuteImpl<false>(sources, rows, write_buffer, offsets_to, nullmaps);

        data_to.resize(write_buffer.count());
        block.getByPosition(result).column = std::move(col_to);
    }

private:
    template <bool is_input_nullable>
    static void doExecuteImpl(
        StringSources & sources,
        size_t rows,
        JsonBinary::JsonBinaryWriteBuffer & write_buffer,
        ColumnString::Offsets & offsets_to,
        const std::vector<const NullMap *> & nullmaps)
    {
        std::vector<JsonBinary> jsons;
        jsons.reserve(sources.size());
        for (size_t i = 0; i < rows; ++i)
        {
            for (size_t col = 0; col < sources.size(); ++col)
            {
                if constexpr (is_input_nullable)
                {
                    if (!sources[col] || (nullmaps[col] && nullmaps[col][i] != 0))
                    {
                        jsons.emplace_back(JsonBinary::TYPE_CODE_LITERAL, StringRef(&JsonBinary::LITERAL_NIL, 1));
                    }
                    else
                    {
                        const auto & data_from = sources[col]->getWhole();
                        jsons.emplace_back(data_from.data[0], StringRef(&data_from.data[1], data_from.size - 1));
                    }
                }
                else
                {
                    assert(sources[col]);
                    const auto & data_from = sources[col]->getWhole();
                    jsons.emplace_back(data_from.data[0], StringRef(&data_from.data[1], data_from.size - 1));
                }
            }
            JsonBinary::buildBinaryJsonArrayInBuffer(jsons, write_buffer);
            jsons.clear();
            writeChar(0, write_buffer);
            offsets_to[i] = write_buffer.count();
            for (const auto & source : sources)
            {
                if constexpr (is_input_nullable)
                {
                    if (source)
                        source->next();
                }
                else
                {
                    assert(source);
                    source->next();
                }
            }
        }
    }
};


class FunctionCastJsonAsJson : public IFunction
{
public:
    static constexpr auto name = "cast_json_as_json";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionCastJsonAsJson>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override { return arguments[0]; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto from = block.getByPosition(arguments[0]).column;
        block.getByPosition(result).column = std::move(from);
    }
};

class FunctionCastRealAsJson : public IFunction
{
public:
    static constexpr auto name = "cast_real_as_json";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionCastRealAsJson>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!arguments[0]->isFloatingPoint())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto col_to = ColumnString::create();
        auto & data_to = col_to->getChars();
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to);
        auto & offsets_to = col_to->getOffsets();
        auto rows = block.rows();
        offsets_to.resize(rows);

        const auto & from = block.getByPosition(arguments[0]);
        if (from.type->getTypeId() == TypeIndex::Float32)
        {
            doExecute<Float32>(write_buffer, offsets_to, from.column);
        }
        else
        {
            doExecute<Float64>(write_buffer, offsets_to, from.column);
        }
        data_to.resize(write_buffer.count());
        block.getByPosition(result).column = std::move(col_to);
    }

private:
    template <typename FromType>
    static void doExecute(
        JsonBinary::JsonBinaryWriteBuffer & data_to,
        ColumnString::Offsets & offsets_to,
        const ColumnPtr & column_ptr_from)
    {
        const auto * column_from = checkAndGetColumn<ColumnVector<FromType>>(column_ptr_from.get());
        RUNTIME_CHECK(column_from);
        const auto & data_from = column_from->getData();
        for (size_t i = 0; i < data_from.size(); ++i)
        {
            JsonBinary::appendNumber(data_to, static_cast<Float64>(data_from[i]));
            writeChar(0, data_to);
            offsets_to[i] = data_to.count();
        }
    }
};

class FunctionCastDecimalAsJson : public IFunction
{
public:
    static constexpr auto name = "cast_decimal_as_json";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionCastDecimalAsJson>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!arguments[0]->isDecimal())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto col_to = ColumnString::create();
        auto & data_to = col_to->getChars();
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to);
        auto & offsets_to = col_to->getOffsets();
        auto rows = block.rows();
        offsets_to.resize(rows);

        const auto & from = block.getByPosition(arguments[0]);
        TypeIndex from_type_index = from.type->getTypeId();
        switch (from_type_index)
        {
        case TypeIndex::Decimal32:
            doExecute<Decimal32>(write_buffer, offsets_to, from.column);
            break;
        case TypeIndex::Decimal64:
            doExecute<Decimal64>(write_buffer, offsets_to, from.column);
            break;
        case TypeIndex::Decimal128:
            doExecute<Decimal128>(write_buffer, offsets_to, from.column);
            break;
        case TypeIndex::Decimal256:
            doExecute<Decimal256>(write_buffer, offsets_to, from.column);
            break;
        default:
            throw Exception(
                fmt::format(
                    "Illegal type {} of argument of function {}",
                    magic_enum::enum_name(from_type_index),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        data_to.resize(write_buffer.count());
        block.getByPosition(result).column = std::move(col_to);
    }

private:
    template <typename FromType>
    static void doExecute(
        JsonBinary::JsonBinaryWriteBuffer & data_to,
        ColumnString::Offsets & offsets_to,
        const ColumnPtr & column_ptr_from)
    {
        const auto * column_from = checkAndGetColumn<ColumnDecimal<FromType>>(column_ptr_from.get());
        RUNTIME_CHECK(column_from);
        for (size_t i = 0; i < column_from->size(); ++i)
        {
            const auto & field = (*column_from)[i].template safeGet<DecimalField<FromType>>();
            JsonBinary::appendNumber(data_to, static_cast<Float64>(field));
            writeChar(0, data_to);
            offsets_to[i] = data_to.count();
        }
    }
};

class FunctionCastIntAsJson : public IFunction
{
public:
    static constexpr auto name = "cast_int_as_json";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionCastIntAsJson>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    void setInputTiDBFieldType(const tipb::FieldType & tidb_tp_) { input_tidb_tp = &tidb_tp_; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (unlikely(!arguments[0]->isInteger()))
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto col_to = ColumnString::create();
        auto & data_to = col_to->getChars();
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to);
        auto & offsets_to = col_to->getOffsets();
        auto rows = block.rows();
        offsets_to.resize(rows);

        const auto & int_base_type = block.getByPosition(arguments[0]).type;
        bool is_types_valid = getIntType(int_base_type, [&](const auto & int_type, bool) {
            using IntType = std::decay_t<decltype(int_type)>;
            using IntFieldType = typename IntType::FieldType;
            const auto & from = block.getByPosition(arguments[0]);
            // In raw function test, input_tidb_tp is nullptr.
            if (unlikely(input_tidb_tp == nullptr) || !hasIsBooleanFlag(*input_tidb_tp))
            {
                if constexpr (std::is_unsigned_v<IntFieldType>)
                    doExecute<IntFieldType, UInt64>(write_buffer, offsets_to, from.column);
                else
                    doExecute<IntFieldType, Int64>(write_buffer, offsets_to, from.column);
            }
            else
            {
                doExecute<IntFieldType, bool>(write_buffer, offsets_to, from.column);
            }

            data_to.resize(write_buffer.count());
            block.getByPosition(result).column = std::move(col_to);
            return true;
        });

        if (unlikely(!is_types_valid))
            throw Exception(
                fmt::format("Illegal types {} arguments of function {}", int_base_type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    template <typename F>
    static bool getIntType(DataTypePtr type, F && f)
    {
        return castTypeToEither<
            DataTypeInt8,
            DataTypeInt16,
            DataTypeInt32,
            DataTypeInt64,
            DataTypeUInt8,
            DataTypeUInt16,
            DataTypeUInt32,
            DataTypeUInt64>(type.get(), std::forward<F>(f));
    }

    template <typename FromType, typename ToType>
    static void doExecute(
        JsonBinary::JsonBinaryWriteBuffer & data_to,
        ColumnString::Offsets & offsets_to,
        const ColumnPtr & column_ptr_from)
    {
        const auto * column_from = checkAndGetColumn<ColumnVector<FromType>>(column_ptr_from.get());
        RUNTIME_CHECK(column_from);
        const auto & data_from = column_from->getData();
        for (size_t i = 0; i < data_from.size(); ++i)
        {
            JsonBinary::appendNumber(data_to, static_cast<ToType>(data_from[i]));
            writeChar(0, data_to);
            offsets_to[i] = data_to.count();
        }
    }

private:
    const tipb::FieldType * input_tidb_tp = nullptr;
};

class FunctionCastStringAsJson : public IFunction
{
public:
    static constexpr auto name = "cast_string_as_json";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionCastStringAsJson>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    void setInputTiDBFieldType(const tipb::FieldType & tidb_tp_) { input_tidb_tp = &tidb_tp_; }
    void setOutputTiDBFieldType(const tipb::FieldType & tidb_tp_) { output_tidb_tp = &tidb_tp_; }
    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { collator = collator_; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & input_type = arguments[0];
        if (input_type->onlyNull())
        {
            return input_type;
        }

        if unlikely (!removeNullable(input_type)->isStringOrFixedString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        auto return_type = std::make_shared<DataTypeString>();
        return input_type->isNullable() ? makeNullable(return_type) : return_type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto & from = block.getByPosition(arguments[0]);
        if (from.type->onlyNull())
        {
            auto only_null_column = from.column;
            block.getByPosition(result).column = std::move(only_null_column);
            return;
        }

        auto nested_block = createBlockWithNestedColumns(block, arguments);
        auto input_source = createDynamicStringSource(*nested_block.getByPosition(arguments[0]).column);

        auto col_to = ColumnString::create();
        auto & data_to = col_to->getChars();
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to);
        auto & offsets_to = col_to->getOffsets();
        auto rows = block.rows();
        offsets_to.resize(rows);

        // In raw function test, input_tidb_tp/output_tidb_tp is nullptr.
        if (collator && collator->isBinary())
        {
            if ((unlikely(input_tidb_tp == nullptr)) || input_tidb_tp->tp() == TiDB::TypeString)
            {
                doExecuteForBinary<true>(
                    write_buffer,
                    offsets_to,
                    input_source,
                    input_tidb_tp->tp(),
                    input_tidb_tp->flen(),
                    block.rows());
            }
            else
            {
                doExecuteForBinary<false>(
                    write_buffer,
                    offsets_to,
                    input_source,
                    input_tidb_tp->tp(),
                    input_tidb_tp->flen(),
                    block.rows());
            }
        }
        else if ((unlikely(output_tidb_tp == nullptr)) || hasParseToJSONFlag(*output_tidb_tp))
        {
            if (from.column->isColumnNullable())
            {
                const auto & column_nullable = static_cast<const ColumnNullable &>(*from.column);
                doExecuteForParsingJson<true>(
                    write_buffer,
                    offsets_to,
                    input_source,
                    column_nullable.getNullMapData(),
                    block.rows());
            }
            else
            {
                auto tmp_null_map = ColumnUInt8::create(0, 0);
                doExecuteForParsingJson<false>(
                    write_buffer,
                    offsets_to,
                    input_source,
                    tmp_null_map->getData(),
                    block.rows());
            }
        }
        else
        {
            doExecuteForOthers(write_buffer, offsets_to, input_source, block.rows());
        }

        data_to.resize(write_buffer.count());
        if (from.column->isColumnNullable())
        {
            auto null_map = static_cast<const ColumnNullable &>(*from.column).getNullMapColumnPtr();
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(null_map));
        }
        else
        {
            block.getByPosition(result).column = std::move(col_to);
        }
    }

private:
    template <bool is_binary_str>
    static void doExecuteForBinary(
        JsonBinary::JsonBinaryWriteBuffer & data_to,
        ColumnString::Offsets & offsets_to,
        const std::unique_ptr<IStringSource> & data_from,
        UInt8 from_type_code,
        Int32 flen,
        size_t size)
    {
        for (size_t i = 0; i < size; ++i)
        {
            const auto & slice = data_from->getWhole();
            if constexpr (is_binary_str)
            {
                if (unlikely(flen <= 0))
                {
                    JsonBinary::appendOpaque(
                        data_to,
                        JsonBinary::Opaque{from_type_code, StringRef{slice.data, slice.size}});
                }
                else
                {
                    auto size_t_flen = static_cast<size_t>(flen);
                    if (slice.size >= size_t_flen)
                    {
                        JsonBinary::appendOpaque(
                            data_to,
                            JsonBinary::Opaque{from_type_code, StringRef{slice.data, size_t_flen}});
                    }
                    else
                    {
                        ColumnString::Chars_t buf;
                        buf.resize_fill(size_t_flen, 0);
                        std::memcpy(buf.data(), slice.data, slice.size);
                        JsonBinary::appendOpaque(
                            data_to,
                            JsonBinary::Opaque{from_type_code, StringRef{buf.data(), size_t_flen}});
                    }
                }
            }
            else
            {
                JsonBinary::appendOpaque(
                    data_to,
                    JsonBinary::Opaque{from_type_code, StringRef{slice.data, slice.size}});
            }
            writeChar(0, data_to);
            offsets_to[i] = data_to.count();
            data_from->next();
        }
    }

    template <bool is_nullable>
    static void doExecuteForParsingJson(
        JsonBinary::JsonBinaryWriteBuffer & data_to,
        ColumnString::Offsets & offsets_to,
        const std::unique_ptr<IStringSource> & data_from,
        const NullMap & null_map_from,
        size_t size)
    {
        simdjson::dom::parser parser;
        for (size_t i = 0; i < size; ++i)
        {
            if constexpr (is_nullable)
            {
                if (null_map_from[i])
                {
                    writeChar(0, data_to);
                    offsets_to[i] = data_to.count();
                    data_from->next();
                    continue;
                }
            }

            const auto & slice = data_from->getWhole();
            if (unlikely(slice.size == 0))
                throw Exception("Invalid JSON text: The document is empty.");

            const auto & json_elem = parser.parse(slice.data, slice.size);
            if (unlikely(json_elem.error()))
            {
                throw Exception(fmt::format(
                    "Invalid JSON text: The document root must not be followed by other values, details: {}",
                    simdjson::error_message(json_elem.error())));
            }
            JsonBinary::appendSIMDJsonElem(data_to, json_elem.value_unsafe());

            writeChar(0, data_to);
            offsets_to[i] = data_to.count();
            data_from->next();
        }
    }

    static void doExecuteForOthers(
        JsonBinary::JsonBinaryWriteBuffer & data_to,
        ColumnString::Offsets & offsets_to,
        const std::unique_ptr<IStringSource> & from_data,
        size_t size)
    {
        for (size_t i = 0; i < size; ++i)
        {
            const auto & slice = from_data->getWhole();
            JsonBinary::appendStringRef(data_to, StringRef{slice.data, slice.size});
            writeChar(0, data_to);
            offsets_to[i] = data_to.count();
            from_data->next();
        }
    }

private:
    const tipb::FieldType * input_tidb_tp = nullptr;
    const tipb::FieldType * output_tidb_tp = nullptr;
    TiDB::TiDBCollatorPtr collator = nullptr;
};

class FunctionCastTimeAsJson : public IFunction
{
public:
    static constexpr auto name = "cast_time_as_json";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionCastTimeAsJson>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    void setInputTiDBFieldType(const tipb::FieldType & tidb_tp_) { input_tidb_tp = &tidb_tp_; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!arguments[0]->isMyDateOrMyDateTime())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto col_to = ColumnString::create();
        auto & data_to = col_to->getChars();
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to);
        auto & offsets_to = col_to->getOffsets();
        auto rows = block.rows();
        offsets_to.resize(rows);

        const auto & from = block.getByPosition(arguments[0]);
        if (checkDataType<DataTypeMyDateTime>(from.type.get()))
        {
            doExecute<DataTypeMyDateTime, false>(write_buffer, offsets_to, from.column);
        }
        else if (checkDataType<DataTypeMyDate>(from.type.get()))
        {
            // In raw function test, input_tidb_tp is nullptr.
            bool is_timestamp = (unlikely(input_tidb_tp == nullptr)) || input_tidb_tp->tp() == TiDB::TypeTimestamp;
            if (is_timestamp)
                doExecute<DataTypeMyDate, true>(write_buffer, offsets_to, from.column);
            else
                doExecute<DataTypeMyDate, false>(write_buffer, offsets_to, from.column);
        }

        data_to.resize(write_buffer.count());
        block.getByPosition(result).column = std::move(col_to);
    }

private:
    template <typename FromDataType, bool is_timestamp>
    static void doExecute(
        JsonBinary::JsonBinaryWriteBuffer & data_to,
        ColumnString::Offsets & offsets_to,
        const ColumnPtr & column_ptr_from)
    {
        const auto * column_from
            = checkAndGetColumn<ColumnVector<typename FromDataType::FieldType>>(column_ptr_from.get());
        RUNTIME_CHECK(column_from);
        const auto & data_from = column_from->getData();
        for (size_t i = 0; i < data_from.size(); ++i)
        {
            if constexpr (std::is_same_v<DataTypeMyDate, FromDataType>)
            {
                MyDate date(data_from[i]);
                JsonBinary::appendDate(data_to, date);
            }
            else
            {
                MyDateTime date_time(data_from[i]);
                if constexpr (is_timestamp)
                    JsonBinary::appendTimestamp(data_to, date_time);
                else
                    JsonBinary::appendDatetime(data_to, date_time);
            }

            writeChar(0, data_to);
            offsets_to[i] = data_to.count();
        }
    }

private:
    const tipb::FieldType * input_tidb_tp = nullptr;
};

class FunctionCastDurationAsJson : public IFunction
{
public:
    static constexpr auto name = "cast_duration_as_json";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionCastDurationAsJson>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!arguments[0]->isMyTime())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto col_to = ColumnString::create();
        auto & data_to = col_to->getChars();
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to);
        auto & offsets_to = col_to->getOffsets();
        auto rows = block.rows();
        offsets_to.resize(rows);

        const auto & from = block.getByPosition(arguments[0]);
        if (const auto * duration_type = checkAndGetDataType<DataTypeMyDuration>(from.type.get());
            likely(duration_type))
        {
            auto fsp = duration_type->getFsp();
            const auto & col_from = checkAndGetColumn<ColumnVector<DataTypeMyDuration::FieldType>>(from.column.get());
            const auto & data_from = col_from->getData();
            for (size_t i = 0; i < data_from.size(); ++i)
            {
                JsonBinary::appendDuration(write_buffer, data_from[i], fsp);
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
            }
        }
        else
        {
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", from.type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        data_to.resize(write_buffer.count());
        block.getByPosition(result).column = std::move(col_to);
    }
};
} // namespace DB
