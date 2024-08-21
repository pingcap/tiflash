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
#include <Columns/ColumnsCommon.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeMyDuration.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsTiDBConversion.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/Context.h>
#include <TiDB/Decode/JsonBinary.h>
#include <TiDB/Decode/JsonPathExprRef.h>
#include <TiDB/Decode/JsonScanner.h>
#include <TiDB/Schema/TiDBTypes.h>
#include <TiDB/Schema/TiDB_fwd.h>
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
    bool isVariadic() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (arguments.size() < 2)
        {
            throw Exception(
                fmt::format("Illegal arguments count {} of function {}", arguments.size(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        for (const auto & arg : arguments)
        {
            if (!arg->onlyNull())
            {
                const auto * nested_arg_type = removeNullable(arg).get();
                if unlikely (!nested_arg_type->isStringOrFixedString())
                    throw Exception(
                        fmt::format("Illegal type {} of argument of function {}", arg->getName(), getName()),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
        return makeNullable(std::make_shared<DataTypeString>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        size_t rows = block.rows();
        auto & res_col = block.getByPosition(result).column;
        /// First check if JsonObject is only null, if so, result is null, no need to parse path exprs even if path is
        const auto * json_column = block.getByPosition(arguments[0]).column.get();
        if unlikely (json_column->onlyNull())
        {
            res_col = block.getByPosition(result).type->createColumnConst(rows, Null());
            return;
        }

        auto nested_block = createBlockWithNestedColumns(block, arguments);
        auto json_source = createDynamicStringSource(*nested_block.getByPosition(arguments[0]).column);

        bool all_path_arguments_constants = true;
        size_t arguments_size = arguments.size();
        RUNTIME_CHECK(arguments_size > 1);
        StringSources path_sources;
        path_sources.reserve(arguments.size() - 1);
        std::vector<const NullMap *> path_null_maps;
        path_null_maps.reserve(arguments.size() - 1);
        for (size_t i = 1; i < arguments_size; ++i)
        {
            const auto & path_col = block.getByPosition(arguments[i]).column;
            if (path_col->onlyNull())
            {
                res_col = block.getByPosition(result).type->createColumnConst(rows, Null());
                return;
            }

            all_path_arguments_constants &= path_col->isColumnConst();
            path_sources.push_back(createDynamicStringSource(*nested_block.getByPosition(arguments[i]).column));
            if (path_col->isColumnNullable())
            {
                const auto & path_column_nullable = static_cast<const ColumnNullable &>(*path_col);
                path_null_maps.push_back(&path_column_nullable.getNullMapData());
            }
            else
            {
                path_null_maps.push_back(nullptr);
            }
        }

        if (json_column->isColumnNullable())
        {
            const auto & column_nullable = static_cast<const ColumnNullable &>(*json_column);
            const auto & null_map = column_nullable.getNullMapData();
            if (all_path_arguments_constants)
            {
                res_col = doExecuteForConstPaths<true>(json_source, null_map, path_sources, rows);
            }
            else
            {
                res_col = doExecuteCommon<true>(json_source, null_map, path_sources, path_null_maps, rows);
            }
        }
        else
        {
            if (all_path_arguments_constants)
            {
                res_col = doExecuteForConstPaths<false>(json_source, {}, path_sources, rows);
            }
            else
            {
                res_col = doExecuteCommon<false>(json_source, {}, path_sources, path_null_maps, rows);
            }
        }
    }

private:
    template <bool is_json_nullable>
    MutableColumnPtr doExecuteForConstPaths(
        const std::unique_ptr<IStringSource> & json_source,
        const NullMap & null_map_json,
        const StringSources & path_sources,
        size_t rows) const
    {
#define FINISH_PER_ROW                      \
    writeChar(0, write_buffer);             \
    offsets_to[row] = write_buffer.count(); \
    json_source->next();

        // build path expressions for const paths.
        auto path_expr_container_vec = buildJsonPathExprContainer(path_sources);

        auto col_to = ColumnString::create();
        ColumnString::Chars_t & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();
        offsets_to.resize(rows);
        ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(rows, 0);
        ColumnUInt8::Container & null_map_to = col_null_map->getData();
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to, rows);

        for (size_t row = 0; row < rows; ++row)
        {
            if constexpr (is_json_nullable)
            {
                if (null_map_json[row])
                {
                    FINISH_PER_ROW
                    null_map_to[row] = 1;
                    continue;
                }
            }

            const auto & json_val = json_source->getWhole();
            assert(json_val.size > 0);
            JsonBinary json_binary(json_val.data[0], StringRef(&json_val.data[1], json_val.size - 1));
            if (!json_binary.extract(path_expr_container_vec, write_buffer))
                null_map_to[row] = 1;
            FINISH_PER_ROW
        }
        return ColumnNullable::create(std::move(col_to), std::move(col_null_map));

#undef FINISH_PER_ROW
    }

    template <bool is_json_nullable>
    MutableColumnPtr doExecuteCommon(
        const std::unique_ptr<IStringSource> & json_source,
        const NullMap & null_map_json,
        const StringSources & path_sources,
        const std::vector<const NullMap *> & path_null_maps,
        size_t rows) const
    {
#define FINISH_PER_ROW                            \
    writeChar(0, write_buffer);                   \
    offsets_to[row] = write_buffer.count();       \
    for (const auto & path_source : path_sources) \
    {                                             \
        assert(path_source);                      \
        path_source->next();                      \
    }                                             \
    json_source->next();

        auto col_to = ColumnString::create();
        ColumnString::Chars_t & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();
        offsets_to.resize(rows);
        ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(rows, 0);
        ColumnUInt8::Container & null_map_to = col_null_map->getData();
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to, rows);

        for (size_t row = 0; row < rows; ++row)
        {
            if constexpr (is_json_nullable)
            {
                if (null_map_json[row])
                {
                    FINISH_PER_ROW
                    null_map_to[row] = 1;
                    continue;
                }
            }

            bool has_null_path = false;
            for (const auto * path_null_map : path_null_maps)
            {
                if (path_null_map && (*path_null_map)[row])
                {
                    has_null_path = true;
                    break;
                }
            }
            if (has_null_path)
            {
                FINISH_PER_ROW
                null_map_to[row] = 1;
                continue;
            }

            const auto & json_val = json_source->getWhole();
            assert(json_val.size > 0);
            JsonBinary json_binary(json_val.data[0], StringRef(&json_val.data[1], json_val.size - 1));
            auto path_expr_container_vec = buildJsonPathExprContainer(path_sources);
            if (!json_binary.extract(path_expr_container_vec, write_buffer))
                null_map_to[row] = 1;
            FINISH_PER_ROW
        }
        return ColumnNullable::create(std::move(col_to), std::move(col_null_map));

#undef FINISH_PER_ROW
    }

    std::vector<JsonPathExprRefContainerPtr> buildJsonPathExprContainer(const StringSources & path_sources) const
    {
        std::vector<JsonPathExprRefContainerPtr> path_expr_container_vec;
        path_expr_container_vec.reserve(path_sources.size());
        for (const auto & path_source : path_sources)
        {
            assert(path_source);
            const auto & path_val = path_source->getWhole();
            const auto & path_str_ref = StringRef{path_val.data, path_val.size};
            auto path_expr = JsonPathExpr::parseJsonPathExpr(path_str_ref);
            /// If path_expr failed to parse, throw exception
            if unlikely (!path_expr)
                throw Exception(
                    fmt::format(
                        "Illegal json path expression `{}` of function {}",
                        path_str_ref.toStringView(),
                        getName()),
                    ErrorCodes::ILLEGAL_COLUMN);

            path_expr_container_vec.push_back(std::make_unique<JsonPathExprRefContainer>(path_expr));
        }
        return path_expr_container_vec;
    }
};


class FunctionJsonUnquote : public IFunction
{
public:
    static constexpr auto name = "json_unquote";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonUnquote>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    void setNeedValidCheck(bool need_valid_check_) { need_valid_check = need_valid_check_; }
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
            if (need_valid_check)
                doUnquote<true>(block, data_from, offsets_from, offsets_to, write_buffer);
            else
                doUnquote<false>(block, data_from, offsets_from, offsets_to, write_buffer);
            data_to.resize(write_buffer.count());
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map));
        }
        else
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    template <bool validCheck>
    void doUnquote(
        const Block & block,
        const ColumnString::Chars_t & data_from,
        const IColumn::Offsets & offsets_from,
        IColumn::Offsets & offsets_to,
        JsonBinary::JsonBinaryWriteBuffer & write_buffer) const
    {
        size_t current_offset = 0;
        for (size_t i = 0; i < block.rows(); ++i)
        {
            size_t next_offset = offsets_from[i];
            size_t data_length = next_offset - current_offset - 1;
            if constexpr (validCheck)
            {
                // TODO(hyb): use SIMDJson to check when SIMDJson is proved in practice
                if (data_length >= 2 && data_from[current_offset] == '"' && data_from[next_offset - 2] == '"'
                    && unlikely(
                        !checkJsonValid(reinterpret_cast<const char *>(&data_from[current_offset]), data_length)))
                {
                    throw Exception(
                        "Invalid JSON text: The document root must not be followed by other values.",
                        ErrorCodes::ILLEGAL_COLUMN);
                }
            }
            JsonBinary::unquoteStringInBuffer(StringRef(&data_from[current_offset], data_length), write_buffer);
            writeChar(0, write_buffer);
            offsets_to[i] = write_buffer.count();
            current_offset = next_offset;
        }
    }

private:
    bool need_valid_check = false;
};


class FunctionCastJsonAsString : public IFunction
{
public:
    static constexpr auto name = "cast_json_as_string";
    static FunctionPtr create(const Context & context)
    {
        if (!context.getDAGContext())
        {
            throw Exception("DAGContext should not be nullptr.", ErrorCodes::LOGICAL_ERROR);
        }
        return std::make_shared<FunctionCastJsonAsString>(context);
    }

    explicit FunctionCastJsonAsString(const Context & context)
        : context(context)
    {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    void setOutputTiDBFieldType(const tipb::FieldType & tidb_tp_) { tidb_tp = &tidb_tp_; }

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
            if likely (tidb_tp->flen() < 0)
            {
                size_t current_offset = 0;
                for (size_t i = 0; i < block.rows(); ++i)
                {
                    size_t next_offset = offsets_from[i];
                    size_t json_length = next_offset - current_offset - 1;
                    if unlikely (isNullJsonBinary(json_length))
                        vec_null_map[i] = 1;
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
            }
            else
            {
                ColumnString::Chars_t container_per_element;
                size_t current_offset = 0;
                for (size_t i = 0; i < block.rows(); ++i)
                {
                    size_t next_offset = offsets_from[i];
                    size_t json_length = next_offset - current_offset - 1;
                    if unlikely (isNullJsonBinary(json_length))
                        vec_null_map[i] = 1;
                    else
                    {
                        JsonBinary::JsonBinaryWriteBuffer element_write_buffer(container_per_element);
                        JsonBinary json_binary(
                            data_from[current_offset],
                            StringRef(&data_from[current_offset + 1], json_length - 1));
                        json_binary.toStringInBuffer(element_write_buffer);
                        size_t orig_length = element_write_buffer.count();
                        auto byte_length = charLengthToByteLengthFromUTF8(
                            reinterpret_cast<char *>(container_per_element.data()),
                            orig_length,
                            tidb_tp->flen());
                        if (byte_length < element_write_buffer.count())
                            context.getDAGContext()->handleTruncateError("Data Too Long");
                        write_buffer.write(reinterpret_cast<char *>(container_per_element.data()), byte_length);
                    }

                    writeChar(0, write_buffer);
                    offsets_to[i] = write_buffer.count();
                    current_offset = next_offset;
                }
            }
            data_to.resize(write_buffer.count());
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map));
        }
        else
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }

private:
    const tipb::FieldType * tidb_tp = nullptr;
    const Context & context;
};

class FunctionJsonLength : public IFunction
{
public:
    static constexpr auto name = "jsonLength";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonLength>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (unlikely(arguments.size() != 1 && arguments.size() != 2))
            throw Exception(
                fmt::format("Illegal arguments count {} of function {}", arguments.size(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        for (const auto & arg : arguments)
        {
            if (!arg->onlyNull())
            {
                if (unlikely(!removeNullable(arg)->isString()))
                    throw Exception(
                        fmt::format("Illegal type {} of argument of function {}", arg->getName(), getName()),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
        return (arguments[0]->isNullable() || arguments.size() == 2) ? makeNullable(std::make_shared<DataTypeUInt64>())
                                                                     : std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        size_t rows = block.rows();
        auto & res_col = block.getByPosition(result).column;
        /// First check if Json object is only null.
        const auto * json_column = block.getByPosition(arguments[0]).column.get();
        if unlikely (json_column->onlyNull())
        {
            res_col = block.getByPosition(result).type->createColumnConst(rows, Null());
            return;
        }

        auto nested_block = createBlockWithNestedColumns(block, arguments);
        auto json_source = createDynamicStringSource(*nested_block.getByPosition(arguments[0]).column);
        if (arguments.size() == 1)
        {
            if (json_column->isColumnNullable())
            {
                const auto & json_column_nullable = static_cast<const ColumnNullable &>(*json_column);
                res_col = doExecuteForOneArg<true>(json_source, json_column_nullable.getNullMapData(), rows);
            }
            else
            {
                res_col = doExecuteForOneArg<false>(json_source, {}, rows);
            }
        }
        else
        {
            const auto * path_column = block.getByPosition(arguments[1]).column.get();
            /// First check if path expr is only null.
            if unlikely (path_column->onlyNull())
            {
                res_col = block.getByPosition(result).type->createColumnConst(rows, Null());
                return;
            }

            if (json_column->isColumnNullable())
            {
                const auto & json_column_nullable = static_cast<const ColumnNullable &>(*json_column);
                if (path_column->isColumnConst())
                {
                    auto path_source = createDynamicStringSource(*nested_block.getByPosition(arguments[1]).column);
                    res_col = doExecuteConstForTwoArgs<true>(
                        json_source,
                        json_column_nullable.getNullMapData(),
                        path_source,
                        rows);
                }
                else
                {
                    auto path_source = createDynamicStringSource(*nested_block.getByPosition(arguments[1]).column);
                    if (path_column->isColumnNullable())
                    {
                        const auto & path_column_nullable = static_cast<const ColumnNullable &>(*path_column);
                        res_col = doExecuteCommonForTwoArgs<true, true>(
                            json_source,
                            json_column_nullable.getNullMapData(),
                            path_source,
                            path_column_nullable.getNullMapData(),
                            rows);
                    }
                    else
                    {
                        res_col = doExecuteCommonForTwoArgs<true, false>(
                            json_source,
                            json_column_nullable.getNullMapData(),
                            path_source,
                            {},
                            rows);
                    }
                }
            }
            else
            {
                if (path_column->isColumnConst())
                {
                    auto path_source = createDynamicStringSource(*nested_block.getByPosition(arguments[1]).column);
                    res_col = doExecuteConstForTwoArgs<false>(json_source, {}, path_source, rows);
                }
                else
                {
                    auto path_source = createDynamicStringSource(*nested_block.getByPosition(arguments[1]).column);
                    if (path_column->isColumnNullable())
                    {
                        const auto & path_column_nullable = static_cast<const ColumnNullable &>(*path_column);
                        res_col = doExecuteCommonForTwoArgs<false, true>(
                            json_source,
                            {},
                            path_source,
                            path_column_nullable.getNullMapData(),
                            rows);
                    }
                    else
                    {
                        res_col = doExecuteCommonForTwoArgs<false, false>(json_source, {}, path_source, {}, rows);
                    }
                }
            }
        }
    }

private:
    template <bool is_nullable>
    static MutableColumnPtr doExecuteForOneArg(
        const std::unique_ptr<IStringSource> & json_source,
        const NullMap & null_map_json,
        size_t rows)
    {
        auto col_to = ColumnUInt64::create(rows, 1);
        auto & data_to = col_to->getData();
        ColumnUInt8::MutablePtr col_null_map;
        if constexpr (is_nullable)
            col_null_map = ColumnUInt8::create(rows, 0);
        else
            col_null_map = ColumnUInt8::create(0, 0);
        ColumnUInt8::Container & null_map_to = col_null_map->getData();
        for (size_t row = 0; row < rows; ++row)
        {
            if constexpr (is_nullable)
            {
                if (null_map_json[row])
                {
                    json_source->next();
                    null_map_to[row] = 1;
                    continue;
                }
            }

            const auto & json_val = json_source->getWhole();
            assert(json_val.size > 0);
            JsonBinary json_binary(json_val.data[0], StringRef(&json_val.data[1], json_val.size - 1));
            if (json_binary.getType() == JsonBinary::TYPE_CODE_ARRAY
                || json_binary.getType() == JsonBinary::TYPE_CODE_OBJECT)
                data_to[row] = json_binary.getElementCount();
            json_source->next();
        }

        if constexpr (is_nullable)
            return ColumnNullable::create(std::move(col_to), std::move(col_null_map));
        else
            return col_to;
    }

    template <bool is_json_nullable, bool is_path_nullable>
    static MutableColumnPtr doExecuteCommonForTwoArgs(
        const std::unique_ptr<IStringSource> & json_source,
        const NullMap & null_map_json,
        const std::unique_ptr<IStringSource> & path_source,
        const NullMap & null_map_path,
        size_t rows)
    {
#define FINISH_PER_ROW   \
    json_source->next(); \
    path_source->next();

        auto col_to = ColumnUInt64::create(rows, 1);
        auto & data_to = col_to->getData();
        ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(rows, 0);
        ColumnUInt8::Container & null_map_to = col_null_map->getData();
        for (size_t row = 0; row < rows; ++row)
        {
            if constexpr (is_json_nullable)
            {
                if (null_map_json[row])
                {
                    FINISH_PER_ROW
                    null_map_to[row] = 1;
                    continue;
                }
            }

            if constexpr (is_path_nullable)
            {
                if (null_map_path[row])
                {
                    FINISH_PER_ROW
                    null_map_to[row] = 1;
                    continue;
                }
            }

            const auto & json_val = json_source->getWhole();
            assert(json_val.size > 0);
            JsonBinary json_binary(json_val.data[0], StringRef(&json_val.data[1], json_val.size - 1));

            const auto & path_val = path_source->getWhole();
            auto path_expr_container_vec = buildPathExprContainer(StringRef{path_val.data, path_val.size});

            auto extract_json_binaries = json_binary.extract(path_expr_container_vec);
            if (extract_json_binaries.empty())
            {
                FINISH_PER_ROW
                null_map_to[row] = 1;
                continue;
            }
            assert(extract_json_binaries.size() == 1);
            const auto & extract_json_binary = extract_json_binaries.back();
            if (extract_json_binary.getType() == JsonBinary::TYPE_CODE_ARRAY
                || extract_json_binary.getType() == JsonBinary::TYPE_CODE_OBJECT)
                data_to[row] = extract_json_binary.getElementCount();
            FINISH_PER_ROW
        }
#undef FINISH_PER_ROW

        return ColumnNullable::create(std::move(col_to), std::move(col_null_map));
    }

    template <bool is_json_nullable>
    static MutableColumnPtr doExecuteConstForTwoArgs(
        const std::unique_ptr<IStringSource> & json_source,
        const NullMap & null_map_json,
        const std::unique_ptr<IStringSource> & path_source,
        size_t rows)
    {
        std::vector<JsonPathExprRefContainerPtr> path_expr_container_vec;
        assert(path_source);
        const auto & path_val = path_source->getWhole();
        path_expr_container_vec = buildPathExprContainer(StringRef{path_val.data, path_val.size});

        auto col_to = ColumnUInt64::create(rows, 1);
        auto & data_to = col_to->getData();
        ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(rows, 0);
        ColumnUInt8::Container & null_map_to = col_null_map->getData();
        for (size_t row = 0; row < rows; ++row)
        {
            if constexpr (is_json_nullable)
            {
                if (null_map_json[row])
                {
                    json_source->next();
                    null_map_to[row] = 1;
                    continue;
                }
            }

            const auto & json_val = json_source->getWhole();
            assert(json_val.size > 0);
            JsonBinary json_binary(json_val.data[0], StringRef(&json_val.data[1], json_val.size - 1));
            auto extract_json_binaries = json_binary.extract(path_expr_container_vec);
            if (extract_json_binaries.empty())
            {
                json_source->next();
                null_map_to[row] = 1;
                continue;
            }
            assert(extract_json_binaries.size() == 1);
            const auto & extract_json_binary = extract_json_binaries.back();
            if (extract_json_binary.getType() == JsonBinary::TYPE_CODE_ARRAY
                || extract_json_binary.getType() == JsonBinary::TYPE_CODE_OBJECT)
                data_to[row] = extract_json_binary.getElementCount();
            json_source->next();
        }

        return ColumnNullable::create(std::move(col_to), std::move(col_null_map));
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
        if (arguments.empty())
        {
            // clang-format off
            const UInt8 empty_array_json_value[] = {
                JsonBinary::TYPE_CODE_ARRAY, // array_type
                0x0, 0x0, 0x0, 0x0, // element_count
                0x8, 0x0, 0x0, 0x0}; // total_size
            // clang-format on
            auto empty_array_json = ColumnString::create();
            empty_array_json->insertData(
                reinterpret_cast<const char *>(empty_array_json_value),
                sizeof(empty_array_json_value) / sizeof(UInt8));
            block.getByPosition(result).column = ColumnConst::create(std::move(empty_array_json), block.rows());
            return;
        }

        auto nested_block = createBlockWithNestedColumns(block, arguments);
        StringSources sources;
        for (auto column_number : arguments)
        {
            sources.push_back(
                block.getByPosition(column_number).column->onlyNull()
                    ? nullptr
                    : createDynamicStringSource(*nested_block.getByPosition(column_number).column));
        }

        auto rows = block.rows();
        auto col_to = ColumnString::create();
        auto & data_to = col_to->getChars();
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
            doExecuteImpl<true>(sources, rows, data_to, offsets_to, nullmaps);
        else
            doExecuteImpl<false>(sources, rows, data_to, offsets_to, nullmaps);

        block.getByPosition(result).column = std::move(col_to);
    }

private:
    template <bool is_input_nullable>
    static void doExecuteImpl(
        StringSources & sources,
        size_t rows,
        ColumnString::Chars_t & data_to,
        ColumnString::Offsets & offsets_to,
        const std::vector<const NullMap *> & nullmaps)
    {
        // rows * json_type.
        size_t reserve_size = rows;
        // for only null: null literal.
        // for non only null: size of data_from.
        for (const auto & source : sources)
            reserve_size += source ? source->getSizeForReserve() : rows;
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to, reserve_size);

        std::vector<JsonBinary> jsons;
        jsons.reserve(sources.size());
        for (size_t i = 0; i < rows; ++i)
        {
            for (size_t col = 0; col < sources.size(); ++col)
            {
                if constexpr (is_input_nullable)
                {
                    const auto * nullmap = nullmaps[col];
                    if (!sources[col] || (nullmap && (*nullmap)[i]))
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
        auto rows = block.rows();
        auto col_to = ColumnString::create();
        auto & data_to = col_to->getChars();
        auto & offsets_to = col_to->getOffsets();
        offsets_to.resize(rows);

        const auto & from = block.getByPosition(arguments[0]);
        if (from.type->getTypeId() == TypeIndex::Float32)
        {
            doExecute<Float32>(data_to, offsets_to, from.column);
        }
        else
        {
            doExecute<Float64>(data_to, offsets_to, from.column);
        }
        block.getByPosition(result).column = std::move(col_to);
    }

private:
    template <typename FromType>
    static void doExecute(
        ColumnString::Chars_t & data_to,
        ColumnString::Offsets & offsets_to,
        const ColumnPtr & column_ptr_from)
    {
        const auto * column_from = checkAndGetColumn<ColumnVector<FromType>>(column_ptr_from.get());
        RUNTIME_CHECK(column_from);
        const auto & data_from = column_from->getData();
        // json_type + char 0 of string end + value
        size_t reserve_size = data_from.size() * (1 + 1 + sizeof(Float64));
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to, reserve_size);
        for (size_t i = 0; i < data_from.size(); ++i)
        {
            JsonBinary::appendNumber(write_buffer, static_cast<Float64>(data_from[i]));
            writeChar(0, write_buffer);
            offsets_to[i] = write_buffer.count();
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
        auto rows = block.rows();
        auto col_to = ColumnString::create();
        auto & data_to = col_to->getChars();
        auto & offsets_to = col_to->getOffsets();
        offsets_to.resize(rows);

        const auto & from = block.getByPosition(arguments[0]);
        TypeIndex from_type_index = from.type->getTypeId();
        switch (from_type_index)
        {
        case TypeIndex::Decimal32:
            doExecute<Decimal32>(data_to, offsets_to, from.column);
            break;
        case TypeIndex::Decimal64:
            doExecute<Decimal64>(data_to, offsets_to, from.column);
            break;
        case TypeIndex::Decimal128:
            doExecute<Decimal128>(data_to, offsets_to, from.column);
            break;
        case TypeIndex::Decimal256:
            doExecute<Decimal256>(data_to, offsets_to, from.column);
            break;
        default:
            throw Exception(
                fmt::format(
                    "Illegal type {} of argument of function {}",
                    magic_enum::enum_name(from_type_index),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        block.getByPosition(result).column = std::move(col_to);
    }

private:
    template <typename FromType>
    static void doExecute(
        ColumnString::Chars_t & data_to,
        ColumnString::Offsets & offsets_to,
        const ColumnPtr & column_ptr_from)
    {
        const auto * column_from = checkAndGetColumn<ColumnDecimal<FromType>>(column_ptr_from.get());
        RUNTIME_CHECK(column_from);
        // json_type + char 0 of string end + value
        size_t reserve_size = column_from->size() * (1 + 1 + sizeof(Float64));
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to, reserve_size);
        for (size_t i = 0; i < column_from->size(); ++i)
        {
            // same as https://github.com/pingcap/tidb/blob/90628349860718bb84c94fe7dc1e1f9bd9da4348/pkg/expression/builtin_cast.go#L854-L865
            // https://github.com/pingcap/tidb/issues/48796
            // TODO `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
            JsonBinary::appendNumber(
                write_buffer,
                static_cast<Float64>((*column_from)[i].template safeGet<DecimalField<FromType>>()));
            writeChar(0, write_buffer);
            offsets_to[i] = write_buffer.count();
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
                    doExecute<IntFieldType, UInt64>(data_to, offsets_to, from.column);
                else
                    doExecute<IntFieldType, Int64>(data_to, offsets_to, from.column);
            }
            else
            {
                doExecute<IntFieldType, bool>(data_to, offsets_to, from.column);
            }

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
        ColumnString::Chars_t & data_to,
        ColumnString::Offsets & offsets_to,
        const ColumnPtr & column_ptr_from)
    {
        const auto * column_from = checkAndGetColumn<ColumnVector<FromType>>(column_ptr_from.get());
        RUNTIME_CHECK(column_from);
        const auto & data_from = column_from->getData();

        // json_type + char 0 of string end + value
        size_t reserve_size = 0;
        if constexpr (std::is_same_v<bool, ToType>)
            reserve_size = data_from.size() * (1 + 1 + 1);
        else
            reserve_size = data_from.size() * (1 + 1 + sizeof(ToType));
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to, reserve_size);

        for (size_t i = 0; i < data_from.size(); ++i)
        {
            JsonBinary::appendNumber(write_buffer, static_cast<ToType>(data_from[i]));
            writeChar(0, write_buffer);
            offsets_to[i] = write_buffer.count();
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
        if (from.column->onlyNull())
        {
            block.getByPosition(result).column
                = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
            return;
        }

        auto nested_block = createBlockWithNestedColumns(block, arguments);
        auto input_source = createDynamicStringSource(*nested_block.getByPosition(arguments[0]).column);

        auto col_to = ColumnString::create();
        auto & data_to = col_to->getChars();
        auto & offsets_to = col_to->getOffsets();
        auto rows = block.rows();
        offsets_to.resize(rows);

        // In raw function test, input_tidb_tp/output_tidb_tp is nullptr.
        if (collator && collator->isBinary())
        {
            if (unlikely(input_tidb_tp == nullptr))
            {
                doExecuteForBinary<false, false>(
                    data_to,
                    offsets_to,
                    input_source,
                    {},
                    TiDB::TypeVarchar,
                    -1,
                    block.rows());
            }
            else if (input_tidb_tp->tp() == TiDB::TypeString)
            {
                if (from.column->isColumnNullable())
                {
                    const auto & column_nullable = static_cast<const ColumnNullable &>(*from.column);
                    doExecuteForBinary<true, true>(
                        data_to,
                        offsets_to,
                        input_source,
                        column_nullable.getNullMapData(),
                        input_tidb_tp->tp(),
                        input_tidb_tp->flen(),
                        block.rows());
                }
                else
                {
                    doExecuteForBinary<true, false>(
                        data_to,
                        offsets_to,
                        input_source,
                        {},
                        input_tidb_tp->tp(),
                        input_tidb_tp->flen(),
                        block.rows());
                }
            }
            else
            {
                doExecuteForBinary<false, false>(
                    data_to,
                    offsets_to,
                    input_source,
                    {},
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
                    data_to,
                    offsets_to,
                    input_source,
                    column_nullable.getNullMapData(),
                    block.rows());
            }
            else
            {
                doExecuteForParsingJson<false>(data_to, offsets_to, input_source, {}, block.rows());
            }
        }
        else
        {
            doExecuteForOthers(data_to, offsets_to, input_source, block.rows());
        }

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
    template <bool is_binary_str, bool nullable_input_for_binary_str>
    static void doExecuteForBinary(
        ColumnString::Chars_t & data_to,
        ColumnString::Offsets & offsets_to,
        const std::unique_ptr<IStringSource> & data_from,
        const NullMap & null_map_from,
        UInt8 from_type_code,
        Int32 flen,
        size_t size)
    {
        size_t reserve_size = 0;
        if constexpr (is_binary_str)
        {
            if (flen <= 0)
            {
                // json_type + from_type_code + size of data_from.
                reserve_size += (size * (1 + 1) + data_from->getSizeForReserve());
            }
            else
            {
                // for non-null value: char 0 of string end + json_type + from_type_code + flen.
                size_t size_of_non_null_value = (1 + 1 + 1 + flen);
                if constexpr (nullable_input_for_binary_str)
                {
                    auto null_count = countBytesInFilter(null_map_from.data(), null_map_from.size());
                    // for null value: char 0 of string end.
                    reserve_size += (null_count + (size - null_count) * size_of_non_null_value);
                }
                else
                {
                    reserve_size += (size * size_of_non_null_value);
                }
            }
        }
        else
        {
            // json_type + from_type_code + size of data_from.
            reserve_size += (size * (1 + 1) + data_from->getSizeForReserve());
        }
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to, reserve_size);
        ColumnString::Chars_t tmp_buf;
        for (size_t i = 0; i < size; ++i)
        {
            const auto & slice = data_from->getWhole();
            if constexpr (is_binary_str)
            {
                if constexpr (nullable_input_for_binary_str)
                {
                    if (null_map_from[i])
                    {
                        writeChar(0, write_buffer);
                        offsets_to[i] = write_buffer.count();
                        data_from->next();
                        continue;
                    }
                }
                if (unlikely(flen <= 0))
                {
                    JsonBinary::appendOpaque(
                        write_buffer,
                        JsonBinary::Opaque{from_type_code, StringRef{slice.data, slice.size}});
                }
                else
                {
                    auto size_t_flen = static_cast<size_t>(flen);
                    if (slice.size >= size_t_flen)
                    {
                        JsonBinary::appendOpaque(
                            write_buffer,
                            JsonBinary::Opaque{from_type_code, StringRef{slice.data, size_t_flen}});
                    }
                    else
                    {
                        if (tmp_buf.size() < size_t_flen)
                            tmp_buf.resize(size_t_flen);
                        std::memcpy(tmp_buf.data(), slice.data, slice.size);
                        std::fill(tmp_buf.data() + slice.size, tmp_buf.data() + size_t_flen, 0);
                        JsonBinary::appendOpaque(
                            write_buffer,
                            JsonBinary::Opaque{from_type_code, StringRef{tmp_buf.data(), size_t_flen}});
                    }
                }
            }
            else
            {
                JsonBinary::appendOpaque(
                    write_buffer,
                    JsonBinary::Opaque{from_type_code, StringRef{slice.data, slice.size}});
            }
            writeChar(0, write_buffer);
            offsets_to[i] = write_buffer.count();
            data_from->next();
        }
    }

    template <bool is_nullable>
    static void doExecuteForParsingJson(
        ColumnString::Chars_t & data_to,
        ColumnString::Offsets & offsets_to,
        const std::unique_ptr<IStringSource> & data_from,
        const NullMap & null_map_from,
        size_t size)
    {
        // json_type + size of data_from.
        size_t reserve_size = size + data_from->getSizeForReserve();
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to, reserve_size);
        simdjson::dom::parser parser;
        for (size_t i = 0; i < size; ++i)
        {
            if constexpr (is_nullable)
            {
                if (null_map_from[i])
                {
                    writeChar(0, write_buffer);
                    offsets_to[i] = write_buffer.count();
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
            JsonBinary::appendSIMDJsonElem(write_buffer, json_elem.value_unsafe());

            writeChar(0, write_buffer);
            offsets_to[i] = write_buffer.count();
            data_from->next();
        }
    }

    static void doExecuteForOthers(
        ColumnString::Chars_t & data_to,
        ColumnString::Offsets & offsets_to,
        const std::unique_ptr<IStringSource> & data_from,
        size_t size)
    {
        // json_type + size of data_from
        size_t reserve_size = size + data_from->getSizeForReserve();
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to, reserve_size);
        for (size_t i = 0; i < size; ++i)
        {
            const auto & slice = data_from->getWhole();
            JsonBinary::appendStringRef(write_buffer, StringRef{slice.data, slice.size});
            writeChar(0, write_buffer);
            offsets_to[i] = write_buffer.count();
            data_from->next();
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
        auto & offsets_to = col_to->getOffsets();
        auto rows = block.rows();
        offsets_to.resize(rows);

        const auto & from = block.getByPosition(arguments[0]);
        if (checkDataType<DataTypeMyDateTime>(from.type.get()))
        {
            // In raw function test, input_tidb_tp is nullptr.
            bool is_timestamp = (unlikely(input_tidb_tp == nullptr)) || input_tidb_tp->tp() == TiDB::TypeTimestamp;
            if (is_timestamp)
                doExecute<DataTypeMyDateTime, true>(data_to, offsets_to, from.column);
            else
                doExecute<DataTypeMyDateTime, false>(data_to, offsets_to, from.column);
        }
        else if (checkDataType<DataTypeMyDate>(from.type.get()))
        {
            doExecute<DataTypeMyDate, false>(data_to, offsets_to, from.column);
        }

        block.getByPosition(result).column = std::move(col_to);
    }

private:
    template <typename FromDataType, bool is_timestamp>
    static void doExecute(
        ColumnString::Chars_t & data_to,
        ColumnString::Offsets & offsets_to,
        const ColumnPtr & column_ptr_from)
    {
        const auto * column_from
            = checkAndGetColumn<ColumnVector<typename FromDataType::FieldType>>(column_ptr_from.get());
        RUNTIME_CHECK(column_from);
        const auto & data_from = column_from->getData();
        // json_type + char 0 of string end + value
        size_t reserve_size = data_from.size() * (1 + 1 + sizeof(UInt64));
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to, reserve_size);
        for (size_t i = 0; i < data_from.size(); ++i)
        {
            if constexpr (std::is_same_v<DataTypeMyDate, FromDataType>)
            {
                MyDate date(data_from[i]);
                JsonBinary::appendDate(write_buffer, date);
            }
            else
            {
                MyDateTime date_time(data_from[i]);
                if constexpr (is_timestamp)
                    JsonBinary::appendTimestamp(write_buffer, date_time);
                else
                    JsonBinary::appendDatetime(write_buffer, date_time);
            }

            writeChar(0, write_buffer);
            offsets_to[i] = write_buffer.count();
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
        auto & offsets_to = col_to->getOffsets();
        auto rows = block.rows();
        offsets_to.resize(rows);

        const auto & from = block.getByPosition(arguments[0]);
        if (likely(checkDataType<DataTypeMyDuration>(from.type.get())))
        {
            const auto & col_from = checkAndGetColumn<ColumnVector<DataTypeMyDuration::FieldType>>(from.column.get());
            const auto & data_from = col_from->getData();
            // json_type + char 0 of string end + value
            size_t reserve_size = data_from.size() * (1 + 1 + sizeof(UInt64) + sizeof(UInt32));
            JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to, reserve_size);
            for (size_t i = 0; i < data_from.size(); ++i)
            {
                // from https://github.com/pingcap/tidb/blob/3543275dcf4b6454eb874c1362c87d31a963da6d/pkg/expression/builtin_cast.go#L921
                // fsp always is MaxFsp.
                JsonBinary::appendDuration(write_buffer, data_from[i], 6);
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

        block.getByPosition(result).column = std::move(col_to);
    }
};

class FunctionJsonDepth : public IFunction
{
public:
    static constexpr auto name = "json_depth";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonDepth>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!arguments[0]->isString())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                arguments[0]->getName(),
                getName());
        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto & from = block.getByPosition(arguments[0]);
        if (const auto * col_from = checkAndGetColumn<ColumnString>(from.column.get()))
        {
            const auto & data = col_from->getChars();
            const auto & offsets = col_from->getOffsets();
            const size_t size = offsets.size();

            auto col_res = ColumnUInt64::create();
            auto & vec_col_res = col_res->getData();
            vec_col_res.resize(size);

            ColumnString::Offset prev_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                size_t data_length = offsets[i] - prev_offset - 1;
                if (isNullJsonBinary(data_length))
                {
                    vec_col_res[i] = 0;
                }
                else
                {
                    JsonBinary json_binary(data[prev_offset], StringRef(&data[prev_offset + 1], data_length - 1));
                    vec_col_res[i] = json_binary.getDepth();
                }
                prev_offset = offsets[i];
            }

            block.getByPosition(result).column = std::move(col_res);
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                from.type->getName(),
                getName());
        }
    }
};

class FunctionJsonContainsPath : public IFunction
{
private:
    enum class ContainsType
    {
        ALL,
        ONE,
    };

public:
    static constexpr auto name = "json_contains_path";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonContainsPath>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (arguments.size() < 3)
        {
            throw Exception(
                fmt::format("Illegal arguments count {} of function {}", arguments.size(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        for (const auto & arg : arguments)
        {
            if unlikely (!arg->onlyNull() && !removeNullable(arg)->isString())
            {
                throw Exception(
                    fmt::format("Illegal type {} of argument of function {}", arg->getName(), getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
        if (arguments[0]->onlyNull() || arguments[1]->onlyNull() || arguments[2]->onlyNull())
            return makeNullable(std::make_shared<DataTypeNothing>());
        else
        {
            auto return_type = std::make_shared<DataTypeUInt8>();
            for (const auto & arg : arguments)
            {
                if (arg->onlyNull() || arg->isNullable())
                    return makeNullable(return_type);
            }
            return return_type;
        }
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto & json_col = block.getByPosition(arguments[0]).column;
        const auto & type_col = block.getByPosition(arguments[1]).column;
        if (json_col->onlyNull() || type_col->onlyNull() || block.getByPosition(arguments[2]).column->onlyNull())
        {
            block.getByPosition(result).column
                = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
            return;
        }

        auto nested_block = createBlockWithNestedColumns(block, arguments);
        auto json_source = createDynamicStringSource(*nested_block.getByPosition(arguments[0]).column);
        auto type_source = createDynamicStringSource(*nested_block.getByPosition(arguments[1]).column);

        auto & result_col = block.getByPosition(result);
        assert(!result_col.type->onlyNull());

        size_t rows = block.rows();
        auto col_to = ColumnUInt8::create(rows, 1);
        auto & data_to = col_to->getData();
        auto col_null_map = result_col.type->isNullable() ? ColumnUInt8::create(rows, 0) : ColumnUInt8::create(0, 0);
        auto & vec_null_map = col_null_map->getData();

        StringSources path_sources;
        path_sources.reserve(arguments.size() - 2);
        std::vector<const NullMap *> path_null_maps;
        path_null_maps.reserve(arguments.size() - 2);
        bool is_all_path_const = true;
        for (size_t i = 2; i < arguments.size(); ++i)
        {
            const auto & path_col = block.getByPosition(arguments[i]).column;
            if (path_col->onlyNull())
            {
                path_sources.push_back(nullptr);
                path_null_maps.push_back(nullptr);
            }
            else if (path_col->isColumnNullable())
            {
                path_sources.push_back(createDynamicStringSource(*nested_block.getByPosition(arguments[i]).column));
                const auto & path_column_nullable = static_cast<const ColumnNullable &>(*path_col);
                path_null_maps.push_back(&path_column_nullable.getNullMapData());
            }
            else
            {
                path_sources.push_back(createDynamicStringSource(*nested_block.getByPosition(arguments[i]).column));
                path_null_maps.push_back(nullptr);
            }
            is_all_path_const = is_all_path_const && path_col->isColumnConst();
        }

        if (json_col->isColumnNullable())
        {
            const auto & json_column_nullable = static_cast<const ColumnNullable &>(*json_col);
            if (type_col->isColumnNullable())
            {
                const auto & type_column_nullable = static_cast<const ColumnNullable &>(*type_col);
                doExecuteCommon<true, true>(
                    json_source,
                    json_column_nullable.getNullMapData(),
                    type_source,
                    type_column_nullable.getNullMapData(),
                    path_sources,
                    path_null_maps,
                    rows,
                    data_to,
                    vec_null_map);
            }
            else if (type_col->isColumnConst() && is_all_path_const)
            {
                doExecuteForTypeAndPathConst<true>(
                    json_source,
                    json_column_nullable.getNullMapData(),
                    type_source,
                    path_sources,
                    rows,
                    data_to,
                    vec_null_map);
            }
            else
            {
                doExecuteCommon<true, false>(
                    json_source,
                    json_column_nullable.getNullMapData(),
                    type_source,
                    {},
                    path_sources,
                    path_null_maps,
                    rows,
                    data_to,
                    vec_null_map);
            }
        }
        else
        {
            if (type_col->isColumnNullable())
            {
                const auto & type_column_nullable = static_cast<const ColumnNullable &>(*type_col);
                doExecuteCommon<false, true>(
                    json_source,
                    {},
                    type_source,
                    type_column_nullable.getNullMapData(),
                    path_sources,
                    path_null_maps,
                    rows,
                    data_to,
                    vec_null_map);
            }
            else if (type_col->isColumnConst() && is_all_path_const)
            {
                doExecuteForTypeAndPathConst<false>(
                    json_source,
                    {},
                    type_source,
                    path_sources,
                    rows,
                    data_to,
                    vec_null_map);
            }
            else
            {
                doExecuteCommon<false, false>(
                    json_source,
                    {},
                    type_source,
                    {},
                    path_sources,
                    path_null_maps,
                    rows,
                    data_to,
                    vec_null_map);
            }
        }

        if (result_col.type->isNullable())
            result_col.column = ColumnNullable::create(std::move(col_to), std::move(col_null_map));
        else
            result_col.column = std::move(col_to);
    }

private:
    template <bool is_json_nullable, bool is_type_nullable>
    void doExecuteCommon(
        const std::unique_ptr<IStringSource> & json_source,
        const NullMap & null_map_json,
        const std::unique_ptr<IStringSource> & type_source,
        const NullMap & null_map_type,
        const StringSources & path_sources,
        const std::vector<const NullMap *> & path_null_maps,
        size_t rows,
        ColumnUInt8::Container & data_to,
        NullMap & null_map_to) const
    {
#define FINISH_PER_ROW                            \
    for (const auto & path_source : path_sources) \
    {                                             \
        if (path_source)                          \
            path_source->next();                  \
    }                                             \
    json_source->next();                          \
    type_source->next();

        for (size_t row = 0; row < rows; ++row)
        {
            if constexpr (is_json_nullable)
            {
                if (null_map_json[row])
                {
                    FINISH_PER_ROW
                    null_map_to[row] = 1;
                    continue;
                }
            }
            if constexpr (is_type_nullable)
            {
                if (null_map_type[row])
                {
                    FINISH_PER_ROW
                    null_map_to[row] = 1;
                    continue;
                }
            }

            const auto & json_val = json_source->getWhole();
            JsonBinary json_binary{json_val.data[0], StringRef{&json_val.data[1], json_val.size - 1}};

            auto contains_type = getTypeVal(type_source->getWhole());

            auto & res = data_to[row]; // default 1.
            for (size_t i = 0; i < path_sources.size(); ++i)
            {
                if (!path_sources[i] || (path_null_maps[i] && (*path_null_maps[i])[row]))
                {
                    null_map_to[row] = 1;
                    break;
                }

                assert(path_sources[i]);
                auto path_expr_container_vec = buildJsonPathExprContainer(path_sources[i]->getWhole());
                bool exists = !json_binary.extract(path_expr_container_vec).empty();
                if (contains_type == ContainsType::ONE)
                {
                    if (exists)
                    {
                        res = 1;
                        break;
                    }
                    else
                    {
                        res = 0;
                    }
                }
                else // contains_type == ContainsType::ALL
                {
                    if (!exists)
                    {
                        res = 0;
                        break;
                    }
                }
            }

            FINISH_PER_ROW
        }

#undef FINISH_PER_ROW
    }

    template <bool is_json_nullable>
    void doExecuteForTypeAndPathConst(
        const std::unique_ptr<IStringSource> & json_source,
        const NullMap & null_map_json,
        const std::unique_ptr<IStringSource> & type_source,
        const StringSources & path_sources,
        size_t rows,
        ColumnUInt8::Container & data_to,
        NullMap & null_map_to) const
    {
        // build contains_type for type const col first.
        auto contains_type = getTypeVal(type_source->getWhole());

        // build path exprs for path const cols next.
        std::vector<std::vector<JsonPathExprRefContainerPtr>> path_expr_container_vecs;
        path_expr_container_vecs.reserve(path_sources.size());
        bool has_null_path = false;
        for (const auto & path_source : path_sources)
        {
            if (!path_source) // only null const
            {
                has_null_path = true;
                path_expr_container_vecs.push_back({});
            }
            else
            {
                path_expr_container_vecs.push_back(buildJsonPathExprContainer(path_source->getWhole()));
            }
        }
        assert(path_sources.size() == path_expr_container_vecs.size());

        if (contains_type == ContainsType::ONE)
        {
            if (has_null_path)
                doExecuteForTypeAndPathConstImpl<is_json_nullable, true, true>(
                    json_source,
                    null_map_json,
                    path_expr_container_vecs,
                    rows,
                    data_to,
                    null_map_to);
            else
                doExecuteForTypeAndPathConstImpl<is_json_nullable, false, true>(
                    json_source,
                    null_map_json,
                    path_expr_container_vecs,
                    rows,
                    data_to,
                    null_map_to);
        }
        else
        {
            if (has_null_path)
                doExecuteForTypeAndPathConstImpl<is_json_nullable, true, false>(
                    json_source,
                    null_map_json,
                    path_expr_container_vecs,
                    rows,
                    data_to,
                    null_map_to);
            else
                doExecuteForTypeAndPathConstImpl<is_json_nullable, false, false>(
                    json_source,
                    null_map_json,
                    path_expr_container_vecs,
                    rows,
                    data_to,
                    null_map_to);
        }
    }

    template <bool is_json_nullable, bool has_null_path, bool is_contains_one>
    void doExecuteForTypeAndPathConstImpl(
        const std::unique_ptr<IStringSource> & json_source,
        const NullMap & null_map_json,
        const std::vector<std::vector<JsonPathExprRefContainerPtr>> & path_expr_container_vecs,
        size_t rows,
        ColumnUInt8::Container & data_to,
        NullMap & null_map_to) const
    {
        for (size_t row = 0; row < rows; ++row)
        {
            if constexpr (is_json_nullable)
            {
                if (null_map_json[row])
                {
                    json_source->next();
                    null_map_to[row] = 1;
                    continue;
                }
            }

            const auto & json_val = json_source->getWhole();
            JsonBinary json_binary{json_val.data[0], StringRef{&json_val.data[1], json_val.size - 1}};

            auto & res = data_to[row]; // default 1.
            for (const auto & path_expr_container_vec : path_expr_container_vecs)
            {
                if constexpr (has_null_path)
                {
                    if (path_expr_container_vec.empty())
                    {
                        null_map_to[row] = 1;
                        break;
                    }
                }

                assert(!path_expr_container_vec.empty());
                bool exists = !json_binary.extract(path_expr_container_vec).empty();
                if constexpr (is_contains_one)
                {
                    if (exists)
                    {
                        res = 1;
                        break;
                    }
                    else
                    {
                        res = 0;
                    }
                }
                else // is_contains_all
                {
                    if (!exists)
                    {
                        res = 0;
                        break;
                    }
                }
            }

            json_source->next();
        }
    }

    ContainsType getTypeVal(const IStringSource::Slice & type_val) const
    {
        std::string_view type{reinterpret_cast<const char *>(type_val.data), type_val.size};
        if unlikely (type.size() != 3)
            throw Exception(
                fmt::format("The second argument can only be either 'one' or 'all' of function {}.", getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        auto first_char = std::tolower(type[0]);
        if (first_char == 'a')
        {
            if likely (std::tolower(type[1]) == 'l' && std::tolower(type[2]) == 'l')
                return ContainsType::ALL;
        }
        else if (first_char == 'o')
        {
            if likely (std::tolower(type[1]) == 'n' && std::tolower(type[2]) == 'e')
                return ContainsType::ONE;
        }
        throw Exception(
            fmt::format("The second argument can only be either 'one' or 'all' of function {}.", getName()),
            ErrorCodes::ILLEGAL_COLUMN);
    }

    std::vector<JsonPathExprRefContainerPtr> buildJsonPathExprContainer(const IStringSource::Slice & path_val) const
    {
        auto path_expr = JsonPathExpr::parseJsonPathExpr(StringRef{path_val.data, path_val.size});
        /// If path_expr failed to parse, throw exception
        if unlikely (!path_expr)
            throw Exception(
                fmt::format("Illegal json path expression of function {}", getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        std::vector<JsonPathExprRefContainerPtr> path_expr_container_vec;
        path_expr_container_vec.push_back(std::make_unique<JsonPathExprRefContainer>(path_expr));
        return path_expr_container_vec;
    }
};

class FunctionJsonValidOthers : public IFunction
{
public:
    static constexpr auto name = "json_valid_others";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonValidOthers>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    // Although it is stated in https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid that returns NULL if the argument is NULL,
    // both MySQL and TiDB will directly return false instead of NULL.
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & /*arguments*/, size_t result) const override
    {
        block.getByPosition(result).column = ColumnConst::create(ColumnVector<UInt8>::create(1, 0), block.rows());
    }
};

class FunctionJsonValidJson : public IFunction
{
public:
    static constexpr auto name = "json_valid_json";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonValidJson>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!arguments[0]->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & /*arguments*/, size_t result) const override
    {
        block.getByPosition(result).column = ColumnConst::create(ColumnVector<UInt8>::create(1, 1), block.rows());
    }
};

class FunctionJsonValidString : public IFunction
{
public:
    static constexpr auto name = "json_valid_string";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonValidString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!arguments[0]->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;
        size_t rows = block.rows();
        if (const auto * col_from = checkAndGetColumn<ColumnString>(column.get()))
        {
            const auto & data_from = col_from->getChars();
            const auto & offsets_from = col_from->getOffsets();

            auto col_to = ColumnVector<UInt8>::create(rows, 0);
            auto & data_to = col_to->getData();

            size_t current_offset = 0;
            for (size_t i = 0; i < rows; ++i)
            {
                size_t next_offset = offsets_from[i];
                size_t data_length = next_offset - current_offset - 1;
                bool is_valid = checkJsonValid(reinterpret_cast<const char *>(&data_from[current_offset]), data_length);
                data_to[i] = is_valid ? 1 : 0;
                current_offset = next_offset;
            }

            block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

class FunctionJsonKeys : public IFunction
{
public:
    static constexpr auto name = "json_keys";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonKeys>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (!arguments[0]->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return makeNullable(std::make_shared<DataTypeString>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;
        size_t rows = block.rows();
        if (const auto * col_from = checkAndGetColumn<ColumnString>(column.get()))
        {
            const auto & data_from = col_from->getChars();
            const auto & offsets_from = col_from->getOffsets();

            auto col_to = ColumnString::create();
            auto & data_to = col_to->getChars();
            auto & offsets_to = col_to->getOffsets();
            offsets_to.resize(rows);
            ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(rows, 0);
            ColumnUInt8::Container & vec_null_map = col_null_map->getData();

            {
                // reserve only for the char 0 of string end.
                // Keys are typically small, so space is not reserved specifically for them. Instead, buffer is used to expand capacity.
                JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to, rows);
                ColumnString::Offset prev_offset = 0;
                for (size_t i = 0; i < rows; ++i)
                {
                    size_t data_length = offsets_from[i] - prev_offset - 1;
                    if (!isNullJsonBinary(data_length))
                    {
                        JsonBinary json_binary(
                            data_from[prev_offset],
                            StringRef(&data_from[prev_offset + 1], data_length - 1));
                        if (json_binary.getType() != JsonBinary::TYPE_CODE_OBJECT)
                        {
                            vec_null_map[i] = 1;
                        }
                        else
                        {
                            auto keys = json_binary.getKeys();
                            JsonBinary::buildKeyArrayInBuffer(keys, write_buffer);
                        }
                    }
                    prev_offset = offsets_from[i];
                    writeChar(0, write_buffer);
                    offsets_to[i] = write_buffer.count();
                }
            }

            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map));
        }
        else
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

class FunctionJsonKeys2Args : public IFunction
{
public:
    static constexpr auto name = "json_keys_2_args";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonKeys2Args>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->onlyNull() || arguments[1]->onlyNull())
            return makeNullable(std::make_shared<DataTypeNothing>());

        if unlikely (!removeNullable(arguments[0])->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if unlikely (!removeNullable(arguments[1])->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return makeNullable(std::make_shared<DataTypeString>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto & json_col = block.getByPosition(arguments[0]).column;
        const auto & path_col = block.getByPosition(arguments[1]).column;
        if (json_col->onlyNull() || path_col->onlyNull())
        {
            block.getByPosition(result).column
                = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
            return;
        }

        auto nested_block = createBlockWithNestedColumns(block, arguments);
        auto json_source = createDynamicStringSource(*nested_block.getByPosition(arguments[0]).column);
        auto path_source = createDynamicStringSource(*nested_block.getByPosition(arguments[1]).column);

        auto col_to = ColumnString::create();
        auto & data_to = col_to->getChars();
        auto & offsets_to = col_to->getOffsets();
        size_t rows = block.rows();
        offsets_to.resize(rows);
        auto col_null_map = ColumnUInt8::create(rows, 0);
        auto & vec_null_map = col_null_map->getData();

        if (json_col->isColumnNullable())
        {
            const auto & json_column_nullable = static_cast<const ColumnNullable &>(*json_col);
            if (path_col->isColumnNullable())
            {
                const auto & path_column_nullable = static_cast<const ColumnNullable &>(*path_col);
                doExecuteCommon<true, true>(
                    json_source,
                    json_column_nullable.getNullMapData(),
                    path_source,
                    path_column_nullable.getNullMapData(),
                    rows,
                    data_to,
                    offsets_to,
                    vec_null_map);
            }
            else if (path_col->isColumnConst())
            {
                doExecuteForConstPath<true>(
                    json_source,
                    json_column_nullable.getNullMapData(),
                    path_source,
                    rows,
                    data_to,
                    offsets_to,
                    vec_null_map);
            }
            else
            {
                doExecuteCommon<true, false>(
                    json_source,
                    json_column_nullable.getNullMapData(),
                    path_source,
                    {},
                    rows,
                    data_to,
                    offsets_to,
                    vec_null_map);
            }
        }
        else
        {
            if (path_col->isColumnNullable())
            {
                const auto & path_column_nullable = static_cast<const ColumnNullable &>(*path_col);
                doExecuteCommon<false, true>(
                    json_source,
                    {},
                    path_source,
                    path_column_nullable.getNullMapData(),
                    rows,
                    data_to,
                    offsets_to,
                    vec_null_map);
            }
            else if (path_col->isColumnConst())
            {
                doExecuteForConstPath<false>(json_source, {}, path_source, rows, data_to, offsets_to, vec_null_map);
            }
            else
            {
                doExecuteCommon<false, false>(
                    json_source,
                    {},
                    path_source,
                    {},
                    rows,
                    data_to,
                    offsets_to,
                    vec_null_map);
            }
        }

        block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map));
    }

private:
    template <bool is_json_nullable, bool is_path_nullable>
    static void doExecuteCommon(
        const std::unique_ptr<IStringSource> & json_source,
        const NullMap & null_map_json,
        const std::unique_ptr<IStringSource> & path_source,
        const NullMap & null_map_path,
        size_t rows,
        ColumnString::Chars_t & data_to,
        ColumnString::Offsets & offsets_to,
        NullMap & null_map_to)
    {
#define SET_NULL_AND_CONTINUE             \
    null_map_to[i] = 1;                   \
    writeChar(0, write_buffer);           \
    offsets_to[i] = write_buffer.count(); \
    json_source->next();                  \
    path_source->next();                  \
    continue;

        // reserve only for the char 0 of string end.
        // Keys are typically small, so space is not reserved specifically for them. Instead, buffer is used to expand capacity.
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to, rows);
        for (size_t i = 0; i < rows; ++i)
        {
            if constexpr (is_json_nullable)
            {
                if (null_map_json[i])
                {
                    SET_NULL_AND_CONTINUE
                }
            }
            if constexpr (is_path_nullable)
            {
                if (null_map_path[i])
                {
                    SET_NULL_AND_CONTINUE
                }
            }

            const auto & path_val = path_source->getWhole();
            auto path_expr_container_vec = buildPathExprContainer(StringRef{path_val.data, path_val.size});

            const auto & json_val = json_source->getWhole();
            assert(json_val.size > 0);
            JsonBinary json_binary{json_val.data[0], StringRef{&json_val.data[1], json_val.size - 1}};
            if (json_binary.getType() != JsonBinary::TYPE_CODE_OBJECT)
            {
                SET_NULL_AND_CONTINUE
            }

            auto extract_json_binaries = json_binary.extract(path_expr_container_vec);
            if (extract_json_binaries.empty() || extract_json_binaries[0].getType() != JsonBinary::TYPE_CODE_OBJECT)
            {
                SET_NULL_AND_CONTINUE
            }

            auto keys = extract_json_binaries[0].getKeys();
            JsonBinary::buildKeyArrayInBuffer(keys, write_buffer);

            writeChar(0, write_buffer);
            offsets_to[i] = write_buffer.count();
            json_source->next();
            path_source->next();
        }

#undef SET_NULL_AND_CONTINUE
    }

    template <bool is_json_nullable>
    static void doExecuteForConstPath(
        const std::unique_ptr<IStringSource> & json_source,
        const NullMap & null_map_json,
        const std::unique_ptr<IStringSource> & path_source,
        size_t rows,
        ColumnString::Chars_t & data_to,
        ColumnString::Offsets & offsets_to,
        NullMap & null_map_to)
    {
        // build path expr for const path col first.
        const auto & path_val = path_source->getWhole();
        auto path_expr_container_vec = buildPathExprContainer(StringRef{path_val.data, path_val.size});

#define SET_NULL_AND_CONTINUE             \
    null_map_to[i] = 1;                   \
    writeChar(0, write_buffer);           \
    offsets_to[i] = write_buffer.count(); \
    json_source->next();                  \
    continue;

        // reserve only for the char 0 of string end.
        // Keys are typically small, so space is not reserved specifically for them. Instead, buffer is used to expand capacity.
        JsonBinary::JsonBinaryWriteBuffer write_buffer(data_to, rows);
        for (size_t i = 0; i < rows; ++i)
        {
            if constexpr (is_json_nullable)
            {
                if (null_map_json[i])
                {
                    SET_NULL_AND_CONTINUE
                }
            }

            const auto & json_val = json_source->getWhole();
            assert(json_val.size > 0);
            JsonBinary json_binary{json_val.data[0], StringRef{&json_val.data[1], json_val.size - 1}};
            if (json_binary.getType() != JsonBinary::TYPE_CODE_OBJECT)
            {
                SET_NULL_AND_CONTINUE
            }

            auto extract_json_binaries = json_binary.extract(path_expr_container_vec);
            if (extract_json_binaries.empty() || extract_json_binaries[0].getType() != JsonBinary::TYPE_CODE_OBJECT)
            {
                SET_NULL_AND_CONTINUE
            }

            auto keys = extract_json_binaries[0].getKeys();
            JsonBinary::buildKeyArrayInBuffer(keys, write_buffer);

            writeChar(0, write_buffer);
            offsets_to[i] = write_buffer.count();
            json_source->next();
        }

#undef SET_NULL_AND_CONTINUE
    }
};
} // namespace DB
