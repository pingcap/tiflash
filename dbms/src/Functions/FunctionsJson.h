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
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <TiDB/Decode/JsonBinary.h>
#include <TiDB/Decode/JsonPathExprRef.h>

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

class FunctionsCastJsonAsString : public IFunction
{
public:
    static constexpr auto name = "cast_json_as_string";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsCastJsonAsString>(); }

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

extern UInt64 GetJsonLength(const std::string_view & sv);

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
                    vec_col_res[i] = GetJsonLength(sv);
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

} // namespace DB