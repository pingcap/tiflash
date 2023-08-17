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

#include <Columns/ColumnConst.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <Interpreters/castColumn.h>
#include <Parsers/IAST.h>


namespace DB
{

namespace ErrorCodes
{
extern const int THERE_IS_NO_COLUMN;
extern const int BLOCKS_HAVE_DIFFERENT_STRUCTURE;
extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
} // namespace ErrorCodes


ConvertingBlockInputStream::ConvertingBlockInputStream(
    const Context & context_,
    const BlockInputStreamPtr & input,
    const Block & result_header,
    MatchColumnsMode mode)
    : context(context_)
    , header(result_header)
    , conversion(header.columns())
{
    children.emplace_back(input);

    Block input_header = input->getHeader();

    size_t num_input_columns = input_header.columns();
    size_t num_result_columns = result_header.columns();

    if (mode == MatchColumnsMode::Position && num_input_columns != num_result_columns)
        throw Exception("Number of columns doesn't match", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

    for (size_t result_col_num = 0; result_col_num < num_result_columns; ++result_col_num)
    {
        const auto & res_elem = result_header.getByPosition(result_col_num);

        switch (mode)
        {
        case MatchColumnsMode::Position:
            conversion[result_col_num] = result_col_num;
            break;

        case MatchColumnsMode::Name:
            if (input_header.has(res_elem.name))
                conversion[result_col_num] = input_header.getPositionByName(res_elem.name);
            else
                throw Exception(
                    "Cannot find column " + backQuoteIfNeed(res_elem.name) + " in source stream",
                    ErrorCodes::THERE_IS_NO_COLUMN);
            break;
        }

        const auto & src_elem = input_header.getByPosition(conversion[result_col_num]);

        /// Check constants.

        if (res_elem.column->isColumnConst())
        {
            if (!src_elem.column->isColumnConst())
                throw Exception(
                    "Cannot convert column " + backQuoteIfNeed(res_elem.name)
                        + " because it is non constant in source stream but must be constant in result",
                    ErrorCodes::BLOCKS_HAVE_DIFFERENT_STRUCTURE);
            else if (
                static_cast<const ColumnConst &>(*src_elem.column).getField()
                != static_cast<const ColumnConst &>(*res_elem.column).getField())
                throw Exception(
                    "Cannot convert column " + backQuoteIfNeed(res_elem.name)
                        + " because it is constant but values of constants are different in source and result",
                    ErrorCodes::BLOCKS_HAVE_DIFFERENT_STRUCTURE);
        }

        /// Check conversion by dry run CAST function.

        castColumn(src_elem, res_elem.type, context);
    }
}


Block ConvertingBlockInputStream::readImpl()
{
    Block src = children.back()->read();

    if (!src)
        return src;

    Block res = header.cloneEmpty();
    for (size_t res_pos = 0, size = conversion.size(); res_pos < size; ++res_pos)
    {
        const auto & src_elem = src.getByPosition(conversion[res_pos]);
        auto & res_elem = res.getByPosition(res_pos);

        ColumnPtr converted = castColumn(src_elem, res_elem.type, context);

        if (src_elem.column->isColumnConst() && !res_elem.column->isColumnConst())
            converted = converted->convertToFullColumnIfConst();

        res_elem.column = std::move(converted);
    }
    return res;
}

} // namespace DB
