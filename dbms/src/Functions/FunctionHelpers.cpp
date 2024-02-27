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

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

const ColumnConst * checkAndGetColumnConstStringOrFixedString(const IColumn * column)
{
    if (!column->isColumnConst())
        return {};

    const auto * res = static_cast<const ColumnConst *>(column);

    if (checkColumn<ColumnString>(&res->getDataColumn()) || checkColumn<ColumnFixedString>(&res->getDataColumn()))
        return res;

    return {};
}


Columns convertConstTupleToConstantElements(const ColumnConst & column)
{
    const auto & src_tuple = static_cast<const ColumnTuple &>(column.getDataColumn());
    const Columns & src_tuple_columns = src_tuple.getColumns();
    size_t tuple_size = src_tuple_columns.size();
    size_t rows = column.size();

    Columns res(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        res[i] = ColumnConst::create(src_tuple_columns[i], rows);

    return res;
}


static Block createBlockWithNestedColumnsImpl(const Block & block, const std::unordered_set<size_t> & args)
{
    Block res;
    size_t rows = block.rows();
    size_t columns = block.columns();

    for (size_t i = 0; i < columns; ++i)
    {
        const auto & col = block.getByPosition(i);

        if (args.contains(i) && col.type->isNullable())
        {
            const DataTypePtr & nested_type = static_cast<const DataTypeNullable &>(*col.type).getNestedType();

            if (!col.column)
            {
                res.insert({nullptr, nested_type, col.name});
            }
            else if (col.column->isColumnNullable())
            {
                const auto & nested_col = static_cast<const ColumnNullable &>(*col.column).getNestedColumnPtr();

                res.insert({nested_col, nested_type, col.name});
            }
            else if (col.column->isColumnConst())
            {
                const auto & nested_col
                    = static_cast<const ColumnNullable &>(static_cast<const ColumnConst &>(*col.column).getDataColumn())
                          .getNestedColumnPtr();

                res.insert({ColumnConst::create(nested_col, rows), nested_type, col.name});
            }
            else
                throw Exception(
                    "Illegal column for DataTypeNullable:" + col.type->getName() + " [column_name=" + col.name
                        + "] [created=" + DB::toString(col.column != nullptr)
                        + "] [nullable=" + (col.column ? DB::toString(col.column->isColumnNullable()) : "null")
                        + "] [const=" + (col.column ? DB::toString(col.column->isColumnConst()) : "null") + "]",
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else
            res.insert(col);
    }

    return res;
}


Block createBlockWithNestedColumns(const Block & block, const ColumnNumbers & args)
{
    std::unordered_set<size_t> args_set(args.begin(), args.end());
    return createBlockWithNestedColumnsImpl(block, args_set);
}

Block createBlockWithNestedColumns(const Block & block, const ColumnNumbers & args, size_t result)
{
    std::unordered_set<size_t> args_set(args.begin(), args.end());
    args_set.insert(result);
    return createBlockWithNestedColumnsImpl(block, args_set);
}

bool functionIsInOperator(const String & name)
{
    return name == "in" || name == "notIn" || name == "tidbIn" || name == "tidbNotIn";
}

bool functionIsInOrGlobalInOperator(const String & name)
{
    return name == "in" || name == "notIn" || name == "globalIn" || name == "globalNotIn" || name == "tidbIn"
        || name == "tidbNotIn";
}


} // namespace DB
