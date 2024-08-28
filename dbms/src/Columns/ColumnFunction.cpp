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

#include <Columns/ColumnFunction.h>
#include <Columns/ColumnsCommon.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <fmt/format.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

ColumnFunction::ColumnFunction(size_t size, FunctionBasePtr function, const ColumnsWithTypeAndName & columns_to_capture)
    : column_size(size)
    , function(function)
{
    appendArguments(columns_to_capture);
}

MutableColumnPtr ColumnFunction::cloneResized(size_t size) const
{
    ColumnsWithTypeAndName capture = captured_columns;
    for (auto & column : capture)
        column.column = column.column->cloneResized(size);

    return ColumnFunction::create(size, function, capture);
}

ColumnPtr ColumnFunction::replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & offsets) const
{
    if (column_size != offsets.size())
        throw Exception(
            fmt::format("Size of offsets ({}) doesn't match size of column ({})", offsets.size(), column_size),
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    assert(start_row < end_row);
    assert(end_row <= column_size);

    ColumnsWithTypeAndName capture = captured_columns;
    for (auto & column : capture)
        column.column = column.column->replicateRange(start_row, end_row, offsets);

    size_t replicated_size = 0 == column_size ? 0 : (offsets[end_row - 1]);
    return ColumnFunction::create(replicated_size, function, capture);
}

ColumnPtr ColumnFunction::cut(size_t start, size_t length) const
{
    ColumnsWithTypeAndName capture = captured_columns;
    for (auto & column : capture)
        column.column = column.column->cut(start, length);

    return ColumnFunction::create(length, function, capture);
}

ColumnPtr ColumnFunction::filter(const Filter & filter, ssize_t result_size_hint) const
{
    if (column_size != filter.size())
        throw Exception(
            fmt::format("Size of filter ({}) doesn't match size of column ({})", filter.size(), column_size),
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    ColumnsWithTypeAndName capture = captured_columns;
    for (auto & column : capture)
        column.column = column.column->filter(filter, result_size_hint);

    size_t filtered_size = 0;
    if (capture.empty())
        filtered_size = countBytesInFilter(filter);
    else
        filtered_size = capture.front().column->size();

    return ColumnFunction::create(filtered_size, function, capture);
}

ColumnPtr ColumnFunction::permute(const Permutation & perm, size_t limit) const
{
    if (limit == 0)
        limit = column_size;
    else
        limit = std::min(column_size, limit);

    if (perm.size() < limit)
        throw Exception(
            fmt::format("Size of permutation ({}) is less than required ({})", perm.size(), limit),
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    ColumnsWithTypeAndName capture = captured_columns;
    for (auto & column : capture)
        column.column = column.column->permute(perm, limit);

    return ColumnFunction::create(limit, function, capture);
}

std::vector<MutableColumnPtr> ColumnFunction::scatter(
    IColumn::ColumnIndex num_columns,
    const IColumn::Selector & selector) const
{
    if (column_size != selector.size())
        throw Exception(
            fmt::format("Size of selector ({}) doesn't match size of column ({})", selector.size(), column_size),
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    std::vector<size_t> counts;
    if (captured_columns.empty())
        counts = countColumnsSizeInSelector(num_columns, selector);

    std::vector<ColumnsWithTypeAndName> captures(num_columns, captured_columns);

    for (size_t capture = 0; capture < captured_columns.size(); ++capture)
    {
        auto parts = captured_columns[capture].column->scatter(num_columns, selector);
        for (IColumn::ColumnIndex part = 0; part < num_columns; ++part)
            captures[part][capture].column = std::move(parts[part]);
    }

    std::vector<MutableColumnPtr> columns;
    columns.reserve(num_columns);
    for (IColumn::ColumnIndex part = 0; part < num_columns; ++part)
    {
        auto & capture = captures[part];
        size_t s = capture.empty() ? counts[part] : capture.front().column->size();
        columns.emplace_back(ColumnFunction::create(s, function, std::move(capture)));
    }

    return columns;
}

std::vector<MutableColumnPtr> ColumnFunction::scatter(
    IColumn::ColumnIndex,
    const IColumn::Selector &,
    const BlockSelective &) const
{
    throw TiFlashException("ColumnFunction does not support scatter", Errors::Coprocessor::Unimplemented);
}

void ColumnFunction::scatterTo(ScatterColumns &, const Selector &) const
{
    throw TiFlashException("ColumnFunction does not support scatterTo", Errors::Coprocessor::Unimplemented);
}

void ColumnFunction::scatterTo(ScatterColumns &, const Selector &, const BlockSelective &) const
{
    throw TiFlashException("ColumnFunction does not support scatterTo", Errors::Coprocessor::Unimplemented);
}

void ColumnFunction::insertDefault()
{
    for (auto & column : captured_columns)
        column.column->assumeMutableRef().insertDefault();
    ++column_size;
}
void ColumnFunction::popBack(size_t n)
{
    for (auto & column : captured_columns)
        column.column->assumeMutableRef().popBack(n);
    column_size -= n;
}

size_t ColumnFunction::byteSize() const
{
    size_t total_size = 0;
    for (const auto & column : captured_columns)
        total_size += column.column->byteSize();

    return total_size;
}

size_t ColumnFunction::byteSize(size_t offset, size_t limit) const
{
    size_t total_size = 0;
    for (const auto & column : captured_columns)
        total_size += column.column->byteSize(offset, limit);

    return total_size;
}

size_t ColumnFunction::allocatedBytes() const
{
    size_t total_size = 0;
    for (const auto & column : captured_columns)
        total_size += column.column->allocatedBytes();

    return total_size;
}

void ColumnFunction::appendArguments(const ColumnsWithTypeAndName & columns)
{
    auto args = function->getArgumentTypes().size();
    auto were_captured = captured_columns.size();
    auto wanna_capture = columns.size();

    if (were_captured + wanna_capture > args)
        throw Exception(
            fmt::format(
                "Cannot capture {} columns because function {} has {} arguments{}.",
                wanna_capture,
                function->getName(),
                args,
                were_captured ? fmt::format(" and {} columns have already been captured", were_captured) : ""),
            ErrorCodes::LOGICAL_ERROR);

    for (const auto & column : columns)
        appendArgument(column);
}

void ColumnFunction::appendArgument(const ColumnWithTypeAndName & column)
{
    const auto & argument_types = function->getArgumentTypes();

    auto index = captured_columns.size();
    if (!column.type->equals(*argument_types[index]))
        throw Exception(
            fmt::format(
                "Cannot capture column {} because it has incompatible type: got {}, but {} is expected.",
                argument_types.size(),
                column.type->getName(),
                argument_types[index]->getName()),
            ErrorCodes::LOGICAL_ERROR);

    captured_columns.push_back(column);
}

ColumnWithTypeAndName ColumnFunction::reduce() const
{
    auto args = function->getArgumentTypes().size();
    auto captured = captured_columns.size();

    if (args != captured)
        throw Exception(
            fmt::format(
                "Cannot call function {} because is has {} arguments but {} columns were captured.",
                function->getName(),
                args,
                captured),
            ErrorCodes::LOGICAL_ERROR);

    Block block(captured_columns);
    block.insert({nullptr, function->getReturnType(), ""});

    ColumnNumbers arguments(captured_columns.size());
    for (size_t i = 0; i < captured_columns.size(); ++i)
        arguments[i] = i;

    function->execute(block, arguments, captured_columns.size());

    return block.getByPosition(captured_columns.size());
}

} // namespace DB
