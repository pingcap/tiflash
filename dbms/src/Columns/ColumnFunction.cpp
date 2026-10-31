// Modified from: https://github.com/ClickHouse/ClickHouse/blob/30fcaeb2a3fff1bf894aae9c776bed7fd83f783f/dbms/src/Columns/ColumnFunction.cpp
//
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
#include <Columns/countBytesInFilter.h>
#include <Columns/filterColumn.h>
#include <Common/typeid_cast.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <fmt/format.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

ColumnFunction::ColumnFunction(
    size_t size,
    FunctionBasePtr function,
    const ColumnsWithTypeAndName & columns_to_capture,
    bool is_short_circuit_argument_)
    : column_size(size)
    , function(function)
    , is_short_circuit_argument(is_short_circuit_argument_)
{
    appendArguments(columns_to_capture);
}

MutableColumnPtr ColumnFunction::cloneResized(size_t size) const
{
    ColumnsWithTypeAndName capture = captured_columns;
    for (auto & column : capture)
        column.column = column.column->cloneResized(size);

    return ColumnFunction::create(size, function, capture, is_short_circuit_argument);
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
    return ColumnFunction::create(replicated_size, function, capture, is_short_circuit_argument);
}

ColumnPtr ColumnFunction::cut(size_t start, size_t length) const
{
    ColumnsWithTypeAndName capture = captured_columns;
    for (auto & column : capture)
        column.column = column.column->cut(start, length);

    return ColumnFunction::create(length, function, capture, is_short_circuit_argument);
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

    return ColumnFunction::create(filtered_size, function, capture, is_short_circuit_argument);
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

    return ColumnFunction::create(limit, function, capture, is_short_circuit_argument);
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
        columns.emplace_back(ColumnFunction::create(s, function, std::move(capture), is_short_circuit_argument));
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

    if (is_short_circuit_argument && column_size == 0)
        return {function->getReturnType()->createColumn(), function->getReturnType(), ""};

    auto columns = captured_columns;
    if (is_short_circuit_argument)
    {
        const size_t required_arguments = function->isShortCircuit() ? 1 : columns.size();
        for (size_t i = 0; i < required_arguments; ++i)
            if (const auto * deferred = checkAndGetShortCircuitArgument(columns[i].column))
                columns[i].column = deferred->reduce().column;
    }

    Block block(columns);
    block.insert({nullptr, function->getReturnType(), ""});

    ColumnNumbers arguments(captured_columns.size());
    for (size_t i = 0; i < captured_columns.size(); ++i)
        arguments[i] = i;

    function->execute(block, arguments, captured_columns.size());

    return block.getByPosition(captured_columns.size());
}

const ColumnFunction * checkAndGetShortCircuitArgument(const ColumnPtr & column)
{
    const auto * function = typeid_cast<const ColumnFunction *>(column.get());
    return function && function->isShortCircuitArgument() ? function : nullptr;
}

ColumnPtr ColumnFunction::executeMaskedIntoBlock(Block && block, const IColumn::Filter & mask) const
{
    block.insert({nullptr, function->getReturnType(), ""});

    ColumnNumbers arguments(captured_columns.size());
    for (size_t i = 0; i < arguments.size(); ++i)
        arguments[i] = i;

    function->executeWithMask(block, arguments, captured_columns.size(), mask);

    /// Unlike the filter-then-reduce path, an executeWithMask override is trusted
    /// to produce the result itself, so enforce the row-count contract here.
    ColumnPtr result = block.getByPosition(captured_columns.size()).column;
    RUNTIME_CHECK_MSG(
        result != nullptr && result->size() == mask.size(),
        "Masked execution of function {} produced {} rows instead of {}",
        function->getName(),
        result ? result->size() : 0,
        mask.size());
    return result;
}

ColumnPtr ColumnFunction::reduceWithMask(const IColumn::Filter & mask) const
{
    bool has_deferred_argument = false;
    for (const auto & captured : captured_columns)
        if (checkAndGetShortCircuitArgument(captured.column))
        {
            has_deferred_argument = true;
            break;
        }

    /// A deferred argument nested in the captures is consumed by passing the
    /// same mask down: the nested subtree is guarded by the same consumer, so
    /// the required row set is identical. Short-circuit functions (and/or)
    /// are the exception - they keep their deferred arguments and take over
    /// the external mask in their own short-circuit loop. Every other
    /// function materializes each nested deferred argument in place
    /// (recursing through maskedExecute -> reduceWithMask) and then runs
    /// executeWithMask on the materialized columns. Whether masked-off rows
    /// are skipped or evaluated is up to the head's executeWithMask: the
    /// default evaluates them (unless the function can throw, in which case
    /// it filters), and native overrides - e.g. the JSON parsers - skip
    /// them.
    if (has_deferred_argument && !function->isShortCircuit())
    {
        Block block;
        for (auto captured : captured_columns)
        {
            maskedExecute(captured, mask);
            block.insert(std::move(captured));
        }
        return executeMaskedIntoBlock(std::move(block), mask);
    }

    return executeMaskedIntoBlock(Block(captured_columns), mask);
}

void maskedExecute(ColumnWithTypeAndName & column, const IColumn::Filter & mask)
{
    const auto * deferred = checkAndGetShortCircuitArgument(column.column);
    if (!deferred)
        return;

    RUNTIME_CHECK(column.column->size() == mask.size());
    const size_t selected = countBytesInFilter(mask);
    if (selected == 0)
    {
        column.column = column.type->createColumnConstWithDefaultValue(mask.size());
        return;
    }
    if (selected == mask.size())
    {
        column.column = deferred->reduce().column;
        return;
    }

    /// Let the function consume the mask itself (IFunctionBase::
    /// executeWithMask): it decides how to treat the masked-off rows and
    /// returns a full-size column, so no filter/expand copies are needed at
    /// this level.
    column.column = deferred->reduceWithMask(mask);
}

} // namespace DB
