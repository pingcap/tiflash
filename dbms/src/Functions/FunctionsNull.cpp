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

#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsConditional.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/FunctionsNull.h>


namespace DB
{
void registerFunctionsNull(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsNull>();
    factory.registerFunction<FunctionIsNotNull>();
    factory.registerFunction<FunctionCoalesce>();
    factory.registerFunction<FunctionNullIf>();
    factory.registerFunction<FunctionAssumeNotNull>();
    factory.registerFunction<FunctionToNullable>();
}

/// Implementation of isNull.

FunctionPtr FunctionIsNull::create(const Context &)
{
    return std::make_shared<FunctionIsNull>();
}

std::string FunctionIsNull::getName() const
{
    return name;
}

DataTypePtr FunctionIsNull::getReturnTypeImpl(const DataTypes &) const
{
    return std::make_shared<DataTypeUInt8>();
}

void FunctionIsNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const
{
    const ColumnWithTypeAndName & elem = block.getByPosition(arguments[0]);
    if (elem.column->isColumnNullable())
    {
        /// Merely return the embedded null map.
        block.getByPosition(result).column = static_cast<const ColumnNullable &>(*elem.column).getNullMapColumnPtr();
    }
    else if (elem.column->onlyNull())
    {
        /// Since all element is null, return a one-constant column representing
        /// a one-filled null map.
        block.getByPosition(result).column
            = DataTypeUInt8().createColumnConst(elem.column->size(), static_cast<UInt64>(1));
    }
    else
    {
        /// Since no element is nullable, return a zero-constant column representing
        /// a zero-filled null map.
        block.getByPosition(result).column
            = DataTypeUInt8().createColumnConst(elem.column->size(), static_cast<UInt64>(0));
    }
}

/// Implementation of isNotNull.

FunctionPtr FunctionIsNotNull::create(const Context &)
{
    return std::make_shared<FunctionIsNotNull>();
}

std::string FunctionIsNotNull::getName() const
{
    return name;
}

DataTypePtr FunctionIsNotNull::getReturnTypeImpl(const DataTypes &) const
{
    return std::make_shared<DataTypeUInt8>();
}

void FunctionIsNotNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const
{
    Block temp_block{
        block.getByPosition(arguments[0]),
        {nullptr, std::make_shared<DataTypeUInt8>(), ""},
        {nullptr, std::make_shared<DataTypeUInt8>(), ""}};

    DefaultExecutable(std::make_shared<FunctionIsNull>()).execute(temp_block, {0}, 1);
    DefaultExecutable(std::make_shared<FunctionNot>()).execute(temp_block, {1}, 2);

    block.getByPosition(result).column = std::move(temp_block.getByPosition(2).column);
}

/// Implementation of coalesce.

FunctionPtr FunctionCoalesce::create(const Context & context)
{
    return std::make_shared<FunctionCoalesce>(context);
}

std::string FunctionCoalesce::getName() const
{
    return name;
}

DataTypePtr FunctionCoalesce::getReturnTypeImpl(const DataTypes & arguments) const
{
    /// Skip all NULL arguments. If any argument is non-Nullable, skip all next arguments.
    DataTypes filtered_args;
    filtered_args.reserve(arguments.size());
    for (const auto & arg : arguments)
    {
        if (arg->onlyNull())
            continue;

        filtered_args.push_back(arg);

        if (!arg->isNullable())
            break;
    }

    DataTypes new_args;
    for (size_t i = 0; i < filtered_args.size(); ++i)
    {
        bool is_last = i + 1 == filtered_args.size();

        if (is_last)
        {
            new_args.push_back(filtered_args[i]);
        }
        else
        {
            new_args.push_back(std::make_shared<DataTypeUInt8>());
            new_args.push_back(removeNullable(filtered_args[i]));
        }
    }

    if (new_args.empty())
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
    if (new_args.size() == 1)
        return new_args.front();

    auto res = FunctionMultiIf{context}.getReturnTypeImpl(new_args);

    /// if last argument is not nullable, result should be also not nullable
    if (!new_args.back()->isNullable() && res->isNullable())
        res = removeNullable(res);

    return res;
}

void FunctionCoalesce::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const
{
    /// coalesce(arg0, arg1, ..., argN) is essentially
    /// multiIf(isNotNull(arg0), assumeNotNull(arg0), isNotNull(arg1), assumeNotNull(arg1), ..., argN)
    /// with constant NULL arguments removed.

    ColumnNumbers filtered_args;
    filtered_args.reserve(arguments.size());
    for (const auto & arg : arguments)
    {
        const auto & type = block.getByPosition(arg).type;

        if (type->onlyNull())
            continue;

        filtered_args.push_back(arg);

        if (!type->isNullable())
            break;
    }

    DefaultExecutable is_not_null(std::make_shared<FunctionIsNotNull>());
    DefaultExecutable assume_not_null(std::make_shared<FunctionAssumeNotNull>());
    ColumnNumbers multi_if_args;

    Block temp_block = block;

    for (size_t i = 0; i < filtered_args.size(); ++i)
    {
        size_t res_pos = temp_block.columns();
        bool is_last = i + 1 == filtered_args.size();

        if (is_last)
        {
            multi_if_args.push_back(filtered_args[i]);
        }
        else
        {
            temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});
            is_not_null.execute(temp_block, {filtered_args[i]}, res_pos);
            temp_block.insert({nullptr, removeNullable(block.getByPosition(filtered_args[i]).type), ""});
            assume_not_null.execute(temp_block, {filtered_args[i]}, res_pos + 1);

            multi_if_args.push_back(res_pos);
            multi_if_args.push_back(res_pos + 1);
        }
    }

    /// If all arguments appeared to be NULL.
    if (multi_if_args.empty())
    {
        block.getByPosition(result).column
            = block.getByPosition(result).type->createColumnConstWithDefaultValue(block.rows());
        return;
    }

    if (multi_if_args.size() == 1)
    {
        block.getByPosition(result).column = block.getByPosition(multi_if_args.front()).column;
        return;
    }

    DefaultExecutable(std::make_shared<FunctionMultiIf>(context)).execute(temp_block, multi_if_args, result);

    ColumnPtr res = std::move(temp_block.getByPosition(result).column);

    /// if last argument is not nullable, result should be also not nullable
    if (!block.getByPosition(filtered_args.back()).type->isNullable() && res->isColumnNullable())
        res = static_cast<const ColumnNullable &>(*res).getNestedColumnPtr();

    block.getByPosition(result).column = std::move(res);
}

/// Implementation of nullIf.

FunctionPtr FunctionNullIf::create(const Context &)
{
    return std::make_shared<FunctionNullIf>();
}

std::string FunctionNullIf::getName() const
{
    return name;
}

DataTypePtr FunctionNullIf::getReturnTypeImpl(const DataTypes & arguments) const
{
    return FunctionIf{}.getReturnTypeImpl(
        {std::make_shared<DataTypeUInt8>(), makeNullable(arguments[0]), arguments[0]});
}

void FunctionNullIf::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const
{
    /// nullIf(col1, col2) == if(col1 == col2, NULL, col1)

    Block temp_block = block;

    size_t res_pos = temp_block.columns();
    temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});

    DefaultExecutable(std::make_shared<FunctionEquals>()).execute(temp_block, {arguments[0], arguments[1]}, res_pos);

    /// Argument corresponding to the NULL value.
    size_t null_pos = temp_block.columns();

    /// Append a NULL column.
    ColumnWithTypeAndName null_elem;
    null_elem.type = block.getByPosition(result).type;
    null_elem.column = null_elem.type->createColumnConstWithDefaultValue(temp_block.rows());
    null_elem.name = "NULL";

    temp_block.insert(null_elem);

    DefaultExecutable(std::make_shared<FunctionIf>()).execute(temp_block, {res_pos, null_pos, arguments[0]}, result);

    block.getByPosition(result).column = std::move(temp_block.getByPosition(result).column);
}

/// Implementation of assumeNotNull.

FunctionPtr FunctionAssumeNotNull::create(const Context &)
{
    return std::make_shared<FunctionAssumeNotNull>();
}

std::string FunctionAssumeNotNull::getName() const
{
    return name;
}

DataTypePtr FunctionAssumeNotNull::getReturnTypeImpl(const DataTypes & arguments) const
{
    return removeNullable(arguments[0]);
}

void FunctionAssumeNotNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const
{
    const ColumnPtr & col = block.getByPosition(arguments[0]).column;
    ColumnPtr & res_col = block.getByPosition(result).column;

    if (col->isColumnNullable())
    {
        const auto & nullable_col = static_cast<const ColumnNullable &>(*col);
        res_col = nullable_col.getNestedColumnPtr();
    }
    else
        res_col = col;
}

/// Implementation of toNullable.

FunctionPtr FunctionToNullable::create(const Context &)
{
    return std::make_shared<FunctionToNullable>();
}

std::string FunctionToNullable::getName() const
{
    return name;
}

DataTypePtr FunctionToNullable::getReturnTypeImpl(const DataTypes & arguments) const
{
    return makeNullable(arguments[0]);
}

void FunctionToNullable::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const
{
    block.getByPosition(result).column = makeNullable(block.getByPosition(arguments[0]).column);
}

} // namespace DB
