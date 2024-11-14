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
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
} // namespace ErrorCodes

template <typename Impl, typename Name>
class FunctionStringReplace : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionStringReplace>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if constexpr (Impl::support_non_const_needle && Impl::support_non_const_replacement)
        {
            return {3, 4, 5};
        }
        else if constexpr (Impl::support_non_const_needle)
        {
            return {2, 3, 4, 5};
        }
        else if constexpr (Impl::support_non_const_replacement)
        {
            return {1, 3, 4, 5};
        }
        else
        {
            return {1, 2, 3, 4, 5};
        }
    }
    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { collator = collator_; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isStringOrFixedString())
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isStringOrFixedString())
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[2]->isStringOrFixedString())
            throw Exception(
                "Illegal type " + arguments[2]->getName() + " of third argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() > 3 && !arguments[3]->isInteger())
            throw Exception(
                "Illegal type " + arguments[2]->getName() + " of forth argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() > 4 && !arguments[4]->isInteger())
            throw Exception(
                "Illegal type " + arguments[2]->getName() + " of fifth argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() > 5 && !arguments[5]->isStringOrFixedString())
            throw Exception(
                "Illegal type " + arguments[2]->getName() + " of sixth argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        ColumnPtr column_src = block.getByPosition(arguments[0]).column;
        ColumnPtr column_needle = block.getByPosition(arguments[1]).column;
        ColumnPtr column_replacement = block.getByPosition(arguments[2]).column;
        const ColumnPtr column_pos = arguments.size() > 3 ? block.getByPosition(arguments[3]).column : nullptr;
        const ColumnPtr column_occ = arguments.size() > 4 ? block.getByPosition(arguments[4]).column : nullptr;
        const ColumnPtr column_match_type = arguments.size() > 5 ? block.getByPosition(arguments[5]).column : nullptr;

        if ((column_pos != nullptr && !column_pos->isColumnConst())
            || (column_occ != nullptr && !column_occ->isColumnConst())
            || (column_match_type != nullptr && !column_match_type->isColumnConst()))
            throw Exception("4th, 5th, 6th arguments of function " + getName() + " must be constants.");
        Int64 pos = column_pos == nullptr ? 1 : typeid_cast<const ColumnConst *>(column_pos.get())->getInt(0);
        Int64 occ = column_occ == nullptr ? 0 : typeid_cast<const ColumnConst *>(column_occ.get())->getInt(0);
        String match_type = column_match_type == nullptr
            ? ""
            : typeid_cast<const ColumnConst *>(column_match_type.get())->getValue<String>();

        ColumnWithTypeAndName & column_result = block.getByPosition(result);

        bool needle_const = column_needle->isColumnConst();
        bool replacement_const = column_replacement->isColumnConst();
        
        if (const auto * column_src_const = checkAndGetColumn<ColumnConst>(column_src.get()))
        {
            column_src = column_src_const->convertToFullColumn();
        }

        if (needle_const && replacement_const)
        {
            executeImpl(column_src, column_needle, column_replacement, pos, occ, match_type, column_result);
        }
        else if (needle_const)
        {
            executeImplNonConstReplacement(
                column_src,
                column_needle,
                column_replacement,
                pos,
                occ,
                match_type,
                column_result);
        }
        else if (replacement_const)
        {
            executeImplNonConstNeedle(
                column_src,
                column_needle,
                column_replacement,
                pos,
                occ,
                match_type,
                column_result);
        }
        else
        {
            executeImplNonConstNeedleReplacement(
                column_src,
                column_needle,
                column_replacement,
                pos,
                occ,
                match_type,
                column_result);
        }
    }

private:
    void executeImpl(
        const ColumnPtr & column_src,
        const ColumnPtr & column_needle,
        const ColumnPtr & column_replacement,
        Int64 pos,
        Int64 occ,
        const String & match_type,
        ColumnWithTypeAndName & column_result) const
    {
        const auto * c1_const = typeid_cast<const ColumnConst *>(column_needle.get());
        const auto * c2_const = typeid_cast<const ColumnConst *>(column_replacement.get());
        auto needle = c1_const->getValue<String>();
        auto replacement = c2_const->getValue<String>();

        if (const auto * col = checkAndGetColumn<ColumnString>(column_src.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vector(
                col->getChars(),
                col->getOffsets(),
                needle,
                replacement,
                pos,
                occ,
                match_type,
                collator,
                col_res->getChars(),
                col_res->getOffsets());
            column_result.column = std::move(col_res);
        }
        else if (const auto * col = checkAndGetColumn<ColumnFixedString>(column_src.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vectorFixed(
                col->getChars(),
                col->getN(),
                needle,
                replacement,
                pos,
                occ,
                match_type,
                collator,
                col_res->getChars(),
                col_res->getOffsets());
            column_result.column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + column_src->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    void executeImplNonConstNeedle(
        const ColumnPtr & column_src,
        const ColumnPtr & column_needle,
        const ColumnPtr & column_replacement,
        Int64 pos [[maybe_unused]],
        Int64 occ [[maybe_unused]],
        const String & match_type,
        ColumnWithTypeAndName & column_result) const
    {
        if constexpr (Impl::support_non_const_needle)
        {
            const auto * col_needle = typeid_cast<const ColumnString *>(column_needle.get());
            const auto * col_replacement_const = typeid_cast<const ColumnConst *>(column_replacement.get());
            auto replacement = col_replacement_const->getValue<String>();

            if (const auto * col = checkAndGetColumn<ColumnString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorNonConstNeedle(
                    col->getChars(),
                    col->getOffsets(),
                    col_needle->getChars(),
                    col_needle->getOffsets(),
                    replacement,
                    pos,
                    occ,
                    match_type,
                    collator,
                    col_res->getChars(),
                    col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else if (const auto * col = checkAndGetColumn<ColumnFixedString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorFixedNonConstNeedle(
                    col->getChars(),
                    col->getN(),
                    col_needle->getChars(),
                    col_needle->getOffsets(),
                    replacement,
                    pos,
                    occ,
                    match_type,
                    collator,
                    col_res->getChars(),
                    col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else
                throw Exception(
                    "Illegal column " + column_src->getName() + " of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else
        {
            throw Exception("Argument at index 2 for function replace must be constant", ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    void executeImplNonConstReplacement(
        const ColumnPtr & column_src,
        const ColumnPtr & column_needle,
        const ColumnPtr & column_replacement,
        Int64 pos [[maybe_unused]],
        Int64 occ [[maybe_unused]],
        const String & match_type,
        ColumnWithTypeAndName & column_result) const
    {
        if constexpr (Impl::support_non_const_replacement)
        {
            const auto * col_needle_const = typeid_cast<const ColumnConst *>(column_needle.get());
            auto needle = col_needle_const->getValue<String>();
            const auto * col_replacement = typeid_cast<const ColumnString *>(column_replacement.get());

            if (const auto * col = checkAndGetColumn<ColumnString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorNonConstReplacement(
                    col->getChars(),
                    col->getOffsets(),
                    needle,
                    col_replacement->getChars(),
                    col_replacement->getOffsets(),
                    pos,
                    occ,
                    match_type,
                    collator,
                    col_res->getChars(),
                    col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else if (const auto * col = checkAndGetColumn<ColumnFixedString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorFixedNonConstReplacement(
                    col->getChars(),
                    col->getN(),
                    needle,
                    col_replacement->getChars(),
                    col_replacement->getOffsets(),
                    pos,
                    occ,
                    match_type,
                    collator,
                    col_res->getChars(),
                    col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else
                throw Exception(
                    "Illegal column " + column_src->getName() + " of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else
        {
            throw Exception("Argument at index 3 for function replace must be constant", ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    void executeImplNonConstNeedleReplacement(
        const ColumnPtr & column_src,
        const ColumnPtr & column_needle,
        const ColumnPtr & column_replacement,
        Int64 pos [[maybe_unused]],
        Int64 occ [[maybe_unused]],
        const String & match_type,
        ColumnWithTypeAndName & column_result) const
    {
        if constexpr (Impl::support_non_const_needle && Impl::support_non_const_replacement)
        {
            const auto * col_needle = typeid_cast<const ColumnString *>(column_needle.get());
            const auto * col_replacement = typeid_cast<const ColumnString *>(column_replacement.get());

            if (const auto * col = checkAndGetColumn<ColumnString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorNonConstNeedleReplacement(
                    col->getChars(),
                    col->getOffsets(),
                    col_needle->getChars(),
                    col_needle->getOffsets(),
                    col_replacement->getChars(),
                    col_replacement->getOffsets(),
                    pos,
                    occ,
                    match_type,
                    collator,
                    col_res->getChars(),
                    col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else if (const auto * col = checkAndGetColumn<ColumnFixedString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorFixedNonConstNeedleReplacement(
                    col->getChars(),
                    col->getN(),
                    col_needle->getChars(),
                    col_needle->getOffsets(),
                    col_replacement->getChars(),
                    col_replacement->getOffsets(),
                    pos,
                    occ,
                    match_type,
                    collator,
                    col_res->getChars(),
                    col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else
                throw Exception(
                    "Illegal column " + column_src->getName() + " of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else
        {
            throw Exception(
                "Argument at index 2 and 3 for function replace must be constant",
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    TiDB::TiDBCollatorPtr collator{};
};
} // namespace DB
