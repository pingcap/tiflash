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
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/StringUtil.h>
#include <common/defines.h>

#include <cstring>
#include <string_view>

namespace DB
{
using Chars_t = ColumnString::Chars_t;
using Offsets = ColumnString::Offsets;

class IlikeLowerHelper
{
public:
    static void convertCollatorToBin(TiDB::TiDBCollatorPtr & collator)
    {
        if (collator == nullptr)
            return;

        switch (collator->getCollatorType())
        {
        case TiDB::ITiDBCollator::CollatorType::UTF8_GENERAL_CI:
        case TiDB::ITiDBCollator::CollatorType::UTF8_UNICODE_CI:
            collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8_BIN);
            break;
        case TiDB::ITiDBCollator::CollatorType::UTF8MB4_GENERAL_CI:
        case TiDB::ITiDBCollator::CollatorType::UTF8MB4_UNICODE_CI:
            collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
            break;
        case TiDB::ITiDBCollator::CollatorType::UTF8MB4_0900_AI_CI:
            collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_0900_BIN);
            break;
        default:
            break;
        }
    }

    // Only lower 'A', 'B', 'C'... 'Z', excluding the escape char
    static void lowerAlphaASCII(Block & block, const ColumnNumbers & arguments)
    {
        MutableColumnPtr column_expr = block.getByPosition(arguments[0]).column->assumeMutable();
        MutableColumnPtr column_pat = block.getByPosition(arguments[1]).column->assumeMutable();
        const ColumnPtr & column_escape = block.getByPosition(arguments[2]).column;

        auto * col_haystack_const = typeid_cast<ColumnConst *>(&*column_expr);
        auto * col_needle_const = typeid_cast<ColumnConst *>(&*column_pat);

        if (col_haystack_const != nullptr)
            lowerColumnConst(col_haystack_const, nullptr);
        else
            lowerColumnString(column_expr, nullptr);

        if (col_needle_const != nullptr)
            lowerColumnConst(col_needle_const, column_escape);
        else
            lowerColumnString(column_pat, column_escape);
    }

private:
    static void lowerStrings(Chars_t & chars)
    {
        size_t size = chars.size();
        for (size_t i = 0; i < size; ++i)
        {
            if (isUpperAlphaASCII(chars[i]))
            {
                chars[i] = toLowerIfAlphaASCII(chars[i]);
            }
            else
            {
                size_t utf8_len = UTF8::seqLength(chars[i]);
                i += utf8_len - 1;
            }
        }
    }

    // When escape_char is a lower char, we need to convert it to the capital char
    // Because: when lowering "ABC" with escape 'a', after lower, "ABC" -> "abc",
    // then 'a' will be an escape char and it is not expected.
    // Morever, when escape char is uppered we need to tell it to the caller.
    static void lowerStringsExcludeEscapeChar(Chars_t & chars, char escape_char)
    {
        if (!isAlphaASCII(escape_char))
        {
            lowerStrings(chars);
            return;
        }

        size_t size = chars.size();
        bool escaped = false;
        char actual_escape_char = isLowerAplhaASCII(escape_char) ? toUpperIfAlphaASCII(escape_char) : escape_char;

        for (size_t i = 0; i < size; ++i)
        {
            char char_to_lower = chars[i];
            if (isUpperAlphaASCII(char_to_lower))
            {
                // Do not lower the escape char, however when a char is equal to
                // an escape char and it's after an escape char, we still lower it
                // For example: "AA" (escape 'A'), -> "Aa"
                if (char_to_lower != escape_char || escaped)
                {
                    chars[i] = toLowerIfAlphaASCII(char_to_lower);
                }
                else
                {
                    escaped = true;
                    continue;
                }
            }
            else
            {
                if ((chars[i] == static_cast<unsigned char>(escape_char)) && !escaped)
                {
                    escaped = true;

                    // It should be `chars[i] = toUpperIfAlphaASCII(chars[i])`,
                    // but 'actual_escape_char' is always equal to 'toUpperIfAlphaASCII(str[i])'
                    chars[i] = actual_escape_char;
                    continue;
                }
                size_t utf8_len = UTF8::seqLength(char_to_lower);
                i += utf8_len - 1;
            }
            escaped = false;
        }
    }

    static void lowerColumnConst(ColumnConst * lowered_col_const, const ColumnPtr & column_escape)
    {
        auto * col_data = typeid_cast<ColumnString *>(&lowered_col_const->getDataColumn());
        RUNTIME_ASSERT(col_data != nullptr, "Invalid column type, should be ColumnString");

        lowerColumnStringImpl(col_data, column_escape);
    }

    static void lowerColumnString(MutableColumnPtr & col, const ColumnPtr & column_escape)
    {
        auto * col_vector = typeid_cast<ColumnString *>(&*col);
        RUNTIME_ASSERT(col_vector != nullptr, "Invalid column type, should be ColumnString");

        lowerColumnStringImpl(col_vector, column_escape);
    }

    static void lowerColumnStringImpl(ColumnString * lowered_col_data, const ColumnPtr & column_escape)
    {
        if (column_escape == nullptr)
        {
            lowerStrings(lowered_col_data->getChars());
            return;
        }

        const auto * col_escape_const = typeid_cast<const ColumnConst *>(&*column_escape);
        RUNTIME_CHECK_MSG(col_escape_const != nullptr, "escape char column should be constant");
        char escape_char = static_cast<Int32>(col_escape_const->getValue<Int32>());

        lowerStringsExcludeEscapeChar(lowered_col_data->getChars(), escape_char);
    }
};

/** Search and replace functions in strings:
  *
  * position(haystack, needle)     - the normal search for a substring in a string, returns the position (in bytes) of the found substring starting with 1, or 0 if no substring is found.
  * positionUTF8(haystack, needle) - the same, but the position is calculated at code points, provided that the string is encoded in UTF-8.
  * positionCaseInsensitive(haystack, needle)
  * positionCaseInsensitiveUTF8(haystack, needle)
  *
  * like(haystack, pattern)        - search by the regular expression LIKE; Returns 0 or 1. Case-insensitive, but only for Latin.
  * notLike(haystack, pattern)
  *
  * match(haystack, pattern)       - search by regular expression re2; Returns 0 or 1.
  *
  * Applies regexp re2 and pulls:
  * - the first subpattern, if the regexp has a subpattern;
  * - the zero subpattern (the match part, otherwise);
  * - if not match - an empty string.
  * extract(haystack, pattern)
  *
  * replaceOne(haystack, pattern, replacement) - replacing the pattern with the specified rules, only the first occurrence.
  * replaceAll(haystack, pattern, replacement) - replacing the pattern with the specified rules, all occurrences.
  *
  * replaceRegexpOne(haystack, pattern, replacement) - replaces the pattern with the specified regexp, only the first occurrence.
  * replaceRegexpAll(haystack, pattern, replacement) - replaces the pattern with the specified type, all occurrences.
  *
  * Warning! At this point, the arguments needle, pattern, n, replacement must be constants.
  */

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

static const UInt8 CH_ESCAPE_CHAR = '\\';

struct NameIlike3Args
{
    static constexpr auto name = "ilike3Args";
};

template <typename Impl, typename Name>
class FunctionsStringSearch : public IFunction
{
    static_assert(!(Impl::need_customized_escape_char && Impl::support_match_type));

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsStringSearch>(); }

    String getName() const override { return name; }

    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { collator = collator_; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {3}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isString())
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if constexpr (Impl::need_customized_escape_char)
        {
            if (!arguments[2]->isInteger())
                throw Exception(
                    "Illegal type " + arguments[2]->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else if constexpr (Impl::support_match_type)
        {
            if (arguments.size() > 2 && !arguments[2]->isString())
                throw Exception(
                    "Illegal type " + arguments[2]->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        size_t max_arguments_number = Impl::need_customized_escape_char || Impl::support_match_type ? 3 : 2;
        if (arguments.size() > max_arguments_number)
            throw Exception(
                "Too many arguments, only " + std::to_string(max_arguments_number)
                + " argument is supported for function " + getName());

        return std::make_shared<DataTypeNumber<typename Impl::ResultType>>();
    }

    void executeImpl(Block & result_block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto block = result_block;
        if constexpr (name == std::string_view(NameIlike3Args::name))
        {
            block.getByPosition(arguments[0]).column
                = (*std::move(result_block.getByPosition(arguments[0]).column)).mutate();
            block.getByPosition(arguments[1]).column
                = (*std::move(result_block.getByPosition(arguments[1]).column)).mutate();

            IlikeLowerHelper::lowerAlphaASCII(block, arguments);
            IlikeLowerHelper::convertCollatorToBin(collator);
        }

        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = block.getByPosition(arguments[0]).column;
        const ColumnPtr & column_needle = block.getByPosition(arguments[1]).column;

        const auto * col_haystack_const = typeid_cast<const ColumnConst *>(&*column_haystack);
        const auto * col_needle_const = typeid_cast<const ColumnConst *>(&*column_needle);

        UInt8 escape_char = CH_ESCAPE_CHAR;
        String match_type;
        if constexpr (Impl::need_customized_escape_char)
        {
            const auto * col_escape_const
                = typeid_cast<const ColumnConst *>(&*block.getByPosition(arguments[2]).column);
            bool valid_args = true;
            if (col_escape_const == nullptr)
            {
                valid_args = false;
            }
            else
            {
                auto c = col_escape_const->getValue<Int32>();
                if (c < 0 || c > 255)
                {
                    // todo maybe use more strict constraint
                    valid_args = false;
                }
                else
                {
                    escape_char = static_cast<UInt8>(c);
                    if constexpr (name == std::string_view(NameIlike3Args::name))
                        if (isLowerAplhaASCII(escape_char))
                            escape_char = toUpperIfAlphaASCII(escape_char);
                }
            }
            if (!valid_args)
            {
                throw Exception("3rd arguments of function " + getName() + " must be constants and between 0 and 255.");
            }
        }
        else if constexpr (Impl::support_match_type)
        {
            if (arguments.size() > 2)
            {
                const auto * col_match_type_const
                    = typeid_cast<const ColumnConst *>(&*block.getByPosition(arguments[2]).column);
                if (col_match_type_const == nullptr)
                    throw Exception("Match type argument of function " + getName() + " must be constant");
                match_type = col_match_type_const->getValue<String>();
            }
        }

        if (col_haystack_const && col_needle_const)
        {
            ResultType res{};
            auto needle_string = col_needle_const->getValue<String>();
            Impl::constantConstant(
                col_haystack_const->getValue<String>(),
                needle_string,
                escape_char,
                match_type,
                collator,
                res);
            result_block.getByPosition(result).column
                = result_block.getByPosition(result).type->createColumnConst(col_haystack_const->size(), toField(res));
            return;
        }

        auto col_res = ColumnVector<ResultType>::create();

        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        vec_res.resize(column_haystack->size());

        const auto * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);
        const auto * col_needle_vector = checkAndGetColumn<ColumnString>(&*column_needle);

        if (col_haystack_vector && col_needle_vector)
            Impl::vectorVector(
                col_haystack_vector->getChars(),
                col_haystack_vector->getOffsets(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
                escape_char,
                match_type,
                collator,
                vec_res);
        else if (col_haystack_vector && col_needle_const)
        {
            auto needle_string = col_needle_const->getValue<String>();
            Impl::vectorConstant(
                col_haystack_vector->getChars(),
                col_haystack_vector->getOffsets(),
                needle_string,
                escape_char,
                match_type,
                collator,
                vec_res);
        }
        else if (col_haystack_const && col_needle_vector)
        {
            auto haystack = col_haystack_const->getValue<String>();
            const ColumnString::Chars_t & needle_chars = col_needle_vector->getChars();
            const IColumn::Offsets & needle_offsets = col_needle_vector->getOffsets();
            Impl::constantVector(haystack, needle_chars, needle_offsets, escape_char, match_type, collator, vec_res);
        }
        else
            throw Exception(
                "Illegal columns " + block.getByPosition(arguments[0]).column->getName() + " and "
                    + block.getByPosition(arguments[1]).column->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        result_block.getByPosition(result).column = std::move(col_res);
    }

private:
    mutable TiDB::TiDBCollatorPtr collator = nullptr;
};


template <typename Impl, typename Name>
class FunctionsStringSearchToString : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsStringSearchToString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isString())
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        const ColumnPtr column_needle = block.getByPosition(arguments[1]).column;

        const auto * col_needle = typeid_cast<const ColumnConst *>(&*column_needle);
        if (!col_needle)
            throw Exception(
                "Second argument of function " + getName() + " must be constant string.",
                ErrorCodes::ILLEGAL_COLUMN);

        if (const auto * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();

            ColumnString::Chars_t & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            Impl::vector(col->getChars(), col->getOffsets(), col_needle->getValue<String>(), vec_res, offsets_res);

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};
} // namespace DB
