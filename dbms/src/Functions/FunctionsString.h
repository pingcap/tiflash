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
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Poco/UTF8Encoding.h>
#include <Poco/Unicode.h>
#include <fmt/core.h>

namespace DB
{
namespace tests
{
class StringsLowerUpperUtf8_Simple_Test;
class StringsLowerUpperUtf8_Random_Test;
} // namespace tests
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

/** String functions
  *
  * length, empty, notEmpty,
  * concat, substring, lower, upper, reverse
  * lengthUTF8, substringUTF8, lowerUTF8, upperUTF8, reverseUTF8
  *
  * s                -> UInt8:    empty, notEmpty
  * s                -> UInt64:   length, lengthUTF8
  * s                -> s:        lower, upper, lowerUTF8, upperUTF8, reverse, reverseUTF8
  * s, s             -> s:        concat, trim, ltrim, rtrim
  * s, c, s          -> s:        lpad, rpad
  * s, c1, c2        -> s:        substring, substringUTF8
  * s, c1, c2, s2    -> s:        replace, replaceUTF8
  *
  * The search functions for strings and regular expressions are located separately.
  * URL functions are located separately.
  * String encoding functions, converting to other types are located separately.
  *
  * The functions length, empty, notEmpty, reverse also work with arrays.
  */


/// xor or do nothing
template <bool>
UInt8 xor_or_identity(const UInt8 c, const int mask)
{
    return c ^ mask;
};
template <>
inline UInt8 xor_or_identity<false>(const UInt8 c, const int)
{
    return c;
}

/// It is caller's responsibility to ensure the presence of a valid cyrillic sequence in array
template <bool to_lower>
inline void UTF8CyrillicToCase(const UInt8 *& src, UInt8 *& dst)
{
    if (src[0] == 0xD0u && (src[1] >= 0x80u && src[1] <= 0x8Fu))
    {
        /// ЀЁЂЃЄЅІЇЈЉЊЋЌЍЎЏ
        *dst++ = xor_or_identity<to_lower>(*src++, 0x1);
        *dst++ = xor_or_identity<to_lower>(*src++, 0x10);
    }
    else if (src[0] == 0xD1u && (src[1] >= 0x90u && src[1] <= 0x9Fu))
    {
        /// ѐёђѓєѕіїјљњћќѝўџ
        *dst++ = xor_or_identity<!to_lower>(*src++, 0x1);
        *dst++ = xor_or_identity<!to_lower>(*src++, 0x10);
    }
    else if (src[0] == 0xD0u && (src[1] >= 0x90u && src[1] <= 0x9Fu))
    {
        /// А-П
        *dst++ = *src++;
        *dst++ = xor_or_identity<to_lower>(*src++, 0x20);
    }
    else if (src[0] == 0xD0u && (src[1] >= 0xB0u && src[1] <= 0xBFu))
    {
        /// а-п
        *dst++ = *src++;
        *dst++ = xor_or_identity<!to_lower>(*src++, 0x20);
    }
    else if (src[0] == 0xD0u && (src[1] >= 0xA0u && src[1] <= 0xAFu))
    {
        /// Р-Я
        *dst++ = xor_or_identity<to_lower>(*src++, 0x1);
        *dst++ = xor_or_identity<to_lower>(*src++, 0x20);
    }
    else if (src[0] == 0xD1u && (src[1] >= 0x80u && src[1] <= 0x8Fu))
    {
        /// р-я
        *dst++ = xor_or_identity<!to_lower>(*src++, 0x1);
        *dst++ = xor_or_identity<!to_lower>(*src++, 0x20);
    }
}


/** If the string contains UTF-8 encoded text, convert it to the lower (upper) case.
  * Note: It is assumed that after the character is converted to another case,
  *  the length of its multibyte sequence in UTF-8 does not change.
  * Otherwise, the behavior is undefined.
  */
template <
    char not_case_lower_bound,
    char not_case_upper_bound,
    int to_case(int),
    void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
struct LowerUpperUTF8Impl
{
    static void vector(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets);

    static void vectorFixed(const ColumnString::Chars_t & data, size_t n, ColumnString::Chars_t & res_data);

    static void constant(const std::string & data, std::string & res_data);

    /** Converts a single code point starting at `src` to desired case, storing result starting at `dst`.
     *    `src` and `dst` are incremented by corresponding sequence lengths. */
    static void toCase(const UInt8 *& src, const UInt8 * src_end, UInt8 *& dst);

private:
    static constexpr auto ascii_upper_bound = '\x7f';
    static constexpr auto flip_case_mask = 'A' ^ 'a';
    static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst);

    friend ::DB::tests::StringsLowerUpperUtf8_Simple_Test;
    friend ::DB::tests::StringsLowerUpperUtf8_Random_Test;
};


template <typename Impl, typename Name, bool is_injective = false>
class FunctionStringToString : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionStringToString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isInjective(const Block &) const override { return is_injective; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isStringOrFixedString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const auto * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets());
            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const auto * col = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            auto col_res = ColumnFixedString::create(col->getN());
            Impl::vectorFixed(col->getChars(), col->getN(), col_res->getChars());
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                fmt::format(
                    "Illegal column {} of argument of function {}",
                    block.getByPosition(arguments[0]).column->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

template <char not_case_lower_bound, char not_case_upper_bound, int to_case(int)>
struct TiDBLowerUpperUTF8Impl
{
    static void vector(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets);

    static void vectorFixed(const ColumnString::Chars_t & data, size_t n, ColumnString::Chars_t & res_data);

private:
    static constexpr auto ascii_upper_bound = '\x7f';
    static constexpr auto flip_case_mask = 'A' ^ 'a';
};

struct TiDBLowerUpperBinaryImpl
{
    static void vector(
        const ColumnString::Chars_t &,
        const ColumnString::Offsets &,
        ColumnString::Chars_t &,
        ColumnString::Offsets &)
    {
        throw Exception("the TiDB function of lower or upper for binary should do nothing.");
    }

    static void vectorFixed(const ColumnString::Chars_t &, size_t, ColumnString::Chars_t &)
    {
        throw Exception("the TiDB function of lower or upper for binary should do nothing.");
    }
};

template <typename Name, bool is_injective>
class FunctionStringToString<TiDBLowerUpperBinaryImpl, Name, is_injective> : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionStringToString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isInjective(const Block &) const override { return is_injective; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isStringOrFixedString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (checkAndGetColumn<ColumnString>(column.get()) || checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            block.getByPosition(result).column = column;
        }
        else
            throw Exception(
                fmt::format(
                    "Illegal column {} of argument of function {}",
                    block.getByPosition(arguments[0]).column->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};
} // namespace DB
