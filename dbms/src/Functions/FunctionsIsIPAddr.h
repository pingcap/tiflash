// Copyright 2023 PingCAP, Ltd.
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

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#ifndef IN6ADDRSZ
#define IN6ADDRSZ 16
#endif

#ifndef INT16SZ
#define INT16SZ sizeof(short)
#endif

#ifndef INADDRSZ
#define INADDRSZ 4
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes

/** Helper functions
  *
  * isIPv4(x) - Judge whether the input string is an IPv4 address.
  *
  * isIPv6(x) - Judge whether the input string is an IPv6 address.
  *
  */

/* Description：
 * This function is used to determine whether the input string is an IPv4 address, 
 * and the code comes from the inet_pton4 function of "arpa/inet.h".
 * References: http://svn.apache.org/repos/asf/apr/apr/trunk/network_io/unix/inet_pton.c
 */
static inline UInt8 isIPv4(const char * src)
{
    if (nullptr == src)
        return 0;

    static const char digits[] = "0123456789";
    int saw_digit, octets;
    char ch;
    unsigned char tmp[INADDRSZ], *tp;

    saw_digit = 0;
    octets = 0;
    *(tp = tmp) = 0;
    while ((ch = *src++) != '\0')
    {
        const char * pch;

        if ((pch = strchr(digits, ch)) != nullptr)
        {
            unsigned int num = *tp * 10 + static_cast<unsigned int>(pch - digits);

            if (num > 255)
                return 0;
            *tp = num;
            if (!saw_digit)
            {
                if (++octets > 4)
                    return 0;
                saw_digit = 1;
            }
        }
        else if (ch == '.' && saw_digit)
        {
            if (octets == 4)
                return 0;
            *++tp = 0;
            saw_digit = 0;
        }
        else
            return 0;
    }
    if (octets < 4)
        return 0;

    return 1;
}

/* Description：
 * This function is used to determine whether the input string is an IPv6 address, 
 * and the code comes from the inet_pton6 function of "arpa/inet.h".
 * References: http://svn.apache.org/repos/asf/apr/apr/trunk/network_io/unix/inet_pton.c
 */
static inline UInt8 isIPv6(const char * src)
{
    if (nullptr == src)
        return 0;
    static const char xdigits_l[] = "0123456789abcdef",
                      xdigits_u[] = "0123456789ABCDEF";
    unsigned char tmp[16], *tp, *endp, *colonp;
    const char *xdigits, *curtok;
    int ch, saw_xdigit;
    unsigned int val;

    memset((tp = tmp), '\0', IN6ADDRSZ);
    endp = tp + IN6ADDRSZ;
    colonp = nullptr;
    if (*src == ':')
        if (*++src != ':')
            return 0;
    curtok = src;
    saw_xdigit = 0;
    val = 0;
    while ((ch = *src++) != '\0')
    {
        const char * pch;

        if ((pch = strchr((xdigits = xdigits_l), ch)) == nullptr)
            pch = strchr((xdigits = xdigits_u), ch);
        if (pch != nullptr)
        {
            val <<= 4;
            val |= (pch - xdigits);
            if (val > 0xffff)
                return 0;
            saw_xdigit = 1;
            continue;
        }
        if (ch == ':')
        {
            curtok = src;
            if (!saw_xdigit)
            {
                if (colonp)
                    return 0;
                colonp = tp;
                continue;
            }
            if (tp + INT16SZ > endp)
                return 0;
            *tp++ = (unsigned char)(val >> 8) & 0xff;
            *tp++ = (unsigned char)val & 0xff;
            saw_xdigit = 0;
            val = 0;
            continue;
        }
        if (ch == '.' && ((tp + INADDRSZ) <= endp) && isIPv4(curtok) > 0)
        {
            tp += INADDRSZ;
            saw_xdigit = 0;
            break; /* '\0' was seen by isIPv4(). */
        }
        return 0;
    }
    if (saw_xdigit)
    {
        if (tp + INT16SZ > endp)
            return 0;
        *tp++ = (unsigned char)(val >> 8) & 0xff;
        *tp++ = (unsigned char)val & 0xff;
    }
    if (colonp != nullptr)
    {
        const size_t n = tp - colonp;
        size_t i;

        for (i = 1; i <= n; i++)
        {
            endp[-i] = colonp[n - i];
            colonp[n - i] = 0;
        }
        tp = endp;
    }
    if (tp != endp)
        return 0;
    return 1;
}

class FunctionIsIPv4 : public IFunction
{
public:
    static constexpr auto name = "tiDBIsIPv4";
    FunctionIsIPv4() = default;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIsIPv4>(); };

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be 1.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeUInt8>();
    }
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        if (const auto * col_input = checkAndGetColumn<ColumnString>(block.getByPosition(arguments[0]).column.get()))
        {
            size_t size = block.getByPosition(arguments[0]).column->size();
            const typename ColumnString::Chars_t & data = col_input->getChars();
            const typename ColumnString::Offsets & offsets = col_input->getOffsets();

            auto col_res = ColumnUInt8::create();
            ColumnUInt8::Container & vec_res = col_res->getData();
            vec_res.resize(size);

<<<<<<< HEAD
            size_t prev_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                vec_res[i] = static_cast<UInt8>(isIPv4(reinterpret_cast<const char *>(&data[prev_offset])));
                prev_offset = offsets[i];
            }
=======
            for (size_t i = 0; i < size; ++i)
            {
                vec_res[i] = static_cast<UInt8>(isIPv4(reinterpret_cast<const char *>(&data[i == 0 ? 0 : offsets[i - 1]])));
            }
>>>>>>> cea864a529d2840edca01fdbfdb3775ca009d427

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", block.getByPosition(arguments[0]).column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

class FunctionIsIPv6 : public IFunction
{
public:
    static constexpr auto name = "tiDBIsIPv6";
    FunctionIsIPv6() = default;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIsIPv6>(); };

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be 1.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeUInt8>();
    }
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        if (const auto * col_input = checkAndGetColumn<ColumnString>(block.getByPosition(arguments[0]).column.get()))
        {
            size_t size = block.getByPosition(arguments[0]).column->size();
            const typename ColumnString::Chars_t & data = col_input->getChars();
            const typename ColumnString::Offsets & offsets = col_input->getOffsets();

            auto col_res = ColumnUInt8::create();
            ColumnUInt8::Container & vec_res = col_res->getData();
            vec_res.resize(size);

<<<<<<< HEAD
            size_t prev_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                vec_res[i] = static_cast<UInt8>(isIPv6(reinterpret_cast<const char *>(&data[prev_offset])));
                prev_offset = offsets[i];
            }
=======
            for (size_t i = 0; i < size; ++i)
            {
                vec_res[i] = static_cast<UInt8>(isIPv6(reinterpret_cast<const char *>(&data[i == 0 ? 0 : offsets[i - 1]])));
            }
>>>>>>> cea864a529d2840edca01fdbfdb3775ca009d427

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", block.getByPosition(arguments[0]).column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};
} // namespace DB
