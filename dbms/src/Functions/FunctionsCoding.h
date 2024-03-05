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

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/formatIPv6.h>
#include <Common/hex.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <arpa/inet.h>
#include <fmt/core.h>

#include <array>
#include <ext/range.h>


namespace DB
{
namespace ErrorCodes
{
extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes


/** TODO This file contains ridiculous amount of copy-paste.
  */

/** Encoding functions:
  *
  * IPv4NumToString (num) - See below.
  * IPv4StringToNum(string) - Convert, for example, '192.168.0.1' to 3232235521 and vice versa.
  *
  * hex(x) - Returns hex; capital letters; there are no prefixes 0x or suffixes h.
  *          For numbers, returns a variable-length string - hex in the "human" (big endian) format, with the leading zeros being cut,
  *          but only by whole bytes. For dates and datetimes - the same as for numbers.
  *          For example, hex(257) = '0101'.
  * unhex(string) - Returns a string, hex of which is equal to `string` with regard of case and discarding one leading zero.
  *                 If such a string does not exist, could return arbitary implementation specific value.
  *
  * bitmaskToArray(x) - Returns an array of powers of two in the binary form of x. For example, bitmaskToArray(50) = [2, 16, 32].
  */


constexpr size_t ipv4_bytes_length = 4;
constexpr size_t ipv6_bytes_length = 16;
constexpr size_t uuid_bytes_length = 16;
constexpr size_t uuid_text_length = 36;


template <size_t mask_tail_octets>
void formatIP(UInt32 ip, char *& out)
{
    char * begin = out;

    if constexpr (mask_tail_octets > 0)
    {
        for (size_t octet = 0; octet < mask_tail_octets; ++octet)
        {
            if (octet > 0)
            {
                *out = '.';
                ++out;
            }

            memcpy(out, "xxx", 3); /// Strange choice, but meets the specification.
            out += 3;
        }
    }

    /// Write everything backwards. NOTE The loop is unrolled.
    for (size_t octet = mask_tail_octets; octet < 4; ++octet)
    {
        if (octet > 0)
        {
            *out = '.';
            ++out;
        }

        /// Get the next byte.
        UInt32 value = (ip >> (octet * 8)) & static_cast<UInt32>(0xFF);

        /// Faster than sprintf. NOTE Actually not good enough. LUT will be better.
        if (value == 0)
        {
            *out = '0';
            ++out;
        }
        else
        {
            while (value > 0)
            {
                *out = '0' + value % 10;
                ++out;
                value /= 10;
            }
        }
    }

    /// And reverse.
    std::reverse(begin, out);

    *out = '\0';
    ++out;
}

class FunctionTiDBIPv6NumToString : public IFunction
{
private:
    static UInt32 extractUInt32(const UInt8 * p)
    {
        UInt32 v = *p++;
        v = (v << 8) + *p++;
        v = (v << 8) + *p++;
        v = (v << 8) + *p++;
        return v;
    }

public:
    static constexpr auto name = "tiDBIPv6NumToString";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTiDBIPv6NumToString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr data_type = removeNullable(arguments[0]);
        if (!data_type->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", data_type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return makeNullable(std::make_shared<DataTypeString>());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        auto [column, nullmap] = removeNullable(block.getByPosition(arguments[0]).column.get());

        if (const auto * col = checkAndGetColumn<ColumnString>(column))
        {
            size_t size = col->size();

            auto col_res = ColumnString::create();
            auto nullmap_res = ColumnUInt8::create();

            ColumnString::Chars_t & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            ColumnUInt8::Container & vec_res_nullmap = nullmap_res->getData();

            vec_res.resize(size * (IPV6_MAX_TEXT_LENGTH + 1)); /// 1 for trailing '\0'
            offsets_res.resize(size);
            vec_res_nullmap.assign(size, static_cast<UInt8>(0));

            char * begin = reinterpret_cast<char *>(&vec_res[0]);
            char * pos = begin;

            const ColumnString::Chars_t & vec_src = col->getChars();
            const ColumnString::Offsets & offsets_src = col->getOffsets();
            size_t prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                if (nullmap && (*nullmap)[i])
                {
                    *pos++ = '\0';
                    vec_res_nullmap[i] = 1;
                }
                else
                {
                    auto size = (i == 0 ? offsets_src[0] : offsets_src[i] - offsets_src[i - 1]) - 1;
                    if (size == ipv6_bytes_length)
                    {
                        formatIPv6(&vec_src[prev_offset], pos);
                    }
                    else if (size == ipv4_bytes_length)
                    {
                        auto v = extractUInt32(&vec_src[prev_offset]);
                        formatIP<0>(v, pos);
                    }
                    else
                    {
                        *pos++ = '\0';
                        vec_res_nullmap[i] = 1;
                    }
                }
                offsets_res[i] = pos - begin;
                prev_offset = offsets_src[i];
            }
            vec_res.resize(pos - begin);

            block.getByPosition(result).column = ColumnNullable::create(std::move(col_res), std::move(nullmap_res));
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


class FunctionCutIPv6 : public IFunction
{
public:
    static constexpr auto name = "cutIPv6";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionCutIPv6>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * ptr = checkAndGetDataType<DataTypeFixedString>(arguments[0].get());
        if (!ptr || ptr->getN() != ipv6_bytes_length)
            throw Exception(
                fmt::format(
                    "Illegal type {} of argument 1 of function {}, expected FixedString({})",
                    arguments[0]->getName(),
                    getName(),
                    ipv6_bytes_length),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!checkDataType<DataTypeUInt8>(arguments[1].get()))
            throw Exception(
                fmt::format("Illegal type {} of argument 2 of function {}", arguments[1]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!checkDataType<DataTypeUInt8>(arguments[2].get()))
            throw Exception(
                fmt::format("Illegal type {} of argument 3 of function {}", arguments[2]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        const auto & col_type_name = block.getByPosition(arguments[0]);
        const ColumnPtr & column = col_type_name.column;

        const auto & col_ipv6_zeroed_tail_bytes_type = block.getByPosition(arguments[1]);
        const auto & col_ipv6_zeroed_tail_bytes = col_ipv6_zeroed_tail_bytes_type.column;
        const auto & col_ipv4_zeroed_tail_bytes_type = block.getByPosition(arguments[2]);
        const auto & col_ipv4_zeroed_tail_bytes = col_ipv4_zeroed_tail_bytes_type.column;

        if (const auto * col_in = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (col_in->getN() != ipv6_bytes_length)
                throw Exception(
                    fmt::format(
                        "Illegal type {} of column {} argument of function {}, expected FixedString({})",
                        col_type_name.type->getName(),
                        col_in->getName(),
                        getName(),
                        ipv6_bytes_length),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto * ipv6_zeroed_tail_bytes
                = checkAndGetColumnConst<ColumnVector<UInt8>>(col_ipv6_zeroed_tail_bytes.get());
            if (!ipv6_zeroed_tail_bytes)
                throw Exception(
                    fmt::format(
                        "Illegal type {} of argument 2 of function {}",
                        col_ipv6_zeroed_tail_bytes_type.type->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            auto ipv6_zeroed_tail_bytes_count = ipv6_zeroed_tail_bytes->getValue<UInt8>();
            if (ipv6_zeroed_tail_bytes_count > ipv6_bytes_length)
                throw Exception(
                    fmt::format(
                        "Illegal value for argument 2 {} of function {}",
                        col_ipv6_zeroed_tail_bytes_type.type->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto * ipv4_zeroed_tail_bytes
                = checkAndGetColumnConst<ColumnVector<UInt8>>(col_ipv4_zeroed_tail_bytes.get());
            if (!ipv4_zeroed_tail_bytes)
                throw Exception(
                    fmt::format(
                        "Illegal type {} of argument 3 of function {}",
                        col_ipv4_zeroed_tail_bytes_type.type->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            auto ipv4_zeroed_tail_bytes_count = ipv4_zeroed_tail_bytes->getValue<UInt8>();
            if (ipv4_zeroed_tail_bytes_count > ipv6_bytes_length)
                throw Exception(
                    fmt::format(
                        "Illegal value for argument 3 {} of function {}",
                        col_ipv4_zeroed_tail_bytes_type.type->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto size = col_in->size();
            const auto & vec_in = col_in->getChars();

            auto col_res = ColumnString::create();

            ColumnString::Chars_t & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            vec_res.resize(size * (IPV6_MAX_TEXT_LENGTH + 1));
            offsets_res.resize(size);

            auto * begin = reinterpret_cast<char *>(&vec_res[0]);
            auto * pos = begin;

            for (size_t offset = 0, i = 0; offset < vec_in.size(); offset += ipv6_bytes_length, ++i)
            {
                const auto * address = &vec_in[offset];
                UInt8 zeroed_tail_bytes_count
                    = isIPv4Mapped(address) ? ipv4_zeroed_tail_bytes_count : ipv6_zeroed_tail_bytes_count;
                cutAddress(address, pos, zeroed_tail_bytes_count);
                offsets_res[i] = pos - begin;
            }

            vec_res.resize(pos - begin);

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

private:
    static bool isIPv4Mapped(const unsigned char * address)
    {
        return (*reinterpret_cast<const UInt64 *>(address) == 0)
            && ((*reinterpret_cast<const UInt64 *>(address + 8) & 0x00000000FFFFFFFFull) == 0x00000000FFFF0000ull);
    }

    static void cutAddress(const unsigned char * address, char *& dst, UInt8 zeroed_tail_bytes_count)
    {
        formatIPv6(address, dst, zeroed_tail_bytes_count);
    }
};


class FunctionIPv6StringToNum : public IFunction
{
public:
    static constexpr auto name = "IPv6StringToNum";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIPv6StringToNum>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeFixedString>(ipv6_bytes_length);
    }


    static bool ipv4Scan(const char * src, unsigned char * dst)
    {
        constexpr auto size = sizeof(UInt32);
        char bytes[size]{};

        for (const auto i : ext::range(0, size))
        {
            UInt32 value = 0;
            size_t len = 0;
            while (isNumericASCII(*src) && len <= 3)
            {
                value = value * 10 + (*src - '0');
                ++len;
                ++src;
            }

            if (len == 0 || value > 255 || (i < size - 1 && *src != '.'))
            {
                memset(dst, 0, size);
                return false;
            }
            bytes[i] = value;
            ++src;
        }

        if (src[-1] != '\0')
        {
            memset(dst, 0, size);
            return false;
        }

        memcpy(dst, bytes, sizeof(bytes));
        return true;
    }

    /// slightly altered implementation from http://svn.apache.org/repos/asf/apr/apr/trunk/network_io/unix/inet_pton.c
    static void ipv6Scan(const char * src, unsigned char * dst)
    {
        const auto clear_dst = [dst] {
            memset(dst, '\0', ipv6_bytes_length);
        };

        /// Leading :: requires some special handling.
        if (*src == ':')
            if (*++src != ':')
                return clear_dst();

        unsigned char tmp[ipv6_bytes_length]{};
        auto * tp = tmp;
        auto * endp = tp + ipv6_bytes_length;
        const auto * curtok = src;
        auto saw_xdigit = false;
        UInt32 val{};
        unsigned char * colonp = nullptr;

        /// Assuming zero-terminated string.
        while (const auto ch = *src++)
        {
            const auto num = unhex(ch);

            if (num != '\xff')
            {
                val <<= 4;
                val |= num;
                if (val > 0xffffu)
                    return clear_dst();

                saw_xdigit = true;
                continue;
            }

            if (ch == ':')
            {
                curtok = src;
                if (!saw_xdigit)
                {
                    if (colonp)
                        return clear_dst();

                    colonp = tp;
                    continue;
                }

                if (tp + sizeof(UInt16) > endp)
                    return clear_dst();

                *tp++ = static_cast<unsigned char>((val >> 8) & 0xffu);
                *tp++ = static_cast<unsigned char>(val & 0xffu);
                saw_xdigit = false;
                val = 0;
                continue;
            }

            if (ch == '.' && (tp + ipv4_bytes_length) <= endp)
            {
                if (!ipv4Scan(curtok, tp))
                    return clear_dst();

                tp += ipv4_bytes_length;
                saw_xdigit = false;
                break; /* '\0' was seen by ipv4_scan(). */
            }

            return clear_dst();
        }

        if (saw_xdigit)
        {
            if (tp + sizeof(UInt16) > endp)
                return clear_dst();

            *tp++ = static_cast<unsigned char>((val >> 8) & 0xffu);
            *tp++ = static_cast<unsigned char>(val & 0xffu);
        }

        if (colonp)
        {
            /*
             * Since some memmove()'s erroneously fail to handle
             * overlapping regions, we'll do the shift by hand.
             */
            const auto n = tp - colonp;

            for (int i = 1; i <= n; ++i)
            {
                endp[-i] = colonp[n - i];
                colonp[n - i] = 0;
            }
            tp = endp;
        }

        if (tp != endp)
            return clear_dst();

        memcpy(dst, tmp, sizeof(tmp));
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;

        if (const auto * col_in = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnFixedString::create(ipv6_bytes_length);

            auto & vec_res = col_res->getChars();
            vec_res.resize(col_in->size() * ipv6_bytes_length);

            const ColumnString::Chars_t & vec_src = col_in->getChars();
            const ColumnString::Offsets & offsets_src = col_in->getOffsets();
            size_t src_offset = 0;

            for (size_t out_offset = 0, i = 0; out_offset < vec_res.size(); out_offset += ipv6_bytes_length, ++i)
            {
                ipv6Scan(reinterpret_cast<const char *>(&vec_src[src_offset]), &vec_res[out_offset]);
                src_offset = offsets_src[i];
            }

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

class FunctionTiDBIPv6StringToNum : public IFunction
{
public:
    static constexpr auto name = "tiDBIPv6StringToNum";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTiDBIPv6StringToNum>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr data_type = removeNullable(arguments[0]);
        if (!data_type->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", data_type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return makeNullable(std::make_shared<DataTypeString>());
    }


    static bool parseIPv4(const char * src, unsigned char * dst)
    {
        constexpr auto size = sizeof(UInt32);
        char bytes[ipv4_bytes_length]{};

        for (const auto i : ext::range(0, size))
        {
            UInt32 value = 0;
            size_t len = 0;
            while (isNumericASCII(*src) && value <= 255)
            {
                value = value * 10 + (*src - '0');
                ++len;
                ++src;
            }

            if (len == 0 || value > 255 || (i < size - 1 && *src != '.'))
                return false;
            bytes[i] = value;
            ++src;
        }

        if (src[-1] != '\0')
        {
            return false;
        }

        memcpy(dst, bytes, sizeof(bytes));
        return true;
    }

    /// slightly altered implementation from http://svn.apache.org/repos/asf/apr/apr/trunk/network_io/unix/inet_pton.c
    static bool parseIPv6(const char * src, unsigned char * dst)
    {
        /// Leading :: requires some special handling.
        if (*src == ':')
            if (*++src != ':')
                return false;

        unsigned char tmp[ipv6_bytes_length]{};
        auto * tp = tmp;
        auto * endp = tp + ipv6_bytes_length;
        const auto * curtok = src;
        auto saw_xdigit = false;
        UInt32 val{};
        unsigned char * colonp = nullptr;

        /// Assuming zero-terminated string.
        while (const auto ch = *src++)
        {
            const auto num = unhex(ch);

            if (num != '\xff')
            {
                val <<= 4;
                val |= num;
                if (val > 0xffffu)
                    return false;

                saw_xdigit = true;
                continue;
            }

            if (ch == ':')
            {
                curtok = src;
                if (!saw_xdigit)
                {
                    if (colonp)
                        return false;

                    colonp = tp;
                    continue;
                }

                if (tp + sizeof(UInt16) > endp)
                    return false;

                *tp++ = static_cast<unsigned char>((val >> 8) & 0xffu);
                *tp++ = static_cast<unsigned char>(val & 0xffu);
                saw_xdigit = false;
                val = 0;
                continue;
            }

            if (ch == '.' && (tp + ipv4_bytes_length) <= endp)
            {
                if (!parseIPv4(curtok, tp))
                    return false;

                tp += ipv4_bytes_length;
                saw_xdigit = false;
                break; /* '\0' was seen by ipv4_scan(). */
            }

            return false;
        }

        if (saw_xdigit)
        {
            if (tp + sizeof(UInt16) > endp)
                return false;

            *tp++ = static_cast<unsigned char>((val >> 8) & 0xffu);
            *tp++ = static_cast<unsigned char>(val & 0xffu);
        }

        if (colonp)
        {
            /*
             * Since some memmove()'s erroneously fail to handle
             * overlapping regions, we'll do the shift by hand.
             */
            const auto n = tp - colonp;

            for (int i = 1; i <= n; ++i)
            {
                endp[-i] = colonp[n - i];
                colonp[n - i] = 0;
            }
            tp = endp;
        }

        if (tp != endp)
            return false;

        memcpy(dst, tmp, sizeof(tmp));
        return ipv6_bytes_length;
    }


    /// Need to return NULL for invalid input.
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto [column, nullmap] = removeNullable(block.getByPosition(arguments[0]).column.get());

        if (const auto * col = checkAndGetColumn<ColumnString>(column))
        {
            size_t size = col->size();

            auto col_res = ColumnString::create();
            auto nullmap_res = ColumnUInt8::create();

            ColumnString::Chars_t & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            ColumnUInt8::Container & vec_res_nullmap = nullmap_res->getData();

            vec_res.resize(size * (ipv6_bytes_length + 1)); /// 1 for trailing '\0'
            offsets_res.resize(size);
            vec_res_nullmap.assign(size, static_cast<UInt8>(0));

            auto * begin = reinterpret_cast<unsigned char *>(&vec_res[0]);
            unsigned char * pos = begin;

            const ColumnString::Chars_t & vec_src = col->getChars();
            const ColumnString::Offsets & offsets_src = col->getOffsets();
            size_t prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                if (nullmap && (*nullmap)[i])
                {
                    *pos++ = '\0';
                    vec_res_nullmap[i] = 1;
                    offsets_res[i] = pos - begin;
                }
                else
                {
                    const auto * src = reinterpret_cast<const char *>(&vec_src[prev_offset]);
                    if (parseIPv6(src, pos))
                        pos += ipv6_bytes_length;
                    else if (parseIPv4(src, pos))
                        pos += ipv4_bytes_length;
                    else
                        vec_res_nullmap[i] = 1;
                    *pos++ = '\0';
                    offsets_res[i] = pos - begin;
                }
                prev_offset = offsets_src[i];
            }
            vec_res.resize(pos - begin);

            block.getByPosition(result).column = ColumnNullable::create(std::move(col_res), std::move(nullmap_res));
        }
        else
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

/** If mask_tail_octets > 0, the last specified number of octets will be filled with "xxx".
  */
template <size_t mask_tail_octets, typename Name>
class FunctionIPv4NumToString : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionIPv4NumToString<mask_tail_octets, Name>>();
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) const override { return mask_tail_octets == 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->isInteger())
            return makeNullable(std::make_shared<DataTypeString>());
        throw Exception(
            fmt::format(
                "Illegal type {} of argument of function {}, expected integer",
                arguments[0]->getName(),
                getName()),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    template <typename ColumnContainer>
    static void executeImplColumnInteger(Block & block, const ColumnContainer & vec_in, size_t result)
    {
        auto col_res = ColumnString::create();
        auto nullmap_res = ColumnUInt8::create();
        ColumnString::Chars_t & vec_res = col_res->getChars();
        ColumnString::Offsets & offsets_res = col_res->getOffsets();
        ColumnUInt8::Container & vec_res_nullmap = nullmap_res->getData();

        vec_res.resize(vec_in.size() * (IPV4_MAX_TEXT_LENGTH + 1)); /// the longest value is: 255.255.255.255\0
        offsets_res.resize(vec_in.size());
        vec_res_nullmap.assign(vec_in.size(), static_cast<UInt8>(0));

        char * begin = reinterpret_cast<char *>(&vec_res[0]);
        char * pos = begin;

        for (size_t i = 0; i < vec_in.size(); ++i)
        {
            auto && value = vec_in[i];
            if (/*always `false` for unsigned integer*/ value < 0
                || /*auto optimized by compiler*/ static_cast<UInt64>(value) > std::numeric_limits<UInt32>::max())
            {
                *pos++ = 0;
                vec_res_nullmap[i] = 1;
            }
            else
            {
                formatIP<mask_tail_octets>(static_cast<UInt32>(value), pos);
            }
            offsets_res[i] = pos - begin;
        }

        vec_res.resize(pos - begin);
        block.getByPosition(result).column = ColumnNullable::create(std::move(col_res), std::move(nullmap_res));
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;

#define DISPATCH(ColType)                                                   \
    else if (const auto * col = typeid_cast<const ColType *>(column.get())) \
    {                                                                       \
        const typename ColType::Container & vec_in = col->getData();        \
        executeImplColumnInteger(block, vec_in, result);                    \
    }

        if (false) {} // NOLINT
        DISPATCH(ColumnUInt64)
        DISPATCH(ColumnInt64)
        DISPATCH(ColumnUInt32)
        DISPATCH(ColumnInt32)
        DISPATCH(ColumnUInt16)
        DISPATCH(ColumnInt16)
        DISPATCH(ColumnUInt8)
        DISPATCH(ColumnInt8)
        else
        {
            throw Exception(
                fmt::format(
                    "Illegal column {} of argument of function {}",
                    block.getByPosition(arguments[0]).column->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        }
#undef DISPATCH
    }
};


class FunctionIPv4StringToNum : public IFunction
{
public:
    static constexpr auto name = "IPv4StringToNum";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIPv4StringToNum>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt32>();
    }

    static UInt32 parseIPv4(const char * pos)
    {
        UInt32 res = 0;
        for (int offset = 24; offset >= 0; offset -= 8)
        {
            UInt32 value = 0;
            size_t len = 0;
            while (isNumericASCII(*pos) && len <= 3)
            {
                value = value * 10 + (*pos - '0');
                ++len;
                ++pos;
            }
            if (len == 0 || value > 255 || (offset > 0 && *pos != '.'))
                return 0;
            res |= value << offset;
            ++pos;
        }
        if (*(pos - 1) != '\0')
            return 0;
        return res;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;

        if (const auto * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnUInt32::create();

            ColumnUInt32::Container & vec_res = col_res->getData();
            vec_res.resize(col->size());

            const ColumnString::Chars_t & vec_src = col->getChars();
            const ColumnString::Offsets & offsets_src = col->getOffsets();
            size_t prev_offset = 0;

            for (size_t i = 0; i < vec_res.size(); ++i)
            {
                vec_res[i] = parseIPv4(reinterpret_cast<const char *>(&vec_src[prev_offset]));
                prev_offset = offsets_src[i];
            }

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

class FunctionTiDBIPv4StringToNum : public IFunction
{
public:
    static constexpr auto name = "tiDBIPv4StringToNum";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTiDBIPv4StringToNum>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr data_type = removeNullable(arguments[0]);
        if (!data_type->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", data_type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return makeNullable(std::make_shared<DataTypeUInt32>());
    }

    /// return <result, is_invalid>
    /// Port from https://github.com/pingcap/tidb/blob/6063386a9d164399924ef046de76e8fa4b3dd91d/expression/builtin_miscellaneous.go#L417
    static std::tuple<UInt32, UInt8> parseIPv4(const char * pos, size_t size)
    {
        // ip address should not end with '.'.
        if (size == 0 || pos[size - 1] == '.')
            return {0, 1};

        UInt32 result = 0;
        UInt32 value = 0;
        int dot_count = 0;
        for (size_t i = 0; i < size; ++i)
        {
            auto c = pos[i];
            if (isNumericASCII(c))
            {
                value = value * 10 + (c - '0');
                if (value > 255)
                    return {0, 1};
            }
            else if (c == '.')
            {
                ++dot_count;
                if (dot_count > 3)
                    return {0, 1};
                result = (result << 8) + value;
                value = 0;
            }
            else
                return {0, 1};
        }
        // 127          -> 0.0.0.127
        // 127.255      -> 127.0.0.255
        // 127.256      -> NULL
        // 127.2.1      -> 127.2.0.1
        switch (dot_count)
        {
        case 1:
            result <<= 24;
            break;
        case 2:
            result <<= 16;
            break;
        case 3:
            result <<= 8;
            break;
        }
        return {result + value, 0};
    }

    /// Need to return NULL for invalid input.
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        auto [column, nullmap] = removeNullable(block.getByPosition(arguments[0]).column.get());

        if (const auto * col = checkAndGetColumn<ColumnString>(column))
        {
            auto col_res = ColumnUInt32::create();
            auto nullmap_res = ColumnUInt8::create();

            ColumnUInt32::Container & vec_res = col_res->getData();
            ColumnUInt8::Container & vec_res_nullmap = nullmap_res->getData();
            vec_res.resize(col->size());
            vec_res_nullmap.assign(col->size(), static_cast<UInt8>(0));

            const ColumnString::Chars_t & vec_src = col->getChars();
            const ColumnString::Offsets & offsets_src = col->getOffsets();
            size_t prev_offset = 0;

            for (size_t i = 0; i < vec_res.size(); ++i)
            {
                if (nullmap && (*nullmap)[i])
                {
                    vec_res_nullmap[i] = 1;
                }
                else
                {
                    const auto * p = reinterpret_cast<const char *>(&vec_src[prev_offset]);
                    /// discard the trailing '\0'
                    auto size = (i == 0 ? offsets_src[0] : offsets_src[i] - offsets_src[i - 1]) - 1;
                    std::tie(vec_res[i], vec_res_nullmap[i]) = parseIPv4(p, size);
                }
                prev_offset = offsets_src[i];
            }

            block.getByPosition(result).column = ColumnNullable::create(std::move(col_res), std::move(nullmap_res));
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

class FunctionIPv4ToIPv6 : public IFunction
{
public:
    static constexpr auto name = "IPv4ToIPv6";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIPv4ToIPv6>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!checkAndGetDataType<DataTypeUInt32>(arguments[0].get()))
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeFixedString>(16);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        const auto & col_type_name = block.getByPosition(arguments[0]);
        const ColumnPtr & column = col_type_name.column;

        if (const auto * col_in = typeid_cast<const ColumnUInt32 *>(column.get()))
        {
            auto col_res = ColumnFixedString::create(ipv6_bytes_length);

            auto & vec_res = col_res->getChars();
            vec_res.resize(col_in->size() * ipv6_bytes_length);

            const auto & vec_in = col_in->getData();

            for (size_t out_offset = 0, i = 0; out_offset < vec_res.size(); out_offset += ipv6_bytes_length, ++i)
                mapIPv4ToIPv6(vec_in[i], &vec_res[out_offset]);

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

private:
    static void mapIPv4ToIPv6(UInt32 in, unsigned char * buf)
    {
        *reinterpret_cast<UInt64 *>(buf) = 0;
        *reinterpret_cast<UInt64 *>(buf + 8) = 0x00000000FFFF0000ull | (static_cast<UInt64>(ntohl(in)) << 32);
    }
};

class FunctionHex : public IFunction
{
public:
    static constexpr auto name = "hex";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionHex>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString() && !arguments[0]->isFixedString() && !arguments[0]->isDateOrDateTime()
            && !checkDataType<DataTypeUInt8>(&*arguments[0]) && !checkDataType<DataTypeUInt16>(&*arguments[0])
            && !checkDataType<DataTypeUInt32>(&*arguments[0]) && !checkDataType<DataTypeUInt64>(&*arguments[0]))
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    template <typename T>
    void executeOneUInt(T x, char *& out) const
    {
        bool was_nonzero = false;
        for (int offset = (sizeof(T) - 1) * 8; offset >= 0; offset -= 8)
        {
            UInt8 byte = x >> offset;

            /// Leading zeros.
            if (byte == 0 && !was_nonzero && offset)
                continue;

            was_nonzero = true;

            writeHexByteUppercase(byte, out);
            out += 2;
        }
        *out = '\0';
        ++out;
    }

    template <typename T>
    bool tryExecuteUInt(const IColumn * col, ColumnPtr & col_res) const
    {
        const auto * col_vec = checkAndGetColumn<ColumnVector<T>>(col);

        static constexpr size_t MAX_UINT_HEX_LENGTH = sizeof(T) * 2 + 1; /// Including trailing zero byte.

        if (col_vec)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars_t & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const typename ColumnVector<T>::Container & in_vec = col_vec->getData();

            size_t size = in_vec.size();
            out_offsets.resize(size);
            out_vec.resize(size * 3 + MAX_UINT_HEX_LENGTH); /// 3 is length of one byte in hex plus zero byte.

            size_t pos = 0;
            for (size_t i = 0; i < size; ++i)
            {
                /// Manual exponential growth, so as not to rely on the linear amortized work time of `resize` (no one guarantees it).
                if (pos + MAX_UINT_HEX_LENGTH > out_vec.size())
                    out_vec.resize(out_vec.size() * 2 + MAX_UINT_HEX_LENGTH);

                char * begin = reinterpret_cast<char *>(&out_vec[pos]);
                char * end = begin;
                executeOneUInt<T>(in_vec[i], end);

                pos += end - begin;
                out_offsets[i] = pos;
            }

            out_vec.resize(pos);

            col_res = std::move(col_str);
            return true;
        }
        else
        {
            return false;
        }
    }

    static void executeOneString(const UInt8 * pos, const UInt8 * end, char *& out)
    {
        while (pos < end)
        {
            writeHexByteUppercase(*pos, out);
            ++pos;
            out += 2;
        }
        *out = '\0';
        ++out;
    }

    static bool tryExecuteString(const IColumn * col, ColumnPtr & col_res)
    {
        const auto * col_str_in = checkAndGetColumn<ColumnString>(col);

        if (col_str_in)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars_t & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars_t & in_vec = col_str_in->getChars();
            const ColumnString::Offsets & in_offsets = col_str_in->getOffsets();

            size_t size = in_offsets.size();
            out_offsets.resize(size);
            out_vec.resize(in_vec.size() * 2 - size);

            char * begin = reinterpret_cast<char *>(&out_vec[0]);
            char * pos = begin;
            size_t prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                size_t new_offset = in_offsets[i];

                executeOneString(&in_vec[prev_offset], &in_vec[new_offset - 1], pos);

                out_offsets[i] = pos - begin;

                prev_offset = new_offset;
            }

            if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
                throw Exception("Column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);

            col_res = std::move(col_str);
            return true;
        }
        else
        {
            return false;
        }
    }

    static bool tryExecuteFixedString(const IColumn * col, ColumnPtr & col_res)
    {
        const auto * col_fstr_in = checkAndGetColumn<ColumnFixedString>(col);

        if (col_fstr_in)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars_t & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars_t & in_vec = col_fstr_in->getChars();

            size_t size = col_fstr_in->size();

            out_offsets.resize(size);
            out_vec.resize(in_vec.size() * 2 + size);

            char * begin = reinterpret_cast<char *>(&out_vec[0]);
            char * pos = begin;

            size_t n = col_fstr_in->getN();

            size_t prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                size_t new_offset = prev_offset + n;

                executeOneString(&in_vec[prev_offset], &in_vec[new_offset], pos);

                out_offsets[i] = pos - begin;
                prev_offset = new_offset;
            }

            if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
                throw Exception("Column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);

            col_res = std::move(col_str);
            return true;
        }
        else
        {
            return false;
        }
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const IColumn * column = block.getByPosition(arguments[0]).column.get();
        ColumnPtr & res_column = block.getByPosition(result).column;

        if (tryExecuteUInt<UInt8>(column, res_column) || tryExecuteUInt<UInt16>(column, res_column)
            || tryExecuteUInt<UInt32>(column, res_column) || tryExecuteUInt<UInt64>(column, res_column)
            || tryExecuteString(column, res_column) || tryExecuteFixedString(column, res_column))
            return;

        throw Exception(
            fmt::format(
                "Illegal column {} of argument of function {}",
                block.getByPosition(arguments[0]).column->getName(),
                getName()),
            ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionUnhex : public IFunction
{
public:
    static constexpr auto name = "unhex";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionUnhex>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    static void unhexOne(const char * pos, const char * end, char *& out)
    {
        if ((end - pos) & 1)
        {
            *out = unhex(*pos);
            ++out;
            ++pos;
        }
        while (pos < end)
        {
            *out = unhex2(pos);
            pos += 2;
            ++out;
        }
        *out = '\0';
        ++out;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;

        if (const auto * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();

            ColumnString::Chars_t & out_vec = col_res->getChars();
            ColumnString::Offsets & out_offsets = col_res->getOffsets();

            const ColumnString::Chars_t & in_vec = col->getChars();
            const ColumnString::Offsets & in_offsets = col->getOffsets();

            size_t size = in_offsets.size();
            out_offsets.resize(size);
            out_vec.resize(in_vec.size() / 2 + size);

            char * begin = reinterpret_cast<char *>(&out_vec[0]);
            char * pos = begin;
            size_t prev_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                size_t new_offset = in_offsets[i];

                unhexOne(
                    reinterpret_cast<const char *>(&in_vec[prev_offset]),
                    reinterpret_cast<const char *>(&in_vec[new_offset - 1]),
                    pos);

                out_offsets[i] = pos - begin;

                prev_offset = new_offset;
            }

            out_vec.resize(pos - begin);

            block.getByPosition(result).column = std::move(col_res);
        }
        else
        {
            throw Exception(
                fmt::format(
                    "Illegal column {} of argument of function {}",
                    block.getByPosition(arguments[0]).column->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};

} // namespace DB
