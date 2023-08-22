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

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <fmt/core.h>

#include <cstdlib>
#include <string>
#if __has_include(<charconv>)
#include <charconv>
#endif
#include <zlib.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

struct CRC32Impl
{
    static void execute(const StringRef & s, Int64 & res)
    {
        // zlib crc32
        res = crc32(0, reinterpret_cast<const unsigned char *>(s.data), s.size);
    }
    template <typename Column>
    static void execute(const Column * arg_col, PaddedPODArray<Int64> & res)
    {
        res.resize(arg_col->size());
        for (size_t i = 0; i < arg_col->size(); ++i)
        {
            execute(arg_col->getDataAt(i), res[i]);
        }
    }
};

class FunctionCRC32 : public IFunction
{
public:
    static constexpr auto name = "crc32";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionCRC32>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments.front()->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments.front()->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        // tidb get int64 from crc32, so we do the same thing
        return std::make_shared<DataTypeInt64>();
    }
    bool useDefaultImplementationForConstants() const override { return true; }
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto * arg = block.getByPosition(arguments[0]).column.get();
        auto col_res = ColumnVector<Int64>::create();
        if (const auto * col = checkAndGetColumn<ColumnString>(arg))
        {
            CRC32Impl::execute(col, col_res->getData());
        }
        else
        {
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", arg->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        }
        block.getByPosition(result).column = std::move(col_res);
    }
};


struct ConvImpl
{
    static String execute(const String & arg, int from_base, int to_base)
    {
        bool is_signed = false, is_negative = false, ignore_sign = false;
        if (from_base < 0)
        {
            from_base = -from_base;
            is_signed = true;
        }
        if (to_base < 0)
        {
            to_base = -to_base;
            ignore_sign = true;
        }
        if (from_base > 36 || from_base < 2 || to_base > 36 || to_base < 2)
        {
            return arg;
        }

        auto begin_pos_iter = std::find_if_not(arg.begin(), arg.end(), isspace);
        if (begin_pos_iter == arg.end())
        {
            return "0";
        }
        if (*begin_pos_iter == '-')
        {
            is_negative = true;
            ++begin_pos_iter;
        }
        auto begin_pos = begin_pos_iter - arg.begin();

#if __has_include(<charconv>)
        UInt64 value;
        auto from_chars_res = std::from_chars(arg.data() + begin_pos, arg.data() + arg.size(), value, from_base);
        if (from_chars_res.ec != std::errc{})
        {
            throw Exception(fmt::format("Int too big to conv: {}", arg.c_str() + begin_pos));
        }
#else
        UInt64 value = strtoull(arg.c_str() + begin_pos, nullptr, from_base);
        if (errno)
        {
            errno = 0;
            throw Exception(fmt::format("Int too big to conv: {}", arg.c_str() + begin_pos));
        }
#endif


        if (is_signed)
        {
            if (is_negative && value > static_cast<UInt64>(std::numeric_limits<Int64>::max()) + 1)
            {
                value = static_cast<UInt64>(std::numeric_limits<Int64>::max()) + 1;
            }
            if (!is_negative && value > std::numeric_limits<Int64>::max())
            {
                value = std::numeric_limits<Int64>::max();
            }
        }
        if (is_negative)
        {
            value = -value;
        }

        is_negative = static_cast<Int64>(value) < 0;
        if (ignore_sign && is_negative)
        {
            value = 0 - value;
        }

#if __has_include(<charconv>)
        char buf[100] = {0};
        std::to_chars(buf, std::end(buf), value, to_base);
        String result(buf);
        for (char & c : result)
        {
            c = toupper(c);
        }
#else
        String result;
        while (value != 0)
        {
            int digit = value % to_base;
            result += (digit > 9 ? 'A' + digit - 10 : digit + '0');
            value /= to_base;
        }
        std::reverse(result.begin(), result.end());
#endif

        if (is_negative && ignore_sign)
        {
            result = "-" + result;
        }
        return result;
    }

    template <typename T1, typename T2, typename Column>
    static void execute(
        const Column * arg_col0,
        const std::unique_ptr<IGetVecHelper<T1>> & arg_col1,
        const std::unique_ptr<IGetVecHelper<T2>> & arg_col2,
        ColumnString & res_col)
    {
        for (size_t i = 0; i < arg_col0->size(); ++i)
        {
            // we want some std::string operation in ConvImpl so we call toString here
            String result = execute(arg_col0->getDataAt(i).toString(), arg_col1->get(i), arg_col2->get(i));
            res_col.insertData(result.c_str(), result.size());
        }
    }
};

class FunctionConv : public IFunction
{
    template <typename FirstIntType, typename SecondIntType, typename FirstIntColumn, typename SecondIntColumn>
    void executeWithIntTypes(
        Block & block,
        const ColumnNumbers & arguments,
        const size_t result,
        const FirstIntColumn * first_int_arg_typed,
        const SecondIntColumn * second_int_arg_typed) const
    {
        const auto * string_arg = block.getByPosition(arguments[0]).column.get();

        auto col_res = ColumnString::create();

        if (const auto * string_col = checkAndGetColumn<ColumnString>(string_arg))
        {
            auto first_int_vec_helper = IGetVecHelper<FirstIntType>::getHelper(first_int_arg_typed);
            auto second_int_vec_helper = IGetVecHelper<SecondIntType>::getHelper(second_int_arg_typed);
            ConvImpl::execute(string_col, first_int_vec_helper, second_int_vec_helper, *col_res);
            block.getByPosition(result).column = std::move(col_res);
        }
        else
        {
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", string_arg->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    template <typename FirstIntType, typename SecondIntType, typename FirstIntColumn>
    bool executeIntRight(
        Block & block,
        const ColumnNumbers & arguments,
        const size_t result,
        const FirstIntColumn * first_int_arg_typed,
        const IColumn * second_int_arg) const
    {
        if (const auto * second_int_arg_typed = checkAndGetColumn<ColumnVector<SecondIntType>>(second_int_arg))
        {
            executeWithIntTypes<FirstIntType, SecondIntType>(
                block,
                arguments,
                result,
                first_int_arg_typed,
                second_int_arg_typed);
            return true;
        }
        else if (
            const auto * second_int_arg_typed = checkAndGetColumnConst<ColumnVector<SecondIntType>>(second_int_arg))
        {
            executeWithIntTypes<FirstIntType, SecondIntType>(
                block,
                arguments,
                result,
                first_int_arg_typed,
                second_int_arg_typed);
            return true;
        }

        return false;
    }

    template <typename FirstIntType>
    bool executeIntLeft(
        Block & block,
        const ColumnNumbers & arguments,
        const size_t result,
        const IColumn * first_int_arg) const
    {
        if (const auto first_int_arg_typed = checkAndGetColumn<ColumnVector<FirstIntType>>(first_int_arg))
        {
            const auto * second_int_arg = block.getByPosition(arguments[2]).column.get();

            if (executeIntRight<FirstIntType, UInt8>(block, arguments, result, first_int_arg_typed, second_int_arg)
                || executeIntRight<FirstIntType, UInt16>(block, arguments, result, first_int_arg_typed, second_int_arg)
                || executeIntRight<FirstIntType, UInt32>(block, arguments, result, first_int_arg_typed, second_int_arg)
                || executeIntRight<FirstIntType, UInt64>(block, arguments, result, first_int_arg_typed, second_int_arg)
                || executeIntRight<FirstIntType, Int8>(block, arguments, result, first_int_arg_typed, second_int_arg)
                || executeIntRight<FirstIntType, Int16>(block, arguments, result, first_int_arg_typed, second_int_arg)
                || executeIntRight<FirstIntType, Int32>(block, arguments, result, first_int_arg_typed, second_int_arg)
                || executeIntRight<FirstIntType, Int64>(block, arguments, result, first_int_arg_typed, second_int_arg))
            {
                return true;
            }
            else
            {
                throw Exception(
                    fmt::format(
                        "Illegal column {} of second argument of function {}",
                        block.getByPosition(arguments[1]).column->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_COLUMN);
            }
        }
        else if (const auto first_int_arg_typed = checkAndGetColumnConst<ColumnVector<FirstIntType>>(first_int_arg))
        {
            const auto * second_int_arg = block.getByPosition(arguments[2]).column.get();

            if (executeIntRight<FirstIntType, UInt8>(block, arguments, result, first_int_arg_typed, second_int_arg)
                || executeIntRight<FirstIntType, UInt16>(block, arguments, result, first_int_arg_typed, second_int_arg)
                || executeIntRight<FirstIntType, UInt32>(block, arguments, result, first_int_arg_typed, second_int_arg)
                || executeIntRight<FirstIntType, UInt64>(block, arguments, result, first_int_arg_typed, second_int_arg)
                || executeIntRight<FirstIntType, Int8>(block, arguments, result, first_int_arg_typed, second_int_arg)
                || executeIntRight<FirstIntType, Int16>(block, arguments, result, first_int_arg_typed, second_int_arg)
                || executeIntRight<FirstIntType, Int32>(block, arguments, result, first_int_arg_typed, second_int_arg)
                || executeIntRight<FirstIntType, Int64>(block, arguments, result, first_int_arg_typed, second_int_arg))
            {
                return true;
            }
            else
            {
                throw Exception(
                    fmt::format(
                        "Illegal column {} of second argument of function {}",
                        block.getByPosition(arguments[1]).column->getName(),
                        getName()),
                    ErrorCodes::ILLEGAL_COLUMN);
            }
        }

        return false;
    }

public:
    static constexpr auto name = "conv";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionConv>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                fmt::format(
                    "Illegal type {} of first argument of function {} because not string",
                    arguments[0]->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!arguments[1]->isInteger())
            throw Exception(
                fmt::format(
                    "Illegal type {} of second argument of function {} because not integer",
                    arguments[1]->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!arguments[2]->isInteger())
            throw Exception(
                fmt::format(
                    "Illegal type {} of third argument of function {} because not integer",
                    arguments[1]->getName(),
                    getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        const auto * first_int_arg = block.getByPosition(arguments[1]).column.get();

        if (!executeIntLeft<UInt8>(block, arguments, result, first_int_arg)
            && !executeIntLeft<UInt16>(block, arguments, result, first_int_arg)
            && !executeIntLeft<UInt32>(block, arguments, result, first_int_arg)
            && !executeIntLeft<UInt64>(block, arguments, result, first_int_arg)
            && !executeIntLeft<Int8>(block, arguments, result, first_int_arg)
            && !executeIntLeft<Int16>(block, arguments, result, first_int_arg)
            && !executeIntLeft<Int32>(block, arguments, result, first_int_arg)
            && !executeIntLeft<Int64>(block, arguments, result, first_int_arg))
        {
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", first_int_arg->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};

} // namespace DB
