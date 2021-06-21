#pragma once

#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <string>
#include <cstdlib>
#if __GNUC__ > 7
#include <charconv>
#endif
#include <boost/crc.hpp>
#include <zlib.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

struct CRC32Impl
{
    static void execute(const String& s, Int64& res)
    {
        // zlib crc32
        res = crc32(0, reinterpret_cast<const unsigned char*>(s.data()), s.size());
    }
    static void execute(const ColumnString* arg_col, PaddedPODArray<Int64> & res)
    {
        res.resize(arg_col->size());
        for(size_t i = 0;i < arg_col->size();++i)
        {
            execute((*arg_col)[i].get<String>(), res[i]);
        }
    }
    static void execute(const ColumnFixedString* arg_col, PaddedPODArray<Int64> & res)
    {
        res.resize(arg_col->size());
        for(size_t i = 0;i < arg_col->size();++i)
        {
            execute((*arg_col)[i].get<String>(), res[i]);
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
        if (!arguments.front()->isStringOrFixedString())
            throw Exception{
                "Illegal type " + arguments.front()->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        // tidb get int64 from crc32, so we do the same thing
        return std::make_shared<DataTypeInt64>();
    }
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const auto arg = block.getByPosition(arguments[0]).column.get();
        auto col_res = ColumnVector<Int64>::create();
        if (const auto col = checkAndGetColumn<ColumnString>(arg))
        {
            CRC32Impl::execute(col, col_res->getData());
        }
        else if(const auto col = checkAndGetColumn<ColumnFixedString>(arg))
        {
            CRC32Impl::execute(col, col_res->getData());
        }
        else
        {
            throw Exception{
                "Illegal column " + arg->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
        }
        block.getByPosition(result).column = std::move(col_res);
    }
};


struct ConvImpl
{
    static String execute(const String& arg, int from_base, int to_base)
    {
        bool is_signed = false, is_negative = false, ignore_sign = false;
        if(from_base < 0)
        {
            from_base = -from_base;
            is_signed = true;
        }
        if(to_base < 0)
        {
            to_base = -to_base;
            ignore_sign = true;
        }
        if(from_base > 36 || from_base < 2 || to_base > 36 || to_base < 2)
        {
            return arg;
        }

        #if __GNUC__ > 7
        UInt64 value;
        auto from_chars_res = std::from_chars(arg.data(), arg.data() + arg.size(), value, from_base);
        if(from_chars_res.ec != 0)
        {
            throw Exception(String("Int too big to conv: ") + (arg.c_str() + begin_pos));
        }
        #else
        auto begin_pos_iter = std::find_if_not(arg.begin(), arg.end(), isspace);
        if(begin_pos_iter == arg.end())
        {
            return "0";
        }
        if(*begin_pos_iter == '-')
        {
            is_negative = true;
            ++begin_pos_iter;
        }
        auto begin_pos = begin_pos_iter - arg.begin();
        UInt64 value = strtoull(arg.c_str() + begin_pos, nullptr, from_base);
        if(errno)
        {
            errno = 0;
            throw Exception(String("Int too big to conv: ") + (arg.c_str() + begin_pos));
        }
        #endif


        if(is_signed)
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
        if(is_negative)
        {
            value = -value;
        }

        if(static_cast<Int64>(value) < 0)
        {
            is_negative = true;
        }
        else {
            is_negative = false;
        }
        if(ignore_sign && is_negative)
        {
            value = 0 - value;
        }

        #if __GNUC__ > 7
        char buf[100] = {0};
        std::to_chars(buf, std::end(buf), value, to_base);
        String result(buf);
        #else
        String result;
        while (value != 0) {
            int digit = value % to_base;
            result += (digit > 9 ? 'A' + digit - 10 : digit +'0');
            value /= to_base;
        }
        std::reverse(result.begin(), result.end());
        #endif

        if(is_negative && ignore_sign) {
            result = "-" + result;
        }
        return result;
    }

    template <typename T1, typename T2>
    static void execute(const ColumnString* arg_col0, const std::vector<T1>* arg_col1, const std::vector<T2>* arg_col2, ColumnString& res_col)
    {
        for(size_t i = 0;i < arg_col0->size();++i)
        {
            String result = execute((*arg_col0)[i].get<String>(), (*arg_col1)[i], (*arg_col2)[i]);
            res_col.insertData(result.c_str(), result.size());
        }
    }
    template <typename T1, typename T2>
    static void executeFixed(const ColumnFixedString* arg_col0, const std::vector<T1>* arg_col1, const std::vector<T2>* arg_col2, ColumnString& res_col)
    {
        for(size_t i = 0;i < arg_col0->size();++i)
        {
            String result = execute((*arg_col0)[i].get<String>(), (*arg_col1)[i], (*arg_col2)[i]);
            res_col.insertData(result.c_str(), result.size());
        }
    }
};

class FunctionConv : public IFunction
{   
    template <typename IntType>
    struct GetIntVecHelper
    {
        static std::vector<IntType> GetVec(const ColumnConst* int_arg_typed, size_t N)
        {
            return std::vector<IntType>(N, int_arg_typed->getValue<IntType>());
        }

        static std::vector<IntType> GetVec(const ColumnVector<IntType>* int_arg_typed, size_t N)
        {
            std::vector<IntType> result;
            result.reserve(N);
            for(size_t i = 0;i < int_arg_typed->size();++i) {
                result.push_back(int_arg_typed->getElement(i));
            }
            return result;
        }
    };




    template <typename FirstIntType, typename SecondIntType, typename FirstIntColumn, typename SecondIntColumn>
    void executeWithIntTypes(Block & block, const ColumnNumbers & arguments, const size_t result, const FirstIntColumn * first_int_arg_typed,
        const SecondIntColumn * second_int_arg_typed)
    {
        const auto string_arg = block.getByPosition(arguments[0]).column.get();

        auto col_res = ColumnString::create();

        if (const auto string_col = checkAndGetColumn<ColumnString>(string_arg))
        {
            const size_t N = string_col->size();
            std::vector<FirstIntType> first_vec = GetIntVecHelper<FirstIntType>::GetVec(first_int_arg_typed, N);
            std::vector<SecondIntType> second_vec = GetIntVecHelper<SecondIntType>::GetVec(second_int_arg_typed, N);
            ConvImpl::execute(string_col, &first_vec, &second_vec, *col_res);
            block.getByPosition(result).column = std::move(col_res);
        }
        else if(const auto string_col = checkAndGetColumn<ColumnFixedString>(string_arg))
        {
            const size_t N = string_col->size();
            std::vector<FirstIntType> first_vec = GetIntVecHelper<FirstIntType>::GetVec(first_int_arg_typed, N);
            std::vector<SecondIntType> second_vec = GetIntVecHelper<SecondIntType>::GetVec(second_int_arg_typed, N);
            ConvImpl::executeFixed(string_col, &first_vec, &second_vec, *col_res);
            block.getByPosition(result).column = std::move(col_res);
        }
        else
        {
            throw Exception{
                "Illegal column " + string_arg->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
        }
    }

    template <typename FirstIntType, typename SecondIntType, typename FirstIntColumn>
    bool executeIntRight(Block & block, const ColumnNumbers & arguments, const size_t result, const FirstIntColumn * first_int_arg_typed,
        const IColumn * second_int_arg)
    {
        if (const auto second_int_arg_typed = checkAndGetColumn<ColumnVector<SecondIntType>>(second_int_arg))
        {
            executeWithIntTypes<FirstIntType, SecondIntType>(block, arguments, result, first_int_arg_typed, second_int_arg_typed);
            return true;
        }
        else if (const auto second_int_arg_typed = checkAndGetColumnConst<ColumnVector<SecondIntType>>(second_int_arg))
        {
            executeWithIntTypes<FirstIntType, SecondIntType>(block, arguments, result, first_int_arg_typed, second_int_arg_typed);
            return true;
        }

        return false;
    }

    template <typename FirstIntType>
    bool executeIntLeft(Block & block, const ColumnNumbers & arguments, const size_t result,
        const IColumn * first_int_arg)
    {
        if (const auto first_int_arg_typed = checkAndGetColumn<ColumnVector<FirstIntType>>(first_int_arg))
        {
            const auto second_int_arg = block.getByPosition(arguments[2]).column.get();

            if (executeIntRight<FirstIntType, UInt8>(block, arguments, result, first_int_arg_typed, second_int_arg) ||
                executeIntRight<FirstIntType, UInt16>(block, arguments, result, first_int_arg_typed, second_int_arg) ||
                executeIntRight<FirstIntType, UInt32>(block, arguments, result, first_int_arg_typed, second_int_arg) ||
                executeIntRight<FirstIntType, UInt64>(block, arguments, result, first_int_arg_typed, second_int_arg) ||
                executeIntRight<FirstIntType, Int8>(block, arguments, result, first_int_arg_typed, second_int_arg) ||
                executeIntRight<FirstIntType, Int16>(block, arguments, result, first_int_arg_typed, second_int_arg) ||
                executeIntRight<FirstIntType, Int32>(block, arguments, result, first_int_arg_typed, second_int_arg) ||
                executeIntRight<FirstIntType, Int64>(block, arguments, result, first_int_arg_typed, second_int_arg))
            {
                return true;
            }
            else
            {
                throw Exception{
                    "Illegal column " + block.getByPosition(arguments[1]).column->getName() +
                    " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN};
            }
        }
        else if (const auto first_int_arg_typed = checkAndGetColumnConst<ColumnVector<FirstIntType>>(first_int_arg))
        {
            const auto second_int_arg = block.getByPosition(arguments[2]).column.get();

            if (executeIntRight<FirstIntType, UInt8>(block, arguments, result, first_int_arg_typed, second_int_arg) ||
                executeIntRight<FirstIntType, UInt16>(block, arguments, result, first_int_arg_typed, second_int_arg) ||
                executeIntRight<FirstIntType, UInt32>(block, arguments, result, first_int_arg_typed, second_int_arg) ||
                executeIntRight<FirstIntType, UInt64>(block, arguments, result, first_int_arg_typed, second_int_arg) ||
                executeIntRight<FirstIntType, Int8>(block, arguments, result, first_int_arg_typed, second_int_arg) ||
                executeIntRight<FirstIntType, Int16>(block, arguments, result, first_int_arg_typed, second_int_arg) ||
                executeIntRight<FirstIntType, Int32>(block, arguments, result, first_int_arg_typed, second_int_arg) ||
                executeIntRight<FirstIntType, Int64>(block, arguments, result, first_int_arg_typed, second_int_arg))
            {
                return true;
            }
            else
            {
                throw Exception{
                    "Illegal column " + block.getByPosition(arguments[1]).column->getName() +
                    " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN};
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
        if (!arguments[0]->isStringOrFixedString())
            throw Exception{
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + " because not string",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        if (!arguments[1]->isInteger())
            throw Exception{
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + " because not integer",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        if (!arguments[2]->isInteger())
            throw Exception{
                "Illegal type " + arguments[2]->getName() + " of third argument of function " + getName() + " because not integer",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {

        const auto first_int_arg = block.getByPosition(arguments[1]).column.get();

        if (!executeIntLeft<UInt8>(block, arguments, result, first_int_arg) &&
            !executeIntLeft<UInt16>(block, arguments, result, first_int_arg) &&
            !executeIntLeft<UInt32>(block, arguments, result, first_int_arg) &&
            !executeIntLeft<UInt64>(block, arguments, result, first_int_arg) &&
            !executeIntLeft<Int8>(block, arguments, result, first_int_arg) &&
            !executeIntLeft<Int16>(block, arguments, result, first_int_arg) &&
            !executeIntLeft<Int32>(block, arguments, result, first_int_arg) &&
            !executeIntLeft<Int64>(block, arguments, result, first_int_arg))
        {
            throw Exception{
                "Illegal column " + first_int_arg->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
        }
    }
};



}