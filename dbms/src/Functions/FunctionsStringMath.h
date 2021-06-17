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
//#include <charconv>
#include <boost/crc.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

struct CRC32Impl
{
    static void execute(const char* s, size_t len, Int64& res)
    {
        boost::crc_32_type result;
        result.process_bytes(s, len);
        res = static_cast<Int64>(result.checksum());
    }
    static void execute(const ColumnString* arg_col, PaddedPODArray<Int64> & res)
    {
        res.resize(arg_col->size());
        auto data = reinterpret_cast<const char*>(arg_col->getChars().data());
        auto& offsets = arg_col->getOffsets();
        for(size_t i = 0;i < arg_col->size();++i)
        {
            const char* s = reinterpret_cast<const char*>(data + offsets[i]);
            execute(s, (i == 0 ? offsets[0] : (offsets[i] - offsets[i - 1])) - 1, res[i]);
        }
    }
    static void execute(const ColumnFixedString* arg_col, PaddedPODArray<Int64> & res)
    {
        res.resize(arg_col->size());
        auto data = reinterpret_cast<const char*>(arg_col->getChars().data());
        const size_t N = arg_col->getN();
        auto get_fixed_string_len = [N](const char* const s) -> size_t {
            const char* s_end = s + N - 1;
            while(s_end != s && *s_end == '\0') {
                s_end--;
            }
            return s_end - s + 1;
        };

        for(size_t i = 0;i < arg_col->size();++i)
        {
            const char* s = reinterpret_cast<const char*>(data + i * N);
            execute(s, get_fixed_string_len(s), res[i]);
        }
    }
};

class FunctionCRC32 : public IFunction
{
public:
    static constexpr auto name = "crc32";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionCRC32>(); }
    std::string getName() const override { return name; }
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
    static std::string execute(const std::string& arg, int from_base, int to_base)
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
            throw Exception(std::string("Int too big to conv: ") + (arg.c_str() + begin_pos));
        }


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

        std::string result;
        while (value != 0) {
            int digit = value % to_base;
            result += (digit > 9 ? 'A' + digit - 10 : digit +'0');
            value /= to_base;
        }
        std::reverse(result.begin(), result.end());

        if(is_negative && ignore_sign) {
            result = "-" + result;
        }
        return result;
    }
    template <typename T1, typename T2>
    static void execute(const ColumnString* arg_col0, const ColumnVector<T1>* arg_col1, const ColumnVector<T2>* arg_col2, ColumnString& res_col)
    {
        auto data = reinterpret_cast<const char*>(arg_col0->getChars().data());
        auto& offsets = arg_col0->getOffsets();
        for(size_t i = 0;i < arg_col0->size();++i)
        {
            const char* s = reinterpret_cast<const char*>(data + offsets[i]);
            std::string result = execute(std::string(s), arg_col1->getData()[i], arg_col2->getData()[i]);
            res_col.insertData(result.c_str(), result.size());
        }
    }
    template <typename T1, typename T2>
    static void execute(const ColumnFixedString* arg_col0, const ColumnVector<T1>* arg_col1, const ColumnVector<T2>* arg_col2, ColumnString& res_col)
    {
        auto data = reinterpret_cast<const char*>(arg_col0->getChars().data());
        const size_t N = arg_col0->getN();
        for(size_t i = 0;i < arg_col0->size();++i)
        {
            const char* s = reinterpret_cast<const char*>(data + i * N);
            std::string result = execute(std::string(s, N), arg_col1->getData()[i], arg_col2->getData()[i]);
            res_col.insertData(result.c_str(), result.size());
        }
    }
};

class FunctionConv : public IFunction
{   
    template <typename T1, typename T2>
    void executeWithBothType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        auto type1 = block.getByPosition(arguments[1]).type;
        const auto arg1 = block.getByPosition(arguments[1]).column.get();

        auto type2 = block.getByPosition(arguments[2]).type;
        const auto arg2 = block.getByPosition(arguments[2]).column.get();

        const auto col1 = checkAndGetColumn<ColumnVector<T1>>(arg1);
        if(!col1)
        {
            throw Exception{
                "Illegal type " + arg1->getName() + " of second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        const auto col2 = checkAndGetColumn<ColumnVector<T2>>(arg2);
        if(!col2)
        {
            throw Exception{
                "Illegal type " + arg2->getName() + " of third argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        const auto arg0 = block.getByPosition(arguments[0]).column.get();
        auto col_res = ColumnString::create();
        if (const auto col0 = checkAndGetColumn<ColumnString>(arg0))
        {
            ConvImpl::execute<T1, T2>(col0, col1, col2, *col_res);
        }
        else if(const auto col0 = checkAndGetColumn<ColumnFixedString>(arg0))
        {
            ConvImpl::execute<T1, T2>(col0, col1, col2, *col_res);
        }
        else
        {
            throw Exception{
                "Illegal column " + arg0->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
        }
        block.getByPosition(result).column = std::move(col_res);
    }

    template <typename T>
    void executeWithType1(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const auto arg2 = block.getByPosition(arguments[2]).column.get();
        auto type2 = block.getByPosition(arguments[2]).type;
        switch(type2->getTypeId())
        {
            case TypeIndex::UInt8:
                executeWithBothType<T, Id2Type<TypeIndex::UInt8>::type>(block, arguments, result);
                break;
            case TypeIndex::UInt16:
                executeWithBothType<T, Id2Type<TypeIndex::UInt16>::type>(block, arguments, result);
                break;
            case TypeIndex::UInt32:
                executeWithBothType<T, Id2Type<TypeIndex::UInt32>::type>(block, arguments, result);
                break;
            case TypeIndex::UInt64:
                executeWithBothType<T, Id2Type<TypeIndex::UInt64>::type>(block, arguments, result);
                break;
            case TypeIndex::Int8:
                executeWithBothType<T, Id2Type<TypeIndex::Int8>::type>(block, arguments, result);
                break;
            case TypeIndex::Int16:
                executeWithBothType<T, Id2Type<TypeIndex::Int16>::type>(block, arguments, result);
                break;
            case TypeIndex::Int32:
                executeWithBothType<T, Id2Type<TypeIndex::Int32>::type>(block, arguments, result);
                break;
            case TypeIndex::Int64:
                executeWithBothType<T, Id2Type<TypeIndex::Int64>::type>(block, arguments, result);
                break;
            default:
                throw Exception{
                    "Illegal type " + arg2->getName() + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
    }
public:
    static constexpr auto name = "conv";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionConv>(); }
    std::string getName() const override { return name; }
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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const auto arg1 = block.getByPosition(arguments[1]).column.get();
        auto type1 = block.getByPosition(arguments[1]).type;
        switch(type1->getTypeId())
        {
            case TypeIndex::UInt8:
                executeWithType1<Id2Type<TypeIndex::UInt8>::type>(block, arguments, result);
                break;
            case TypeIndex::UInt16:
                executeWithType1<Id2Type<TypeIndex::UInt16>::type>(block, arguments, result);
                break;
            case TypeIndex::UInt32:
                executeWithType1<Id2Type<TypeIndex::UInt32>::type>(block, arguments, result);
                break;
            case TypeIndex::UInt64:
                executeWithType1<Id2Type<TypeIndex::UInt64>::type>(block, arguments, result);
                break;
            case TypeIndex::Int8:
                executeWithType1<Id2Type<TypeIndex::Int8>::type>(block, arguments, result);
                break;
            case TypeIndex::Int16:
                executeWithType1<Id2Type<TypeIndex::Int16>::type>(block, arguments, result);
                break;
            case TypeIndex::Int32:
                executeWithType1<Id2Type<TypeIndex::Int32>::type>(block, arguments, result);
                break;
            case TypeIndex::Int64:
                executeWithType1<Id2Type<TypeIndex::Int64>::type>(block, arguments, result);
                break;
            default:
                throw Exception{
                    "Illegal type " + arg1->getName() + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
    }
};



}