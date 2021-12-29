#include <Functions/DivisionUtils.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/LeastGreatestGeneric.h>
#include <fmt/core.h>

#include <cstddef>
#include <cstring>

#include "Common/Exception.h"
#include "Common/MyTime.h"
#include "Core/Field.h"
#include "common/types.h"

namespace DB
{
template <typename A, typename B>
struct LeastBaseImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfTiDBLeast<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        /** gcc 4.9.2 successfully vectorizes a loop from this function. */
        return static_cast<Result>(a) < static_cast<Result>(b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }


    static int compareDate(String left, String right, bool & left_is_date, bool & right_is_date)
    {
        UInt64 left_value = 0;
        UInt64 right_value = 0;
        Field left_field;
        Field right_field;
        left_field = parseMyDateTime(left, getFsp(left));
        left_is_date = true;
        right_field = parseMyDateTime(right, getFsp(right));
        right_is_date = true;
        left_value = get<UInt64>(left_field);
        right_value = get<UInt64>(right_field);
        if (left_value < right_value)
            return -1;
        else
            return 1;
        return left_value > right_value;
    }

    static int compareDate(String left, String right)
    {
        UInt64 left_value = 0;
        UInt64 right_value = 0;

        try
        {
            left_value = get<UInt64>(parseMyDateTime(left, getFsp(left)));
            // left_is_date = true;
        }
        catch (...)
        {
        }
        right_value = get<UInt64>(parseMyDateTime(right, getFsp(right)));
        // right_is_date = true;
        if (left_value < right_value)
            return -1;
        else
            return 1;
        return left_value > right_value;
    }

    static void processImplWithoutCollator(size_t a_size, size_t b_size, const unsigned char * a_data, const unsigned char * b_data, ColumnString::Chars_t & c_data, ColumnString::Offsets & c_offsets)
    {
        std::cout << "process without collator" << std::endl;
        int res;
        std::cout << "a_size = " << a_size << std::endl;
        std::cout << "a_size = " << b_size << std::endl;

        StringRef s_ra(&a_data[0], a_size);
        StringRef s_rb(&b_data[0], b_size);
        bool left_is_date = false;
        bool right_is_date = false;
        try
        {
            res = compareDate(s_ra.toString(), s_rb.toString(), left_is_date, right_is_date);
        }
        catch (...)
        {
            std::cout << "Not all date type...." << std::endl;
            res = memcmp(&a_data[0], &b_data[0], std::min(a_size, b_size));
        }
        // std::cout << "res: " << compareDate(s_ra.toString(), s_rb.toString(), left_is_date, right_is_date) << std::endl;
        if (res < 0)
        {
            std::cout << "c_offsets.back():" << c_offsets.back() << std::endl;
            if (left_is_date)
            {
                auto s = s_ra.toString();
                MyDateTime time = MyDateTime(get<UInt64>(parseMyDateTime(s)), getFsp(s));
                size_t size = time.toString(getFsp(s)).size();
                memcpy(&c_data[c_offsets.back()], &time.toString(getFsp(s)).c_str()[0], size + 1);
                c_offsets.push_back(c_offsets.back() + size + 1);
            }
            else
            {
                memcpy(&c_data[c_offsets.back()], &a_data[0], a_size);
                c_offsets.push_back(c_offsets.back() + a_size + 1);
            }
            std::cout << "reach here...." << std::endl;
        }
        else
        {
            std::cout << "c_offsets.back():" << c_offsets.back() << std::endl;
            if (right_is_date)
            {
                auto s = s_rb.toString();
                MyDateTime time = MyDateTime(get<UInt64>(parseMyDateTime(s_rb.toString())), getFsp(s));
                size_t size = time.toString(getFsp(s)).size(); // todo
                memcpy(&c_data[c_offsets.back()], &time.toString(getFsp(s)).c_str()[0], size + 1); // todo refactor
                c_offsets.push_back(c_offsets.back() + size + 1);
            }
            else
            {
                memcpy(&c_data[c_offsets.back()], &b_data[0], b_size);
                c_offsets.push_back(c_offsets.back() + b_size + 1);
            }
            std::cout << "reach here...." << std::endl;
        }
    }

    static void processImplWithCollator(const TiDB::TiDBCollatorPtr & collator, size_t a_size, size_t b_size, const unsigned char * a_data, const unsigned char * b_data, ColumnString::Chars_t & c_data, ColumnString::Offsets & c_offsets)
    {
        std::cout << "process with collator" << std::endl;
        int res;
        StringRef s_ra(&a_data[0], a_size);
        StringRef s_rb(&b_data[0], b_size);
        bool left_is_date = false;
        bool right_is_date = false;
        try
        {
            res = compareDate(s_ra.toString(), s_rb.toString(), left_is_date, right_is_date);
        }
        catch (...)
        {
            res = collator->compare(reinterpret_cast<const char *>(&a_data[0]), a_size, reinterpret_cast<const char *>(&b_data[0]), b_size);
        }
        // std::cout << "res: " << compareDate(s_ra.toString(), s_rb.toString(), left_is_date, right_is_date) << std::endl;
        if (res < 0)
        {
            if (left_is_date)
            {
                MyDateTime time = MyDateTime(get<UInt64>(parseMyDateTime(s_ra.toString())), 6);
                size_t size = time.toString(time.getFsp()).size();
                memcpy(&c_data[0], &time.toString(time.getFsp()).c_str()[0], size); // todo refactor
                c_offsets.push_back(c_offsets.back() + size + 1);
            }
            else
            {
                memcpy(&c_data[0], &a_data[0], a_size);
                c_offsets.push_back(c_offsets.back() + a_size + 1);
            }
        }
        else
        {
            if (right_is_date)
            {
                MyDateTime time = MyDateTime(get<UInt64>(parseMyDateTime(s_rb.toString())), 6); // todo
                size_t size = time.toString(time.getFsp()).size(); // todo
                memcpy(&c_data[0], &time.toString(time.getFsp()).c_str()[0], size);
                c_offsets.push_back(c_offsets.back() + size + 1);
            }
            else
            {
                memcpy(&c_data[0], &b_data[0], b_size);
                c_offsets.push_back(c_offsets.back() + b_size + 1);
            }
        }
    }

    // string_string
    static void
    process(
        const TiDB::TiDBCollatorPtr & collator,
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets,
        size_t i)
    {
        std::cout << "string string with collator" << std::endl;
        size_t a_size;
        size_t b_size;
        std::cout << "i = " << i << std::endl;
        if (i == 0)
        {
            a_size = a_offsets[0] - 1;
            b_size = b_offsets[0] - 1;
            processImplWithCollator(collator, a_size, b_size, &a_data[0], &b_data[0], c_data, c_offsets);
        }
        else
        {
            a_size = a_offsets[i] - a_offsets[i - 1] - 1;
            b_size = b_offsets[i] - b_offsets[i - 1] - 1;
            processImplWithCollator(collator, a_size, b_size, &a_data[a_offsets[i - 1]], &b_data[b_offsets[i - 1]], c_data, c_offsets);
        }
    }

    // string_constant
    static void process(
        const TiDB::TiDBCollatorPtr & collator,
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const String & b,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets,
        size_t i)
    {
        std::cout << "string constant with collator" << std::endl;
        const unsigned char * b_data = reinterpret_cast<const unsigned char *>(b.data());
        ColumnString::Offset b_size = b.size();
        size_t a_size;
        if (i == 0)
        {
            a_size = a_offsets[0] - 1;
            processImplWithCollator(collator, a_size, b_size, &a_data[0], &b_data[0], c_data, c_offsets);
        }
        else
        {
            a_size = a_offsets[i] - a_offsets[i - 1] - 1;
            processImplWithCollator(collator, a_size, b_size, &a_data[a_offsets[i - 1]], &b_data[0], c_data, c_offsets);
        }
    }

    // constant_constant
    static void process(
        const TiDB::TiDBCollatorPtr & collator,
        const std::string & a,
        const std::string & b,
        std::string & c) // todo.....
    {
        std::cout << "constant constant with collator" << std::endl;
        int res;
        try
        {
            res = compareDate(a, b);
        }
        catch (...)
        {
            res = collator->compare(reinterpret_cast<const char *>(a.data()), a.size(), reinterpret_cast<const char *>(b.data()), b.size());
        }
        if (res < 0)
            c = a;
        else
            c = b;
    }

    // string_string
    static void process(
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets,
        size_t i)
    {
        std::cout << "string string without collator" << std::endl;
        size_t a_size;
        size_t b_size;
        std::cout << "i = " << i << std::endl;
        if (i == 0)
        {
            a_size = a_offsets[0] - 1;
            b_size = b_offsets[0] - 1;
            processImplWithoutCollator(a_size, b_size, &a_data[0], &b_data[0], c_data, c_offsets);
        }
        else
        {
            a_size = a_offsets[i] - a_offsets[i - 1] - 1;
            b_size = b_offsets[i] - b_offsets[i - 1] - 1;
            processImplWithoutCollator(a_size, b_size, &a_data[a_offsets[i - 1]], &b_data[b_offsets[i - 1]], c_data, c_offsets);
        }
    }

    // string_constant
    static void process(
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const String & b,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets,
        size_t i)
    {
        std::cout << "string constant without collator" << std::endl;
        const unsigned char * b_data = reinterpret_cast<const unsigned char *>(b.data());
        ColumnString::Offset b_size = b.size();
        size_t a_size;
        if (i == 0)
        {
            a_size = a_offsets[0] - 1;
            processImplWithoutCollator(a_size, b_size, &a_data[0], &b_data[0], c_data, c_offsets);
        }
        else
        {
            a_size = a_offsets[i] - a_offsets[i - 1] - 1;
            processImplWithoutCollator(a_size, b_size, &a_data[a_offsets[i - 1]], &b_data[0], c_data, c_offsets);
        }
    }

    // constant_constant
    static void process(
        const std::string & a,
        const std::string & b,
        std::string & c)
    {
        std::cout << "constant constant without collator" << std::endl;
        int res;
        try
        {
            res = compareDate(a, b);
        }
        catch (...)
        {
            res = memcmp(a.data(), b.data(), std::min(a.size(), b.size()));
        }

        if (res < 0)
            c = a;
        else
            c = b;
    }
};

template <typename A, typename B>
struct LeastBaseImpl<A, B, true>
{
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;
    using ResultPrecInferer = ModDecimalInferer; // todo ywq

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        Result x, y;
        std::cout << "fuck here...." << std::endl;
        if constexpr (IsDecimal<A>)
        {
            x = static_cast<Result>(a.value);
        }
        else
            x = static_cast<Result>(a);
        if constexpr (IsDecimal<B>)
            y = static_cast<Result>(b.value);
        else
            y = static_cast<Result>(b);

        return LeastBaseImpl<Result, Result, false>::apply(x, y);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct LeastSpecialImpl
{
    using ResultType = std::make_signed_t<A>;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        static_assert(std::is_same_v<Result, ResultType>, "ResultType != Result");
        return accurate::lessOp(a, b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

namespace
{
// clang-format off
struct NameLeast                { static constexpr auto name = "least"; };
// clang-format on

using FunctionLeast = FunctionBinaryArithmetic<LeastBaseImpl_t, NameLeast>;
using FunctionTiDBLeast = FunctionBuilderTiDBLeastGreatest<LeastGreatest::Least, FunctionLeast>;

} // namespace

void registerFunctionLeast(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTiDBLeast>();
    factory.registerFunction<FunctionLeast>();
}

} // namespace DB