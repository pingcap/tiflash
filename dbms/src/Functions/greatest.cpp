#include <Functions/DivisionUtils.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/LeastGreatestGeneric.h>

namespace DB
{
template <typename A, typename B>
struct GreatestBaseImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfTiDBLeast<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) > static_cast<Result>(b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }

    // string_string
    static void process(
        const TiDB::TiDBCollatorPtr & collator,
        const ColumnString::Chars_t & a_data,
        const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars_t & b_data,
        const ColumnString::Offsets & b_offsets,
        ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets,
        size_t i)
    {
        size_t a_size;
        size_t b_size;
        int res;
        if (i == 0)
        {
            a_size = a_offsets[0] - 1;
            b_size = b_offsets[0] - 1;
            res = collator->compare(reinterpret_cast<const char *>(&a_data[0]), a_size, reinterpret_cast<const char *>(&b_data[0]), b_size);
            if (res > 0)
            {
                memcpy(&c_data[0], &a_data[0], a_size);
                c_offsets.push_back(a_size + 1);
            }
            else
            {
                memcpy(&c_data[0], &b_data[0], b_size);
                c_offsets.push_back(b_size + 1);
            }
        }
        else
        {
            a_size = a_offsets[i] - a_offsets[i - 1] - 1;
            b_size = b_offsets[i] - b_offsets[i - 1] - 1;
            res = collator->compare(reinterpret_cast<const char *>(&a_data[a_offsets[i - 1]]), a_size, reinterpret_cast<const char *>(&b_data[b_offsets[i - 1]]), b_size);
            if (res > 0)
            {
                memcpy(&c_data[c_offsets.back()], &a_data[a_offsets[i - 1]], a_size);
                c_offsets.push_back(c_offsets.back() + a_size + 1);
            }
            else
            {
                memcpy(&c_data[c_offsets.back()], &b_data[b_offsets[i - 1]], b_size);
                c_offsets.push_back(c_offsets.back() + b_size + 1);
            }
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
        const char * b_data = reinterpret_cast<const char *>(b.data());
        ColumnString::Offset b_size = b.size();
        size_t a_size;
        if (i == 0)
        {
            a_size = a_offsets[0] - 1;
            int res = collator->compare(reinterpret_cast<const char *>(&a_data[0]), a_size, b_data, b_size);

            if (res > 0)
            {
                memcpy(&c_data[0], &a_data[0], a_size);
                c_offsets.push_back(a_size + 1);
            }
            else
            {
                memcpy(&c_data[0], &b_data[0], b_size);
                c_offsets.push_back(b_size + 1);
            }
        }
        else
        {
            a_size = a_offsets[i] - a_offsets[i - 1] - 1;
            int res = collator->compare(reinterpret_cast<const char *>(&a_data[a_offsets[i - 1]]), a_size, b_data, b_size);

            if (res > 0)
            {
                memcpy(&c_data[c_offsets.back()], &a_data[a_offsets[i - 1]], a_size);
                c_offsets.push_back(c_offsets.back() + a_size + 1);
            }
            else
            {
                memcpy(&c_data[c_offsets.back()], &b_data[0], b_size);
                c_offsets.push_back(c_offsets.back() + b_size + 1);
            }
        }
    }

    // constant_constant
    static void process(
        const TiDB::TiDBCollatorPtr & collator,
        const std::string & a,
        const std::string & b,
        std::string & c)
    {
        int res = collator->compare(reinterpret_cast<const char *>(a.data()), a.size(), reinterpret_cast<const char *>(b.data()), b.size());
        if (res > 0)
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
        size_t a_size;
        size_t b_size;
        int res;
        if (i == 0)
        {
            a_size = a_offsets[0] - 1;
            b_size = b_offsets[0] - 1;
            res = memcmp(&a_data[0], &b_data[0], std::min(a_size, b_size));
            if (res > 0)
            {
                memcpy(&c_data[0], &a_data[0], a_size);
                c_offsets.push_back(a_size + 1);
            }
            else
            {
                memcpy(&c_data[0], &b_data[0], b_size);
                c_offsets.push_back(b_size + 1);
            }
        }
        else
        {
            a_size = a_offsets[i] - a_offsets[i - 1] - 1;
            b_size = b_offsets[i] - b_offsets[i - 1] - 1;
            res = memcmp(&a_data[a_offsets[i - 1]], &b_data[b_offsets[i - 1]], std::min(a_size, b_size));

            if (res > 0)
            {
                memcpy(&c_data[c_offsets.back()], &a_data[a_offsets[i - 1]], a_size);
                c_offsets.push_back(c_offsets.back() + a_size + 1);
            }
            else
            {
                memcpy(&c_data[c_offsets.back()], &b_data[b_offsets[i - 1]], b_size);
                c_offsets.push_back(c_offsets.back() + b_size + 1);
            }
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
        const char * b_data = reinterpret_cast<const char *>(b.data());
        ColumnString::Offset b_size = b.size();
        size_t a_size;
        if (i == 0)
        {
            a_size = a_offsets[0] - 1;
            int res = memcmp(&a_data[0], b_data, std::min(a_offsets[0], b_size));
            if (res > 0)
            {
                memcpy(&c_data[0], &a_data[0], a_size);
                c_offsets.push_back(a_size + 1);
            }
            else
            {
                memcpy(&c_data[0], &b_data[0], b_size);
                c_offsets.push_back(b_size + 1);
            }
        }
        else
        {
            a_size = a_offsets[i] - a_offsets[i - 1] - 1;
            int res = memcmp(&a_data[a_offsets[i - 1]], b_data, std::min(a_offsets[i] - a_offsets[i - 1], b_size));

            if (res > 0)
            {
                memcpy(&c_data[c_offsets.back()], &a_data[a_offsets[i - 1]], a_size);
                c_offsets.push_back(c_offsets.back() + a_size + 1);
            }
            else
            {
                memcpy(&c_data[c_offsets.back()], &b_data[0], b_size);
                c_offsets.push_back(c_offsets.back() + b_size + 1);
            }
        }
    }

    // constant_constant
    static void process(
        const std::string & a,
        const std::string & b,
        std::string & c)
    {
        int res = memcmp(a.data(), b.data(), std::min(a.size(), b.size()));

        if (res > 0)
            c = a;
        else
            c = b;
    }
};

template <typename A, typename B>
struct GreatestBaseImpl<A, B, true>
{
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;
    using ResultPrecInferer = ModDecimalInferer;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        Result x, y;
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

        return GreatestBaseImpl<Result, Result, false>::apply(x, y);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct GreatestSpecialImpl
{
    using ResultType = std::make_unsigned_t<A>;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        static_assert(std::is_same_v<Result, ResultType>, "ResultType != Result");
        return accurate::greaterOp(a, b) ? static_cast<Result>(a) : static_cast<Result>(b);
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
struct NameGreatest             { static constexpr auto name = "greatest"; };
// clang-format on

using FunctionGreatest = FunctionBinaryArithmetic<GreatestImpl, NameGreatest>;
using FunctionTiDBGreatest = FunctionBuilderTiDBLeastGreatest<LeastGreatest::Greatest, FunctionGreatest>;

} // namespace

void registerFunctionGreatest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTiDBGreatest>();
    factory.registerFunction<FunctionGreatest>();
}
} // namespace DB