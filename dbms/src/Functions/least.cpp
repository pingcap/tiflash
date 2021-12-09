#include <DataTypes/NumberTraits.h>
#include <Functions/DivisionUtils.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IsOperation.h>
#include <Functions/LeastGreatestGeneric.h>

#include <cstddef>

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

    static void process(const TiDB::TiDBCollatorPtr & collator[[maybe_unused]], const ColumnString::Chars_t & a_data[[maybe_unused]], const ColumnString::Offsets & a_offsets[[maybe_unused]], const ColumnString::Chars_t & b_data[[maybe_unused]], const ColumnString::Offsets & b_offsets[[maybe_unused]], ColumnString::Chars_t & c_data[[maybe_unused]],
        ColumnString::Offsets & c_offsets[[maybe_unused]], size_t i[[maybe_unused]]) 
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct LeastBaseImpl<A, B, true>
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

        return LeastBaseImpl<Result, Result, false>::apply(x, y);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }

    static void process(const TiDB::TiDBCollatorPtr & collator[[maybe_unused]], const ColumnString::Chars_t & a_data[[maybe_unused]], const ColumnString::Offsets & a_offsets[[maybe_unused]], const ColumnString::Chars_t & b_data[[maybe_unused]], const ColumnString::Offsets & b_offsets[[maybe_unused]], ColumnString::Chars_t & c_data[[maybe_unused]],
        ColumnString::Offsets & c_offsets[[maybe_unused]], size_t i[[maybe_unused]]) 
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

    static void process(const TiDB::TiDBCollatorPtr & collator[[maybe_unused]], const ColumnString::Chars_t & a_data[[maybe_unused]], const ColumnString::Offsets & a_offsets[[maybe_unused]], const ColumnString::Chars_t & b_data[[maybe_unused]], const ColumnString::Offsets & b_offsets[[maybe_unused]], ColumnString::Chars_t & c_data[[maybe_unused]],
        ColumnString::Offsets & c_offsets[[maybe_unused]], size_t i[[maybe_unused]]) 
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct LeastStringImpl
{
    
    using ResultType = typename NumberTraits::ResultOfTiDBLeast<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A, B)
    {
        throw Exception("Should not reach here");
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
    static void process(const TiDB::TiDBCollatorPtr & collator, const ColumnString::Chars_t & a_data, const ColumnString::Offsets & a_offsets, const ColumnString::Chars_t & b_data, const ColumnString::Offsets & b_offsets, ColumnString::Chars_t & c_data,
        ColumnString::Offsets & c_offsets, size_t i)
    {
        size_t a_size;
        size_t b_size;
        int res;
        if (i == 0)
        {
            a_size = a_offsets[0] - 1;
            b_size = b_offsets[0] - 1;
            res = collator->compare(reinterpret_cast<const char *>(&a_data[0]), a_size, reinterpret_cast<const char *>(&b_data[0]), b_size);
            if (res < 0)
            {
                memcpy(&c_data[0], &a_data[0], a_size);
                c_offsets.push_back(a_size + 1);
            }
            else 
            {
                memcpy(&c_data[0], &a_data[0], b_size);
                c_offsets.push_back(b_size + 1);
            }
        }
        else
        {
            a_size = a_offsets[i] - a_offsets[i - 1] - 1;
            b_size = b_offsets[i] - b_offsets[i - 1] - 1;
            res = collator->compare(reinterpret_cast<const char *>(&a_data[a_offsets[i - 1]]), a_size, reinterpret_cast<const char *>(&b_data[b_offsets[i - 1]]), b_size);
            if (res < 0)
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
};

namespace
{
// clang-format off
struct NameLeast                { static constexpr auto name = "least"; };
// clang-format on

using FunctionLeast = FunctionBinaryArithmetic<LeastImpl, NameLeast>;
using FunctionLeast1 = FunctionBinaryArithmetic<LeastStringImpl, NameLeast>;
using FunctionTiDBLeast = FunctionTiDBLeastGreatest<LeastGreatest::Least, FunctionLeast1>;
} // namespace

void registerFunctionLeast(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTiDBLeast>();
    factory.registerFunction<FunctionLeast>();
}

} // namespace DB