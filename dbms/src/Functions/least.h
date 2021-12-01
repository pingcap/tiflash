#include <Functions/DivisionUtils.h>
#include <Functions/FunctionBinaryArithmetic.h>

#include "DataTypes/NumberTraits.h"
#include "Functions/IsOperation.h"

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


// clang-format off
struct NameLeast                { static constexpr auto name = "least"; };
// clang-format on

using FunctionLeast = FunctionBinaryArithmetic<LeastImpl, NameLeast>;

} // namespace DB