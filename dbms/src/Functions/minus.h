#pragma once
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
template <typename A, typename B>
struct MinusImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfSubtraction<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) - b;
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct MinusImpl<A, B, true>
{
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;
    using ResultPrecInferer = PlusDecimalInferer;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) - static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

// clang-format off
struct NameMinus                { static constexpr auto name = "minus"; };
// clang-format on

using FunctionMinus = FunctionBinaryArithmetic<MinusImpl, NameMinus>;

} // namespace DB