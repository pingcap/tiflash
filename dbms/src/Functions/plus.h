#pragma once
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
template <typename A, typename B>
struct PlusImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        /// Next everywhere, static_cast - so that there is no wrong result in expressions of the form Int64 c = UInt32(a) * Int32(-1).
        return static_cast<Result>(a) + b;
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct PlusImpl<A, B, true>
{
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;
    using ResultPrecInferer = PlusDecimalInferer;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) + static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

// clang-format off
struct NamePlus                 { static constexpr auto name = "plus"; };
// clang-format on

using FunctionPlus = FunctionBinaryArithmetic<PlusImpl, NamePlus>;

} // namespace DB