#include <Functions/DivisionUtils.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/LeastGreatest.h>

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
    using ResultPrecInferer = PlusDecimalInferer;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) < static_cast<Result>(b) ? static_cast<Result>(a) : static_cast<Result>(b);
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