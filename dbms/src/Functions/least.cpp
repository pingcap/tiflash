#include <Functions/DivisionUtils.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/LeastGreatest.h>

#include <type_traits>


namespace DB
{
template <typename A, typename B>
struct BinaryLeastBaseImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfBinaryLeast<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        /** gcc 4.9.2 successfully vectorizes a loop from this function. */
        // YWQ: in bench, The performance is almost the same..
        return accurate::lessOp(a, b) ? static_cast<Result>(a) : static_cast<Result>(b);
        // return static_cast<Result>(a) < static_cast<Result>(b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BinaryLeastBaseImpl<A, B, true>
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

namespace
{
// clang-format off
struct NameLeast                { static constexpr auto name = "least"; };
// clang-format on

using FunctionBinaryLeast = FunctionBinaryArithmetic<BinaryLeastBaseImpl_t, NameLeast>;
using FunctionTiDBLeast = FunctionBuilderTiDBLeastGreatest<LeastImpl, FunctionBinaryLeast>;

} // namespace

void registerFunctionLeast(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTiDBLeast>();
    factory.registerFunction<FunctionBinaryLeast>();
}

} // namespace DB