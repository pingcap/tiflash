#include <Functions/DivisionUtils.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/LeastGreatest.h>

namespace DB
{
template <typename A, typename B>
struct BinaryGreatestBaseImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfBinaryLeastGreatest<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        const Result tmp_a = static_cast<Result>(a);
        const Result tmp_b = static_cast<Result>(b);
        return accurate::greaterOp(tmp_a, tmp_b) ? tmp_a : tmp_b;
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BinaryGreatestBaseImpl<A, B, true>
{
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;
    using ResultPrecInferer = PlusDecimalInferer;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        const Result tmp_a = static_cast<Result>(a);
        const Result tmp_b = static_cast<Result>(b);
        return tmp_a > tmp_b ? tmp_a : tmp_b;
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

using FunctionBinaryGreatest = FunctionBinaryArithmetic<BinaryGreatestBaseImpl_t, NameGreatest>;
using FunctionTiDBGreatest = FunctionVectorizedLeastGreatest<GreatestImpl, FunctionBinaryGreatest>;

} // namespace

void registerFunctionGreatest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTiDBGreatest>();
}

} // namespace DB
