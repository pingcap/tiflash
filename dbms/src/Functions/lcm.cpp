#include <Functions/DivisionUtils.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace
{
template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct LCMImpl;

template <typename A, typename B>
struct LCMImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<B>::Type(b), typename NumberTraits::ToInteger<A>::Type(a));
        return boost::integer::lcm(
            typename NumberTraits::ToInteger<Result>::Type(a),
            typename NumberTraits::ToInteger<Result>::Type(b));
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct LCMImpl<A, B, true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return LCMImpl<Result, Result>::apply(static_cast<Result>(a), static_cast<Result>(b));
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

// clang-format off
struct NameLCM                  { static constexpr auto name = "lcm"; };
// clang-format on

using FunctionLCM = FunctionBinaryArithmetic<LCMImpl, NameLCM>;

} // namespace

void registerFunctionLCM(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLCM>();
}

} // namespace DB