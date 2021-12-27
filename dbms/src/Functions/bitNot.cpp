#include <Functions/FunctionUnaryArithmetic.h>

namespace DB
{
namespace
{
template <typename A>
struct BitNotImpl
{
    using ResultType = typename NumberTraits::ResultOfBitNot<A>::Type;

    static ResultType apply(A a [[maybe_unused]])
    {
        if constexpr (IsDecimal<A>)
            throw Exception("unimplement");
        else
            return ~static_cast<ResultType>(a);
    }
};

// clang-format off
struct NameBitNot               { static constexpr auto name = "bitNot"; };
// clang-format on

using FunctionBitNot = FunctionUnaryArithmetic<BitNotImpl, NameBitNot, true>;

} // namespace

template <>
struct FunctionUnaryArithmeticMonotonicity<NameBitNot>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return {};
    }
};

void registerFunctionBitNot(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitNot>();
}

} // namespace DB