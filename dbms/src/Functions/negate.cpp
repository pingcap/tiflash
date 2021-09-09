#include <Functions/FunctionUnaryArithmetic.h>

namespace DB
{
namespace
{
template <typename A>
struct NegateImpl
{
    using ResultType = typename NumberTraits::ResultOfNegate<A>::Type;

    static inline ResultType apply(A a)
    {
        if constexpr (IsDecimal<A>)
        {
            return static_cast<ResultType>(-a.value);
        }
        else
        {
            return -static_cast<ResultType>(a);
        }
    }
};

// clang-format off
struct NameNegate               { static constexpr auto name = "negate"; };
// clang-format on

using FunctionNegate = FunctionUnaryArithmetic<NegateImpl, NameNegate, true>;

} // namespace

template <>
struct FunctionUnaryArithmeticMonotonicity<NameNegate>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return {true, false};
    }
};

void registerFunctionNegate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNegate>();
}

} // namespace DB