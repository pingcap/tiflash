#include <Functions/FunctionUnaryArithmetic.h>

namespace DB
{
namespace
{
template <typename A>
struct IntExp2Impl
{
    using ResultType = UInt64;

    static inline ResultType apply(A a)
    {
        return intExp2(a);
    }
};

template <typename A>
struct IntExp2Impl<Decimal<A>>
{
    using ResultType = UInt64;

    static inline ResultType apply(Decimal<A>)
    {
        return 0;
    }
};

// clang-format off
struct NameIntExp2              { static constexpr auto name = "intExp2"; };
// clang-format on

using FunctionIntExp2 = FunctionUnaryArithmetic<IntExp2Impl, NameIntExp2, true>;

} // namespace

template <>
struct FunctionUnaryArithmeticMonotonicity<NameIntExp2>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull()
            ? -std::numeric_limits<Float64>::infinity()
            : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull()
            ? std::numeric_limits<Float64>::infinity()
            : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if (left_float < 0 || right_float > 63)
            return {};

        return {true};
    }
};

void registerFunctionIntExp2(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIntExp2>();
}

} // namespace DB