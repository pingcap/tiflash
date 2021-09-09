#include <Functions/FunctionUnaryArithmetic.h>

namespace DB
{
namespace
{
template <typename A>
struct IntExp10Impl
{
    using ResultType = UInt64;

    static inline ResultType apply(A a)
    {
        return intExp10(a);
    }
};

template <typename A>
struct IntExp10Impl<Decimal<A>>
{
    using ResultType = UInt64;

    static inline ResultType apply(Decimal<A> a)
    {
        return intExp10(a);
    }
};

// clang-format off
struct NameIntExp10             { static constexpr auto name = "intExp10"; };
// clang-format on

using FunctionIntExp10 = FunctionUnaryArithmetic<IntExp10Impl, NameIntExp10, true>;

} // namespace

template <>
struct FunctionUnaryArithmeticMonotonicity<NameIntExp10>
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

        if (left_float < 0 || right_float > 19)
            return {};

        return {true};
    }
};

void registerFunctionIntExp10(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIntExp10>();
}

} // namespace DB