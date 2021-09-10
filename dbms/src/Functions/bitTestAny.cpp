#include <Functions/FunctionBitTestMany.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct BitTestAnyImpl
{
    template <typename A, typename B>
    static inline UInt8 apply(A a, B b)
    {
        return (a & b) != 0;
    };
};

// clang-format off
struct NameBitTestAny           { static constexpr auto name = "bitTestAny"; };
// clang-format on

using FunctionBitTestAny = FunctionBitTestMany<BitTestAnyImpl, NameBitTestAny>;

} // namespace

void registerFunctionBitTestAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTestAny>();
}

} // namespace DB