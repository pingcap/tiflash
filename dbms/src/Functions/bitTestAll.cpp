#include <Functions/FunctionBitTestMany.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct BitTestAllImpl
{
    template <typename A, typename B>
    static UInt8 apply(A a, B b)
    {
        return (a & b) == b;
    };
};

// clang-format off
struct NameBitTestAll           { static constexpr auto name = "bitTestAll"; };
// clang-format on

using FunctionBitTestAll = FunctionBitTestMany<BitTestAllImpl, NameBitTestAll>;

} // namespace

void registerFunctionBitTestAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTestAll>();
}

} // namespace DB