#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsUnaryArithmetic.h>
#include <Functions/registerFunctionIntExp2.h>

namespace DB
{
void registerFunctionIntExp2(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIntExp2>();
}

} // namespace DB
