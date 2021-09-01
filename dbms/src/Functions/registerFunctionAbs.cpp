#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsUnaryArithmetic.h>
#include <Functions/registerFunctionAbs.h>

namespace DB
{
void registerFunctionAbs(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAbs>();
}

} // namespace DB
