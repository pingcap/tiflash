#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsUnaryArithmetic.h>
#include <Functions/registerFunctionNegate.h>

namespace DB
{
void registerFunctionNegate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNegate>();
}

} // namespace DB
