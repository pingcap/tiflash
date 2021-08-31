#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>
#include <Functions/registerFunctionLeast.h>

namespace DB
{
void registerFunctionLeast(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLeast>();
}

} // namespace DB
