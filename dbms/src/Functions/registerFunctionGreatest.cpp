#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>
#include <Functions/registerFunctionGreatest.h>

namespace DB
{
void registerFunctionGreatest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGreatest>();
}

} // namespace DB
