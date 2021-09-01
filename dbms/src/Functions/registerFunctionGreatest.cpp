#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsBinaryArithmetic.h>
#include <Functions/registerFunctionGreatest.h>

namespace DB
{
void registerFunctionGreatest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGreatest>();
}

} // namespace DB
