#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsBinaryArithmetic.h>
#include <Functions/registerFunctionLeast.h>

namespace DB
{
void registerFunctionLeast(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLeast>();
}

} // namespace DB
