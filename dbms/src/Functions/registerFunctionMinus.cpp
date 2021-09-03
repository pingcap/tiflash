#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsBinaryArithmetic.h>
#include <Functions/registerFunctionMinus.h>

namespace DB
{
void registerFunctionMinus(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMinus>();
}

} // namespace DB
